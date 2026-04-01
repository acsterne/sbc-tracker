"""
enrich_shares.py — Backfill shares_outstanding from EDGAR companyfacts API.

Uses edgartools to get historical share counts, applies stock split
adjustments for known splits, and updates filings rows.

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all companies
    DATABASE_URL=... python3 enrich_shares.py --ticker AAPL  # one company
    DATABASE_URL=... python3 enrich_shares.py --force        # overwrite existing values
"""

import os
import argparse
import psycopg2
import psycopg2.extras

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL = os.environ["DATABASE_URL"]

# Try these concepts in order — first one with data wins
SHARE_CONCEPTS = [
    "dei:EntityCommonStockSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
    "us-gaap:CommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
    "us-gaap:CommonStockSharesIssued",
    "CommonStockSharesIssued",
    "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "WeightedAverageNumberOfSharesOutstandingBasic",
]

# Known stock splits: (ticker, split_year, multiplier)
# multiplier applies to all years BEFORE split_year
KNOWN_SPLITS = [
    ("AMZN",  2022, 20),   # 20:1 June 2022
    ("GOOGL", 2022, 20),   # 20:1 July 2022
    ("TSLA",  2022, 3),    # 3:1 August 2022
    ("AAPL",  2020, 4),    # 4:1 August 2020
    ("AAPL",  2014, 7),    # 7:1 June 2014
]


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def apply_split_adjustments(ticker, shares_by_year):
    """
    Apply known stock split multipliers so all years are comparable.
    Multiplies pre-split years by the split ratio.
    """
    splits = [(yr, mult) for t, yr, mult in KNOWN_SPLITS if t == ticker]
    if not splits:
        return shares_by_year

    # Sort splits chronologically
    splits.sort()
    adjusted = dict(shares_by_year)

    for split_year, multiplier in splits:
        for fy in list(adjusted.keys()):
            if fy < split_year:
                adjusted[fy] = int(adjusted[fy] * multiplier)

    applied = [(yr, m) for yr, m in splits]
    print(f"    [SPLIT] applied: {applied}")

    return adjusted


def get_shares_from_facts(ticker):
    """
    Pull shares outstanding from EDGAR companyfacts via edgartools.
    Tries multiple concept names in order, returns first that works.
    Returns dict: {fiscal_year: shares_value}
    """
    company = Company(ticker)
    try:
        facts = company.get_facts()
    except Exception as e:
        print(f"    [WARN] get_facts() failed: {e}")
        return {}

    shares_by_year = {}

    # ── Method 1: time_series() for each concept ─────────────────────────────
    for concept in SHARE_CONCEPTS:
        try:
            df = facts.time_series(concept, periods=40)
            if df is None or df.empty:
                continue
            print(f"    time_series({concept}): {len(df)} rows")

            if "fiscal_period" in df.columns:
                annual = df[df["fiscal_period"] == "FY"]
            else:
                annual = df

            for _, row in annual.iterrows():
                fy = None
                val = None

                for col in ("fiscal_year", "fy"):
                    if col in row.index and row[col]:
                        try:
                            fy = int(row[col])
                        except (ValueError, TypeError):
                            pass
                        break
                if fy is None:
                    for col in ("period_end", "end"):
                        if col in row.index and row[col]:
                            try:
                                fy = int(str(row[col])[:4])
                            except (ValueError, TypeError):
                                pass
                            break

                for col in ("numeric_value", "value", "val"):
                    if col in row.index:
                        try:
                            val = int(float(row[col]))
                        except (ValueError, TypeError):
                            pass
                        break

                if fy and val and val > 0:
                    if fy not in shares_by_year or val > shares_by_year[fy]:
                        shares_by_year[fy] = val

            if shares_by_year:
                print(f"    got {len(shares_by_year)} years via time_series({concept})")
                return shares_by_year

        except Exception:
            continue

    # ── Method 2: to_dataframe() + filter ─────────────────────────────────────
    try:
        df = facts.to_dataframe()
        if df is not None and not df.empty:
            concept_col = next((c for c in ("concept", "tag", "name") if c in df.columns), None)
            if concept_col:
                for concept_name in ["SharesOutstanding", "SharesIssued",
                                     "WeightedAverageNumberOfSharesOutstandingBasic"]:
                    mask = df[concept_col].str.contains(concept_name, case=False, na=False)
                    sub = df[mask]
                    if sub.empty:
                        continue

                    form_col = next((c for c in sub.columns if c.lower() in ("form", "form_type")), None)
                    if form_col:
                        sub = sub[sub[form_col].isin(["10-K", "10-K/A"])]

                    if "fiscal_period" in sub.columns:
                        fy_sub = sub[sub["fiscal_period"] == "FY"]
                        if not fy_sub.empty:
                            sub = fy_sub

                    val_col = next((c for c in sub.columns
                                    if c.lower() in ("numeric_value", "value", "val")), None)
                    fy_col = next((c for c in sub.columns
                                   if c.lower() in ("fiscal_year", "fy")), None)
                    end_col = next((c for c in sub.columns
                                    if c.lower() in ("period_end", "end")), None)

                    if not val_col:
                        continue

                    for _, row in sub.iterrows():
                        fy = None
                        if fy_col:
                            try:
                                fy = int(row[fy_col])
                            except (ValueError, TypeError):
                                pass
                        if fy is None and end_col:
                            try:
                                fy = int(str(row[end_col])[:4])
                            except (ValueError, TypeError):
                                pass
                        if fy is None:
                            continue
                        try:
                            val = int(float(row[val_col]))
                        except (ValueError, TypeError):
                            continue
                        if val <= 0:
                            continue
                        if fy not in shares_by_year or val > shares_by_year[fy]:
                            shares_by_year[fy] = val

                    if shares_by_year:
                        print(f"    got {len(shares_by_year)} years via to_dataframe({concept_name})")
                        return shares_by_year
    except Exception as e:
        print(f"    to_dataframe() failed: {e}")

    return shares_by_year


def enrich_company(cur, company_id, ticker, force=False):
    """Backfill shares_outstanding for filings."""
    if force:
        cur.execute("""
            SELECT fiscal_year FROM filings
            WHERE company_id = %s AND form_type = '10-K'
            ORDER BY fiscal_year
        """, (company_id,))
    else:
        cur.execute("""
            SELECT fiscal_year FROM filings
            WHERE company_id = %s AND form_type = '10-K' AND shares_outstanding IS NULL
            ORDER BY fiscal_year
        """, (company_id,))

    target_years = [r["fiscal_year"] for r in cur.fetchall()]

    if not target_years:
        print(f"    no target years — skip")
        return 0

    print(f"    {len(target_years)} years to fill: {target_years[0]}–{target_years[-1]}")

    shares_by_year = get_shares_from_facts(ticker)
    if not shares_by_year:
        print(f"    [WARN] no shares data found")
        return 0

    # Apply split adjustments
    shares_by_year = apply_split_adjustments(ticker, shares_by_year)

    # Show what we got
    for fy in sorted(shares_by_year):
        print(f"      FY{fy}: {shares_by_year[fy]:,}")

    # Update DB
    updated = 0
    for fy in target_years:
        shares = shares_by_year.get(fy)
        if shares is None:
            continue
        if force:
            cur.execute("""
                UPDATE filings SET shares_outstanding = %s
                WHERE company_id = %s AND fiscal_year = %s AND form_type = '10-K'
            """, (shares, company_id, fy))
        else:
            cur.execute("""
                UPDATE filings SET shares_outstanding = %s
                WHERE company_id = %s AND fiscal_year = %s AND form_type = '10-K'
                  AND shares_outstanding IS NULL
            """, (shares, company_id, fy))
        if cur.rowcount > 0:
            updated += 1

    if updated > 0:
        from fetch_sbc import refresh_metrics
        refresh_metrics(cur, company_id)
        print(f"    updated {updated} years")

    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Backfill shares_outstanding from EDGAR facts API")
    parser.add_argument("--ticker", help="Enrich one company only")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing shares values (for split adjustments)")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT id, ticker, cik FROM companies ORDER BY ticker")
    companies = cur.fetchall()

    if args.ticker:
        companies = [c for c in companies
                     if c["ticker"].upper() == args.ticker.upper()]
        if not companies:
            print(f"[ERROR] {args.ticker} not found")
            return

    total_updated = 0
    for co in companies:
        ticker = co["ticker"]
        print(f"\n  [{ticker}]")
        try:
            updated = enrich_company(cur, co["id"], ticker, force=args.force)
            total_updated += updated
            conn.commit()
        except Exception as e:
            print(f"    [ERROR] {e}")
            conn.rollback()

    # Summary
    print(f"\n{'='*60}")
    print(f"SHARES ENRICHMENT — {total_updated} values updated")
    print(f"{'='*60}")

    cur.execute("""
        SELECT c.ticker,
               COUNT(*) AS total,
               COUNT(f.shares_outstanding) AS with_shares,
               MIN(CASE WHEN f.shares_outstanding IS NOT NULL THEN f.fiscal_year END) AS earliest,
               MAX(CASE WHEN f.shares_outstanding IS NOT NULL THEN f.fiscal_year END) AS latest,
               (SELECT f2.shares_outstanding FROM filings f2
                WHERE f2.company_id = c.id AND f2.form_type = '10-K'
                  AND f2.shares_outstanding IS NOT NULL
                ORDER BY f2.fiscal_year DESC LIMIT 1) AS latest_shares
        FROM companies c
        JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
        GROUP BY c.id, c.ticker
        ORDER BY c.ticker
    """)
    rows = cur.fetchall()

    print(f"\n  {'Ticker':<8} {'Shares':>8} {'/ Tot':>6} {'Earliest':>9} {'Latest':>7} {'Latest Shares':>16}")
    print(f"  {'-'*8} {'-'*8} {'-'*6} {'-'*9} {'-'*7} {'-'*16}")
    for r in rows:
        shr = f"{r['latest_shares']/1e9:.2f}B" if r['latest_shares'] else "—"
        ear = r['earliest'] or "—"
        lat = r['latest'] or "—"
        flag = " !" if r['with_shares'] == 0 else ""
        print(f"  {r['ticker']:<8} {r['with_shares']:>8} / {r['total']:<4} "
              f"{str(ear):>9} {str(lat):>7} {shr:>16}{flag}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
