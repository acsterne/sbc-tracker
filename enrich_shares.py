"""
enrich_shares.py — Backfill shares_outstanding from EDGAR companyfacts API.

Uses edgartools time_series() or to_dataframe() to get historical share counts,
then updates filings rows where shares_outstanding IS NULL.

For multi-class companies (META, GOOGL) where total shares isn't tagged,
falls back to summing per-class shares from individual filings.

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all companies
    DATABASE_URL=... python3 enrich_shares.py --ticker AAPL  # one company
"""

import os
import argparse
import psycopg2
import psycopg2.extras

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL = os.environ["DATABASE_URL"]

SHARE_CONCEPTS = [
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
    "CommonStockSharesIssuedAndOutstanding",
]


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def get_shares_from_facts(ticker):
    """
    Pull shares outstanding from EDGAR companyfacts via edgartools.
    Returns dict: {fiscal_year: shares_value}
    """
    company = Company(ticker)
    try:
        facts = company.get_facts()
    except Exception as e:
        print(f"    [WARN] get_facts() failed: {e}")
        return {}

    shares_by_year = {}

    # ── Method 1: time_series() — cleanest if it works ────────────────────────
    for concept in SHARE_CONCEPTS:
        try:
            df = facts.time_series(concept, periods=40)
            if df is not None and not df.empty:
                print(f"    time_series({concept}): {len(df)} rows")
                print(f"    columns: {list(df.columns)}")

                # Filter to annual (FY) periods
                if "fiscal_period" in df.columns:
                    annual = df[df["fiscal_period"] == "FY"]
                else:
                    annual = df

                # Extract fiscal_year → value
                for _, row in annual.iterrows():
                    fy = None
                    val = None

                    # Try known column names for fiscal year
                    for col in ("fiscal_year", "fy"):
                        if col in row.index and row[col]:
                            try:
                                fy = int(row[col])
                            except (ValueError, TypeError):
                                pass
                            break

                    # Fall back to period_end year
                    if fy is None:
                        for col in ("period_end", "end"):
                            if col in row.index and row[col]:
                                try:
                                    fy = int(str(row[col])[:4])
                                except (ValueError, TypeError):
                                    pass
                                break

                    # Get the value
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
                    print(f"    got {len(shares_by_year)} annual values via time_series")
                    return shares_by_year
        except Exception as e:
            print(f"    time_series({concept}): {e}")
            continue

    # ── Method 2: to_dataframe() + filter — fallback ──────────────────────────
    try:
        df = facts.to_dataframe()
        if df is not None and not df.empty:
            print(f"    to_dataframe(): {len(df)} total facts, columns: {list(df.columns)}")

            # Find shares-related rows
            concept_col = None
            for col in ("concept", "tag", "name"):
                if col in df.columns:
                    concept_col = col
                    break

            if concept_col:
                for concept in SHARE_CONCEPTS:
                    mask = df[concept_col].str.contains(concept, case=False, na=False)
                    shares_df = df[mask]
                    if shares_df.empty:
                        continue

                    print(f"    found {len(shares_df)} rows matching {concept}")

                    # Filter to 10-K
                    form_col = next((c for c in shares_df.columns
                                     if c.lower() in ("form", "form_type")), None)
                    if form_col:
                        shares_df = shares_df[shares_df[form_col].isin(["10-K", "10-K/A"])]

                    # Filter to FY if available
                    if "fiscal_period" in shares_df.columns:
                        fy_df = shares_df[shares_df["fiscal_period"] == "FY"]
                        if not fy_df.empty:
                            shares_df = fy_df

                    val_col = next((c for c in shares_df.columns
                                    if c.lower() in ("numeric_value", "value", "val")), None)
                    end_col = next((c for c in shares_df.columns
                                    if c.lower() in ("period_end", "end")), None)
                    fy_col = next((c for c in shares_df.columns
                                   if c.lower() in ("fiscal_year", "fy")), None)

                    if not val_col:
                        continue

                    for _, row in shares_df.iterrows():
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
                        print(f"    got {len(shares_by_year)} annual values via to_dataframe")
                        return shares_by_year
    except Exception as e:
        print(f"    to_dataframe() failed: {e}")

    # ── Method 3: search for any shares concept ───────────────────────────────
    try:
        results = facts.search_concepts("shares outstanding")
        if results:
            print(f"    search found concepts: {results}")
    except Exception:
        pass

    return shares_by_year


def enrich_company(cur, company_id, ticker):
    """Backfill shares_outstanding for filings where it's null."""
    cur.execute("""
        SELECT fiscal_year FROM filings
        WHERE company_id = %s AND form_type = '10-K' AND shares_outstanding IS NULL
        ORDER BY fiscal_year
    """, (company_id,))
    missing_years = [r["fiscal_year"] for r in cur.fetchall()]

    if not missing_years:
        print(f"    no missing shares — skip")
        return 0

    print(f"    {len(missing_years)} years missing shares: "
          f"{missing_years[0]}–{missing_years[-1]}")

    shares_by_year = get_shares_from_facts(ticker)
    if not shares_by_year:
        print(f"    [WARN] no shares data found")
        return 0

    updated = 0
    for fy in missing_years:
        shares = shares_by_year.get(fy)
        if shares is None:
            continue
        cur.execute("""
            UPDATE filings SET shares_outstanding = %s
            WHERE company_id = %s AND fiscal_year = %s AND form_type = '10-K'
              AND shares_outstanding IS NULL
        """, (shares, company_id, fy))
        if cur.rowcount > 0:
            updated += 1
            print(f"      FY{fy}: {shares:,}")

    if updated > 0:
        from fetch_sbc import refresh_metrics
        refresh_metrics(cur, company_id)

    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Backfill shares_outstanding from EDGAR facts API")
    parser.add_argument("--ticker", help="Enrich one company only")
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
            updated = enrich_company(cur, co["id"], ticker)
            total_updated += updated
            conn.commit()
        except Exception as e:
            print(f"    [ERROR] {e}")
            conn.rollback()

    # Summary
    print(f"\n{'='*60}")
    print(f"SHARES ENRICHMENT COMPLETE — {total_updated} values backfilled")
    print(f"{'='*60}")

    cur.execute("""
        SELECT c.ticker,
               COUNT(*) AS total,
               COUNT(f.shares_outstanding) AS with_shares
        FROM companies c
        JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
        GROUP BY c.id, c.ticker
        ORDER BY c.ticker
    """)
    rows = cur.fetchall()
    print(f"\n  {'Ticker':<8} {'With Shares':>12} {'/ Total':>8}")
    print(f"  {'-'*8} {'-'*12} {'-'*8}")
    for r in rows:
        flag = " !" if r["with_shares"] == 0 else ""
        print(f"  {r['ticker']:<8} {r['with_shares']:>12} / {r['total']:<4}{flag}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
