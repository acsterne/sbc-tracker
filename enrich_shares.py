"""
enrich_shares.py — Backfill shares_outstanding by parsing each 10-K filing.

Same approach as fetch_historical.py — opens each filing's XBRL,
extracts shares from balance sheet or income statement.

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all with gaps
    DATABASE_URL=... python3 enrich_shares.py --ticker DDOG  # one company
"""

import os
import re
import argparse
import psycopg2
import psycopg2.extras

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL = os.environ["DATABASE_URL"]

# Known stock splits
KNOWN_SPLITS = [
    ("AMZN",  2022, 20),
    ("GOOGL", 2022, 20),
    ("TSLA",  2022, 3),
    ("AAPL",  2020, 4),
    ("AAPL",  2014, 7),
]


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def get_shares_from_filing(filing, period):
    """
    Extract shares outstanding from a single 10-K filing.
    Tries balance sheet first (actual EOY count), then income statement
    (weighted average as fallback).
    """
    try:
        xbrl = filing.xbrl()
        if not xbrl:
            return None
    except Exception:
        return None

    # ── Method 1: Balance sheet — direct share concept ────────────────────────
    try:
        bs = xbrl.statements.balance_sheet()
        if bs:
            df = bs.to_dataframe()
            if "is_breakdown" in df.columns:
                df = df[df["is_breakdown"] == False]

            # Look for shares outstanding concepts
            if "concept" in df.columns and period in df.columns:
                share_rows = df[
                    df["concept"].str.contains(
                        "CommonStockSharesOutstanding|CommonStockSharesIssued|"
                        "EntityCommonStockSharesOutstanding",
                        case=False, na=False
                    ) &
                    ~df["concept"].str.contains(
                        "Preferred|Treasury|Repurchase",
                        case=False, na=False
                    )
                ]
                if not share_rows.empty:
                    val = share_rows[period].dropna()
                    if not val.empty:
                        v = float(val.iloc[0])
                        if 1e6 < abs(v) < 1e12:  # 1M to 1T shares
                            return int(v)

            # Parse share count from common stock label text
            # e.g. "Common stock, $0.01 par value; 2,187 million shares outstanding"
            if "label" in df.columns:
                for _, row in df.iterrows():
                    label = str(row.get("label", "")).lower()
                    if "common stock" not in label:
                        continue
                    # "X million shares"
                    m = re.search(r"([\d,]+(?:\.\d+)?)\s*million\s*shares", label, re.I)
                    if m:
                        return int(float(m.group(1).replace(",", "")) * 1e6)
                    # "X,XXX,XXX shares outstanding"
                    m = re.search(r"([\d,]{5,})\s*shares\s*(?:outstanding|issued)", label, re.I)
                    if m:
                        v = int(m.group(1).replace(",", ""))
                        if v > 1e6:
                            return v
    except Exception:
        pass

    # ── Method 2: Income statement — weighted average shares ──────────────────
    try:
        inc = xbrl.statements.income_statement()
        if inc:
            df = inc.to_dataframe()
            if "is_breakdown" in df.columns:
                df = df[df["is_breakdown"] == False]

            if "concept" in df.columns and period in df.columns:
                share_rows = df[
                    df["concept"].str.contains(
                        "WeightedAverageNumberOfSharesOutstandingBasic|"
                        "WeightedAverageNumberOfDilutedSharesOutstanding|"
                        "CommonStockSharesOutstanding",
                        case=False, na=False
                    ) &
                    ~df["concept"].str.contains(
                        "Preferred|Exercise|Grant|Intrinsic|PerShare|EPS",
                        case=False, na=False
                    )
                ]
                if not share_rows.empty:
                    val = share_rows[period].dropna()
                    if not val.empty:
                        v = float(val.iloc[0])
                        if 1e6 < abs(v) < 1e12:
                            return int(v)
    except Exception:
        pass

    # ── Method 3: Cash flow statement sometimes has share data ────────────────
    try:
        cf = xbrl.statements.cash_flow_statement()
        if cf:
            df = cf.to_dataframe()
            if "is_breakdown" in df.columns:
                df = df[df["is_breakdown"] == False]

            if "concept" in df.columns and period in df.columns:
                share_rows = df[
                    df["concept"].str.contains(
                        "WeightedAverageNumberOfSharesOutstandingBasic|"
                        "WeightedAverageNumberOfDilutedSharesOutstanding",
                        case=False, na=False
                    ) &
                    ~df["concept"].str.contains(
                        "Preferred|PerShare|EPS",
                        case=False, na=False
                    )
                ]
                if not share_rows.empty:
                    val = share_rows[period].dropna()
                    if not val.empty:
                        v = float(val.iloc[0])
                        if 1e6 < abs(v) < 1e12:
                            return int(v)
    except Exception:
        pass

    return None


def apply_split_adjustments(ticker, shares_by_year):
    splits = [(yr, mult) for t, yr, mult in KNOWN_SPLITS if t == ticker]
    if not splits:
        return shares_by_year
    splits.sort()
    adjusted = dict(shares_by_year)
    for split_year, multiplier in splits:
        for fy in list(adjusted.keys()):
            if fy < split_year:
                adjusted[fy] = int(adjusted[fy] * multiplier)
    print(f"    [SPLIT] {[(yr, f'{m}x') for yr, m in splits]}")
    return adjusted


def enrich_company(cur, company_id, ticker):
    """Find missing years, fetch those specific filings, extract shares."""
    cur.execute("""
        SELECT fiscal_year FROM filings
        WHERE company_id = %s AND form_type = '10-K' AND shares_outstanding IS NULL
        ORDER BY fiscal_year
    """, (company_id,))
    missing_years = set(r["fiscal_year"] for r in cur.fetchall())

    if not missing_years:
        print(f"    no gaps — skip")
        return 0

    print(f"    {len(missing_years)} missing: {sorted(missing_years)}")

    # Get all 10-K filings from edgartools
    try:
        company = Company(ticker)
        filings = company.get_filings(form="10-K", amendments=False)
        filings_list = list(filings)
    except Exception as e:
        print(f"    [ERROR] get_filings: {e}")
        return 0

    shares_found = {}
    for filing in filings_list:
        period = str(filing.period_of_report or "")
        if not period:
            continue
        fy = int(period[:4])
        if fy not in missing_years:
            continue

        print(f"      FY{fy} ({filing.filing_date})...", end=" ")
        shares = get_shares_from_filing(filing, period)
        if shares:
            shares_found[fy] = shares
            print(f"{shares:,}")
        else:
            print("not found")

    if not shares_found:
        print(f"    no shares extracted")
        return 0

    # Apply split adjustments
    shares_found = apply_split_adjustments(ticker, shares_found)

    # Update DB
    updated = 0
    for fy, shares in shares_found.items():
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
        description="Backfill shares_outstanding from 10-K filings")
    parser.add_argument("--ticker", help="Enrich one company only")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    # Only companies with missing shares data
    if args.ticker:
        cur.execute("""
            SELECT DISTINCT c.id, c.ticker, c.cik
            FROM companies c
            JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
            WHERE f.shares_outstanding IS NULL AND c.ticker = %s
        """, (args.ticker.upper(),))
    else:
        cur.execute("""
            SELECT DISTINCT c.id, c.ticker, c.cik
            FROM companies c
            JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
            WHERE f.shares_outstanding IS NULL
            ORDER BY c.ticker
        """)

    companies = cur.fetchall()
    if not companies:
        print("No companies with missing shares data")
        return

    print(f"[INFO] {len(companies)} companies with gaps\n")

    total_updated = 0
    for co in companies:
        ticker = co["ticker"]
        print(f"  [{ticker}]")
        try:
            updated = enrich_company(cur, co["id"], ticker)
            total_updated += updated
            conn.commit()
        except Exception as e:
            print(f"    [ERROR] {e}")
            conn.rollback()

    # Coverage
    print(f"\n{'='*65}")
    print(f"SHARES ENRICHMENT — {total_updated} values updated")
    print(f"{'='*65}")

    cur.execute("""
        SELECT c.ticker,
               COUNT(*) AS total,
               COUNT(f.shares_outstanding) AS has,
               COUNT(*) - COUNT(f.shares_outstanding) AS miss
        FROM companies c
        JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
        GROUP BY c.id, c.ticker
        HAVING COUNT(*) - COUNT(f.shares_outstanding) > 0
        ORDER BY COUNT(*) - COUNT(f.shares_outstanding) DESC
    """)
    rows = cur.fetchall()

    if rows:
        print(f"\n  Remaining gaps:")
        print(f"  {'Ticker':<8} {'Has':>4} {'/ Tot':>6} {'Miss':>5}")
        print(f"  {'-'*8} {'-'*4} {'-'*6} {'-'*5}")
        for r in rows:
            print(f"  {r['ticker']:<8} {r['has']:>4} / {r['total']:<4} {r['miss']:>5}")
    else:
        print("\n  All companies have full shares coverage!")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
