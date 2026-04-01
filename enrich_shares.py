"""
enrich_shares.py — Backfill shares_outstanding using yfinance.

yfinance provides clean, split-adjusted historical shares outstanding
via get_shares_full(). No manual split math needed.

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all with gaps
    DATABASE_URL=... python3 enrich_shares.py --ticker AAPL  # one company
    DATABASE_URL=... python3 enrich_shares.py --force        # overwrite existing
"""

import os
import argparse
import psycopg2
import psycopg2.extras
import pandas as pd
import yfinance as yf

DATABASE_URL = os.environ["DATABASE_URL"]


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def get_historical_shares(ticker):
    """
    Get split-adjusted shares outstanding from yfinance.
    Returns {year: shares_count} with end-of-year values.
    """
    stock = yf.Ticker(ticker)

    try:
        shares_history = stock.get_shares_full()
    except Exception as e:
        print(f"    [WARN] get_shares_full() failed: {e}")
        return {}

    if shares_history is None or shares_history.empty:
        print(f"    [WARN] no shares data from yfinance")
        return {}

    # Convert to annual — take last value per calendar year
    shares_df = shares_history.to_frame(name="shares")
    shares_df.index = pd.to_datetime(shares_df.index)
    shares_df["year"] = shares_df.index.year
    annual = shares_df.groupby("year")["shares"].last()

    result = {}
    for year, val in annual.items():
        if pd.notna(val) and val > 0:
            result[int(year)] = int(val)

    return result


def enrich_company(cur, company_id, ticker, force=False):
    """Backfill shares_outstanding for filings where it's null."""
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
        print(f"    no gaps — skip")
        return 0

    print(f"    {len(target_years)} years to fill: {sorted(target_years)}")

    shares_by_year = get_historical_shares(ticker)
    if not shares_by_year:
        return 0

    print(f"    yfinance: {len(shares_by_year)} years "
          f"({min(shares_by_year)}–{max(shares_by_year)})")

    # Show what we got
    for fy in sorted(shares_by_year):
        if fy in target_years:
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
        description="Backfill shares_outstanding from yfinance")
    parser.add_argument("--ticker", help="Enrich one company only")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing shares values")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    if args.ticker:
        cur.execute("SELECT id, ticker FROM companies WHERE ticker = %s",
                    (args.ticker.upper(),))
    else:
        # Only companies with gaps
        cur.execute("""
            SELECT DISTINCT c.id, c.ticker
            FROM companies c
            JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
            WHERE f.shares_outstanding IS NULL
            ORDER BY c.ticker
        """)

    companies = cur.fetchall()
    if not companies:
        print("No companies with missing shares data")
        return

    print(f"[INFO] {len(companies)} companies to process\n")

    total_updated = 0
    for co in companies:
        ticker = co["ticker"]
        print(f"  [{ticker}]")
        try:
            updated = enrich_company(cur, co["id"], ticker,
                                     force=args.force)
            total_updated += updated
            conn.commit()
        except Exception as e:
            print(f"    [ERROR] {e}")
            conn.rollback()

    # Coverage report
    print(f"\n{'='*60}")
    print(f"SHARES ENRICHMENT — {total_updated} values updated")
    print(f"{'='*60}")

    cur.execute("""
        SELECT c.ticker,
               COUNT(*) AS total,
               COUNT(f.shares_outstanding) AS has,
               COUNT(*) - COUNT(f.shares_outstanding) AS miss
        FROM companies c
        JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
        GROUP BY c.id, c.ticker
        ORDER BY (COUNT(*) - COUNT(f.shares_outstanding)) DESC, c.ticker
    """)
    rows = cur.fetchall()

    has_gaps = [r for r in rows if r["miss"] > 0]
    no_gaps  = [r for r in rows if r["miss"] == 0]

    if has_gaps:
        print(f"\n  Remaining gaps ({len(has_gaps)} companies):")
        print(f"  {'Ticker':<8} {'Has':>4} {'/ Tot':>6} {'Miss':>5}")
        print(f"  {'-'*8} {'-'*4} {'-'*6} {'-'*5}")
        for r in has_gaps:
            print(f"  {r['ticker']:<8} {r['has']:>4} / {r['total']:<4} {r['miss']:>5}")

    print(f"\n  Complete: {len(no_gaps)} companies with full shares coverage")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
