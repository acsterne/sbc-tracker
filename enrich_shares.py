"""
enrich_shares.py — Backfill shares_outstanding for all years where it is null.

Uses EDGAR companyfacts API via edgartools to get shares outstanding data,
then updates filings rows that are missing shares_outstanding.

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all companies
    DATABASE_URL=... python3 enrich_shares.py --ticker META  # one company
"""

import os
import argparse
import psycopg2
import psycopg2.extras
from datetime import date

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL = os.environ["DATABASE_URL"]

SHARE_CONCEPTS = [
    "CommonStockSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesIssuedAndOutstanding",
]


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def get_shares_from_facts(ticker):
    """
    Pull shares outstanding from EDGAR companyfacts API via edgartools.
    Returns dict: {fiscal_year: shares_value} for 10-K periods.
    """
    company = Company(ticker)

    try:
        facts = company.get_facts()
    except Exception as e:
        print(f"    [WARN] get_facts() failed: {e}")
        return {}

    shares_by_year = {}

    for concept in SHARE_CONCEPTS:
        try:
            df = facts.to_pandas(concept)
        except Exception:
            continue

        if df is None or df.empty:
            continue

        print(f"    found {concept}: {len(df)} data points")
        print(f"    columns: {list(df.columns)}")

        # Look for form and end date columns
        # edgartools facts dataframe typically has: val, end, form, accn, filed, etc.
        form_col = None
        for col in df.columns:
            if col.lower() in ("form", "form_type"):
                form_col = col
                break

        end_col = None
        for col in df.columns:
            if col.lower() in ("end", "period_end", "period"):
                end_col = col
                break

        val_col = None
        for col in df.columns:
            if col.lower() in ("val", "value"):
                val_col = col
                break

        if not val_col:
            print(f"    [WARN] no value column found, columns: {list(df.columns)}")
            # Try using the dataframe index or first numeric column
            for col in df.columns:
                if df[col].dtype in ("float64", "int64", "float32", "int32"):
                    val_col = col
                    break

        if not val_col:
            continue

        for _, row in df.iterrows():
            # Filter to 10-K only if form column exists
            if form_col:
                form = str(row.get(form_col, ""))
                if form not in ("10-K", "10-K/A"):
                    continue

            # Get the period end date
            end_date = None
            if end_col:
                end_raw = row.get(end_col)
                if end_raw:
                    try:
                        end_date = str(end_raw)[:10]
                        fiscal_year = int(end_date[:4])
                    except (ValueError, TypeError):
                        continue
            else:
                continue

            val = row.get(val_col)
            if val is None or val <= 0:
                continue

            shares = int(float(val))

            # Keep the latest value per fiscal year (in case of amendments)
            if fiscal_year not in shares_by_year or shares > shares_by_year[fiscal_year]:
                shares_by_year[fiscal_year] = shares

        if shares_by_year:
            print(f"    extracted {len(shares_by_year)} years from {concept}")
            break  # Use first concept that works

    return shares_by_year


def enrich_company(cur, company_id, ticker):
    """Backfill shares_outstanding for filings where it's null."""
    # Find years missing shares
    cur.execute("""
        SELECT fiscal_year FROM filings
        WHERE company_id = %s AND form_type = '10-K' AND shares_outstanding IS NULL
        ORDER BY fiscal_year
    """, (company_id,))
    missing_years = [r["fiscal_year"] for r in cur.fetchall()]

    if not missing_years:
        print(f"    no missing shares — skip")
        return 0

    print(f"    {len(missing_years)} years missing shares: {missing_years}")

    # Get shares data from EDGAR
    shares_by_year = get_shares_from_facts(ticker)
    if not shares_by_year:
        print(f"    [WARN] no shares data from EDGAR")
        return 0

    # Update missing rows
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
            print(f"    FY{fy}: shares = {shares:,}")

    # Recompute metrics for this company
    if updated > 0:
        from fetch_sbc import refresh_metrics
        refresh_metrics(cur, company_id)

    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Backfill shares_outstanding from EDGAR companyfacts")
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
    print(f"\n{'='*50}")
    print(f"SHARES ENRICHMENT COMPLETE")
    print(f"  Total values backfilled: {total_updated}")
    print(f"{'='*50}")

    # Show current shares coverage
    cur.execute("""
        SELECT c.ticker,
               COUNT(*) AS total_years,
               COUNT(f.shares_outstanding) AS years_with_shares,
               MAX(f.shares_outstanding) AS latest_shares
        FROM companies c
        JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
        GROUP BY c.id, c.ticker
        ORDER BY c.ticker
    """)
    rows = cur.fetchall()
    print(f"\n  {'Ticker':<8} {'With Shares':>12} {'/ Total':>8} {'Latest Shares':>16}")
    print(f"  {'-'*8} {'-'*12} {'-'*8} {'-'*16}")
    for r in rows:
        shr = f"{r['latest_shares']:,}" if r['latest_shares'] else "—"
        print(f"  {r['ticker']:<8} {r['years_with_shares']:>12} {'/ ' + str(r['total_years']):>8} {shr:>16}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
