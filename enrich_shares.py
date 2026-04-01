"""
enrich_shares.py — Backfill shares_outstanding by re-reading each 10-K filing.

For each filing already fetched via edgartools, opens balance sheet XBRL
and sums ALL common stock share classes (handles META Class A+B, GOOGL A+B+C).

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all with gaps
    DATABASE_URL=... python3 enrich_shares.py --ticker META  # one company
    DATABASE_URL=... python3 enrich_shares.py --all          # all companies (even no gaps)
"""

import os
import argparse
import psycopg2
import psycopg2.extras

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL = os.environ["DATABASE_URL"]
START_YEAR = 2009


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def extract_shares_from_filing(filing, period):
    """
    Extract total shares outstanding from a 10-K filing's balance sheet.
    Sums all CommonStock share rows across all classes (A, B, C).
    Does NOT filter is_breakdown — gets everything.
    """
    try:
        xbrl = filing.xbrl()
        if not xbrl:
            return None
    except Exception:
        return None

    # ── Balance sheet: sum all common stock share classes ──────────────────────
    try:
        bs = xbrl.statements.balance_sheet()
        if not bs:
            return None
        df = bs.to_dataframe()

        if "concept" not in df.columns or period not in df.columns:
            return None

        # Find rows: concept contains CommonStock, exclude value/amount/par rows
        share_rows = df[
            df["concept"].str.contains("CommonStock", case=False, na=False) &
            df["concept"].str.contains("Shares", case=False, na=False) &
            ~df["concept"].str.contains(
                "Preferred|Value|Amount|Par|Authorized|Treasury",
                case=False, na=False
            )
        ]

        if share_rows.empty:
            # Broader fallback: EntityCommonStockSharesOutstanding
            share_rows = df[
                df["concept"].str.contains(
                    "SharesOutstanding|SharesIssued",
                    case=False, na=False
                ) &
                ~df["concept"].str.contains(
                    "Preferred|Treasury|Authorized",
                    case=False, na=False
                )
            ]

        if share_rows.empty:
            return None

        vals = share_rows[period].dropna()
        if vals.empty:
            return None

        total = vals.sum()
        if total > 1e6:  # sanity: at least 1M shares
            return int(total)

    except Exception:
        pass

    # ── Income statement fallback: weighted average ───────────────────────────
    try:
        inc = xbrl.statements.income_statement()
        if not inc:
            return None
        df = inc.to_dataframe()

        if "concept" not in df.columns or period not in df.columns:
            return None

        wa_rows = df[
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

        if not wa_rows.empty:
            vals = wa_rows[period].dropna()
            if not vals.empty:
                v = float(vals.iloc[0])
                if v > 1e6:
                    return int(v)
    except Exception:
        pass

    return None


def enrich_company(cur, company_id, ticker):
    """Fetch all 10-K filings and extract shares for missing years."""
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

    try:
        company = Company(ticker)
        filings = list(company.get_filings(form="10-K", amendments=False))
    except Exception as e:
        print(f"    [ERROR] get_filings: {e}")
        return 0

    updated = 0
    for filing in filings:
        period = str(filing.period_of_report or "")
        if not period:
            continue
        fy = int(period[:4])
        if fy < START_YEAR or fy not in missing_years:
            continue

        print(f"      FY{fy}...", end=" ")
        shares = extract_shares_from_filing(filing, period)

        if shares:
            print(f"{shares:,}")
            cur.execute("""
                UPDATE filings SET shares_outstanding = %s
                WHERE company_id = %s AND fiscal_year = %s AND form_type = '10-K'
                  AND shares_outstanding IS NULL
            """, (shares, company_id, fy))
            if cur.rowcount > 0:
                updated += 1
                missing_years.discard(fy)
        else:
            print("not found")

    if updated > 0:
        from fetch_sbc import refresh_metrics
        refresh_metrics(cur, company_id)
        print(f"    updated {updated} years")

    if missing_years:
        print(f"    still missing: {sorted(missing_years)}")

    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Backfill shares_outstanding from 10-K balance sheets")
    parser.add_argument("--ticker", help="Enrich one company only")
    parser.add_argument("--all", action="store_true",
                        help="Process all companies, not just those with gaps")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    if args.ticker:
        cur.execute("SELECT id, ticker FROM companies WHERE ticker = %s",
                    (args.ticker.upper(),))
    elif args.all:
        cur.execute("SELECT id, ticker FROM companies ORDER BY ticker")
    else:
        cur.execute("""
            SELECT DISTINCT c.id, c.ticker
            FROM companies c
            JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
            WHERE f.shares_outstanding IS NULL
            ORDER BY c.ticker
        """)

    companies = cur.fetchall()
    if not companies:
        print("No companies to process")
        return

    print(f"[INFO] {len(companies)} companies\n")

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
    print(f"\n{'='*55}")
    print(f"SHARES — {total_updated} values updated")
    print(f"{'='*55}")

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

    gaps = [r for r in rows if r["miss"] > 0]
    full = [r for r in rows if r["miss"] == 0]

    if gaps:
        print(f"\n  Remaining gaps ({len(gaps)}):")
        print(f"  {'Ticker':<8} {'Has':>4} / {'Tot':<4} {'Miss':>5}")
        for r in gaps:
            print(f"  {r['ticker']:<8} {r['has']:>4} / {r['total']:<4} {r['miss']:>5}")

    print(f"\n  Complete: {len(full)} / {len(rows)} companies")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
