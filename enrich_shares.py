"""
enrich_shares.py — Backfill shares_outstanding from 10-K cover page text.

Every 10-K is legally required to state shares outstanding on the cover page.
Extracts via regex from filing.text(), with DEI XBRL fact as fallback.

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all with gaps
    DATABASE_URL=... python3 enrich_shares.py --ticker META  # one company
"""

import os
import re
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


def get_shares_from_cover_page(filing):
    """
    Extract shares outstanding from the cover page text of a 10-K.
    Falls back to DEI XBRL fact if regex fails.
    """
    # ── Method 1: cover page text regex ───────────────────────────────────────
    try:
        text = filing.text()
        if text:
            shares = _parse_shares_from_text(text)
            if shares:
                return shares, "cover_page"
    except Exception as e:
        print(f"      text() error: {e}")

    # ── Method 2: DEI XBRL fact ───────────────────────────────────────────────
    try:
        xbrl = filing.xbrl()
        if xbrl and hasattr(xbrl, 'instance') and xbrl.instance:
            try:
                facts = xbrl.instance.query_facts(
                    concept="dei:EntityCommonStockSharesOutstanding"
                )
                if facts is not None and not facts.empty:
                    val = float(facts["value"].iloc[0])
                    if 1e7 < val < 1e11:
                        return int(val), "dei_xbrl"
            except Exception:
                pass
    except Exception:
        pass

    return None, None


def _parse_shares_from_text(text):
    """Parse shares outstanding from cover page text (first 10K chars)."""
    # Search cover page area
    for limit in (5000, 10000, 20000):
        cover = text[:limit].lower()
        result = _try_patterns(cover)
        if result:
            return result
    return None


def _try_patterns(text):
    """Try multiple regex patterns to find share counts."""
    patterns = [
        # "15,115,823,000 shares of common stock outstanding"
        r"([\d,]+)\s+shares\s+of\s+(?:the\s+)?(?:registrant.s?\s+)?common\s+stock[^.]*?outstanding",
        # "common stock outstanding ... 15,115,823,000"
        r"common\s+stock[^.]*?outstanding[^.]*?([\d,]{8,})",
        # "X shares of Class A common stock outstanding"
        r"([\d,]+)\s+shares\s+of\s+class\s+[a-c]\s+common\s+stock[^.]*?outstanding",
        # "there were 15,115,823,000 shares ... outstanding"
        r"(?:there\s+were|were)\s+([\d,]+)\s+(?:shares|common\s+shares)[^.]*?outstanding",
        # "shares outstanding ... 15,115,823,000"
        r"shares\s+outstanding[^.]{0,60}?([\d,]{8,})",
        # "X shares of common stock, par value..."
        r"([\d,]{8,})\s+shares\s+of\s+(?:our\s+)?common\s+stock",
    ]

    all_found = []

    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
        for match in matches:
            val_str = match.replace(",", "").strip()
            try:
                val = float(val_str)
                if 1e7 < val < 1e11:
                    all_found.append(int(val))
            except (ValueError, TypeError):
                continue

    if not all_found:
        return None

    # For multi-class companies (META, GOOGL), the cover page lists
    # each class separately. Sum all unique large values found.
    # But avoid double-counting: if one value is roughly the sum of others,
    # use the larger value (it's the total).
    all_found = sorted(set(all_found), reverse=True)

    if len(all_found) == 1:
        return all_found[0]

    # Check if the largest is roughly the sum of the rest (it's a total)
    largest = all_found[0]
    rest_sum = sum(all_found[1:])
    if rest_sum > 0 and 0.8 < largest / rest_sum < 1.2:
        # Largest ≈ sum of rest → it's a pre-summed total, use it
        return largest

    # Otherwise sum all (multi-class, no total provided)
    total = sum(all_found)
    return total


def enrich_company(cur, company_id, ticker):
    """Find missing years, fetch those filings, extract shares from cover page."""
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
        shares, method = get_shares_from_cover_page(filing)

        if shares:
            print(f"{shares:,} ({method})")
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
        description="Backfill shares_outstanding from 10-K cover pages")
    parser.add_argument("--ticker", help="One company only")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    if args.ticker:
        cur.execute("SELECT id, ticker FROM companies WHERE ticker = %s",
                    (args.ticker.upper(),))
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
