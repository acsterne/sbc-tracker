"""
enrich_shares.py — Backfill shares_outstanding from multiple EDGAR sources.

Three sources tried in order per fiscal year gap:
  1. DEI EntityCommonStockSharesOutstanding (companyfacts API)
  2. us-gaap CommonStockSharesOutstanding (companyfacts API, sums multi-class)
  3. 10-K cover page regex via edgartools (final fallback)

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all with gaps
    DATABASE_URL=... python3 enrich_shares.py --ticker META  # one company
    DATABASE_URL=... python3 enrich_shares.py --all          # all companies even if no gaps
"""

import os
import re
import sys
import time
import argparse
import requests
import psycopg2
import psycopg2.extras
from collections import defaultdict

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL = os.environ["DATABASE_URL"]
START_YEAR = 2009

EDGAR_BASE = "https://data.sec.gov"
HEADERS = {
    "User-Agent": "SBC Tracker research@sbctracker.io",
    "Accept": "application/json",
}

# ANSI colors
RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def fetch_companyfacts(cik):
    """Fetch companyfacts JSON from EDGAR XBRL API."""
    url = f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        time.sleep(0.11)
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"      [WARN] companyfacts fetch failed: {e}")
    return None


# ── SOURCE 1: DEI EntityCommonStockSharesOutstanding ─────────────────────────

def source1_dei(facts, missing_years):
    """Extract from facts['dei']['EntityCommonStockSharesOutstanding']."""
    result = {}
    try:
        entries = facts["dei"]["EntityCommonStockSharesOutstanding"]["units"]["shares"]
    except (KeyError, TypeError):
        return result

    for entry in entries:
        form = entry.get("form", "")
        if form != "10-K":
            continue
        end = entry.get("end", "")
        if not end:
            continue
        fy = int(end[:4])
        if fy < START_YEAR or fy not in missing_years:
            continue
        val = entry.get("val")
        if val and 1e6 < val < 1e12:
            # If multiple entries for same year, take the latest filed
            existing = result.get(fy)
            if existing is None:
                result[fy] = int(val)
            else:
                # Keep larger (later filings may include all classes)
                filed = entry.get("filed", "")
                result[fy] = int(val)  # last entry wins (they're typically chronological)

    return result


# ── SOURCE 2: us-gaap CommonStockSharesOutstanding ──────────────────────────

def source2_usgaap(facts, missing_years):
    """
    Extract from facts['us-gaap']['CommonStockSharesOutstanding'].
    For multi-class companies (GOOGL, META), group by accession number and sum.
    Deduplicate to one row per fiscal_year by taking the most recently filed accession.
    """
    result = {}
    try:
        entries = facts["us-gaap"]["CommonStockSharesOutstanding"]["units"]["shares"]
    except (KeyError, TypeError):
        return result

    # Group by (fiscal_year, accn) to handle multi-class shares
    by_year_accn = defaultdict(lambda: {"values": [], "filed": ""})

    for entry in entries:
        form = entry.get("form", "")
        if form != "10-K":
            continue
        end = entry.get("end", "")
        if not end:
            continue
        fy = int(end[:4])
        if fy < START_YEAR or fy not in missing_years:
            continue
        val = entry.get("val")
        if not val or val < 1e4:
            continue
        accn = entry.get("accn", "unknown")
        filed = entry.get("filed", "")
        key = (fy, accn)
        by_year_accn[key]["values"].append(int(val))
        by_year_accn[key]["filed"] = max(by_year_accn[key]["filed"], filed)

    # For each fiscal year, pick the accession with the latest filing date, then sum values
    year_accessions = defaultdict(list)
    for (fy, accn), data in by_year_accn.items():
        year_accessions[fy].append((data["filed"], accn, sum(data["values"])))

    for fy, accns in year_accessions.items():
        # Sort by filed date descending, take the most recent
        accns.sort(reverse=True)
        total = accns[0][2]
        if 1e6 < total < 1e12:
            result[fy] = total

    return result


# ── SOURCE 3: 10-K cover page regex ─────────────────────────────────────────

def source3_cover_page(ticker, missing_years):
    """Fetch 10-K filings via edgartools and extract shares from cover page text."""
    result = {}
    try:
        company = Company(ticker)
        filings = list(company.get_filings(form="10-K", amendments=False))
    except Exception as e:
        print(f"      [WARN] edgartools get_filings failed: {e}")
        return result

    for filing in filings:
        period = str(filing.period_of_report or "")
        if not period:
            continue
        fy = int(period[:4])
        if fy < START_YEAR or fy not in missing_years:
            continue

        shares = _extract_from_filing(filing)
        if shares:
            result[fy] = shares

    return result


def _extract_from_filing(filing):
    """Try cover page regex, then DEI XBRL from individual filing."""
    # Cover page text regex
    try:
        text = filing.text()
        if text:
            shares = _parse_shares_from_text(text)
            if shares:
                return shares
    except Exception:
        pass

    # DEI XBRL from individual filing
    try:
        xbrl = filing.xbrl()
        if xbrl and hasattr(xbrl, 'instance') and xbrl.instance:
            facts = xbrl.instance.query_facts(
                concept="dei:EntityCommonStockSharesOutstanding"
            )
            if facts is not None and not facts.empty:
                val = float(facts["value"].iloc[0])
                if 1e7 < val < 1e11:
                    return int(val)
    except Exception:
        pass

    return None


def _parse_shares_from_text(text):
    """Parse shares outstanding from cover page text using expanding windows."""
    for limit in (5000, 10000, 20000, 40000):
        cover = text[:limit].lower()
        result = _try_patterns(cover)
        if result:
            return result
    return None


def _try_patterns(text):
    """Try multiple regex patterns to find share counts."""
    patterns = [
        r"([\d,]+)\s+shares\s+of\s+(?:the\s+)?(?:registrant.s?\s+)?(?:issuer.s?\s+)?common\s+stock[^.]*?outstanding",
        r"common\s+stock[^.]*?outstanding[^.]*?([\d,]{8,})",
        r"([\d,]+)\s+shares\s+of\s+class\s+[a-c]\s+common\s+stock[^.]*?outstanding",
        r"(?:there\s+were|were)\s+([\d,]+)\s+(?:shares|common\s+shares)[^.]*?outstanding",
        r"shares\s+outstanding[^.]{0,80}?([\d,]{8,})",
        r"([\d,]{8,})\s+shares\s+of\s+(?:our\s+)?(?:the\s+)?(?:issuer.s?\s+)?common\s+stock",
        r"outstanding[:\s]+([\d,]{8,})",
        r"common\s+stock[^.]*?issued\s+and\s+outstanding[^.]*?([\d,]{8,})",
        r"([\d,]{10,})[^.]{0,30}?(?:shares|outstanding)",
        r"(?:shares|outstanding)[^.]{0,30}?([\d,]{10,})",
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

    all_found = sorted(set(all_found), reverse=True)
    if len(all_found) == 1:
        return all_found[0]

    # If the largest is roughly the sum of the rest, it's a pre-summed total
    largest = all_found[0]
    rest_sum = sum(all_found[1:])
    if rest_sum > 0 and 0.8 < largest / rest_sum < 1.2:
        return largest

    # Otherwise sum all (multi-class, no total provided)
    return sum(all_found)


# ── Main enrichment logic ───────────────────────────────────────────────────

def enrich_company(cur, company_id, ticker, cik):
    """Find missing years, try all sources, update DB."""
    cur.execute("""
        SELECT fiscal_year FROM filings
        WHERE company_id = %s AND form_type = '10-K' AND shares_outstanding IS NULL
        ORDER BY fiscal_year
    """, (company_id,))
    missing_years = set(r["fiscal_year"] for r in cur.fetchall())

    if not missing_years:
        print(f"    no gaps — skip")
        return {"filled": 0, "before": 0, "still_missing": 0, "sources": {}}

    gaps_before = len(missing_years)
    print(f"    {gaps_before} gaps: {sorted(missing_years)}")

    source_counts = defaultdict(int)
    filled = 0

    # ── Source 1 & 2: companyfacts API (single HTTP call for both) ────────
    facts = fetch_companyfacts(cik)
    if facts:
        # Source 1: DEI
        dei_shares = source1_dei(facts, missing_years)
        for fy, val in dei_shares.items():
            if fy in missing_years:
                if _update_shares(cur, company_id, fy, val):
                    print(f"      FY{fy}: {val:,} (dei)")
                    missing_years.discard(fy)
                    source_counts["dei"] += 1
                    filled += 1

        # Source 2: us-gaap (only for remaining gaps)
        if missing_years:
            usgaap_shares = source2_usgaap(facts, missing_years)
            for fy, val in usgaap_shares.items():
                if fy in missing_years:
                    if _update_shares(cur, company_id, fy, val):
                        print(f"      FY{fy}: {val:,} (us-gaap)")
                        missing_years.discard(fy)
                        source_counts["us-gaap"] += 1
                        filled += 1

    # ── Source 3: cover page regex (only for remaining gaps) ──────────────
    if missing_years:
        cover_shares = source3_cover_page(ticker, missing_years)
        for fy, val in cover_shares.items():
            if fy in missing_years:
                if _update_shares(cur, company_id, fy, val):
                    print(f"      FY{fy}: {val:,} (cover_page)")
                    missing_years.discard(fy)
                    source_counts["cover_page"] += 1
                    filled += 1

    # Refresh metrics if we updated anything
    if filled > 0:
        from fetch_sbc import refresh_metrics
        refresh_metrics(cur, company_id)
        print(f"    filled {filled}/{gaps_before}")

    if missing_years:
        print(f"    {RED}still missing: {sorted(missing_years)}{RESET}")

    return {
        "filled": filled,
        "before": gaps_before,
        "still_missing": len(missing_years),
        "sources": dict(source_counts),
    }


def _update_shares(cur, company_id, fiscal_year, value):
    """Update shares_outstanding only if still NULL. Returns True if updated."""
    cur.execute("""
        UPDATE filings SET shares_outstanding = %s
        WHERE company_id = %s AND fiscal_year = %s AND form_type = '10-K'
          AND shares_outstanding IS NULL
    """, (value, company_id, fiscal_year))
    return cur.rowcount > 0


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Backfill shares_outstanding from EDGAR sources")
    parser.add_argument("--ticker", help="Process one company only")
    parser.add_argument("--all", action="store_true",
                        help="Process all companies even if no gaps")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    if args.ticker:
        cur.execute("SELECT id, ticker, cik FROM companies WHERE ticker = %s",
                    (args.ticker.upper(),))
    elif args.all:
        cur.execute("SELECT id, ticker, cik FROM companies ORDER BY ticker")
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
        print("No companies to process")
        return

    print(f"[INFO] Processing {len(companies)} companies\n")

    results = []
    for i, co in enumerate(companies):
        ticker = co["ticker"]
        cik = co["cik"]
        print(f"  [{ticker}] (CIK {cik})")
        try:
            stats = enrich_company(cur, co["id"], ticker, cik)
            stats["ticker"] = ticker
            results.append(stats)
            conn.commit()
        except Exception as e:
            print(f"    [ERROR] {e}")
            conn.rollback()
            results.append({
                "ticker": ticker, "filled": 0, "before": 0,
                "still_missing": 0, "sources": {}, "error": str(e),
            })

        # Sleep between companies to respect EDGAR rate limits
        if i < len(companies) - 1:
            time.sleep(0.5)

    # ── Summary matrix ────────────────────────────────────────────────────
    print(f"\n{'='*75}")
    total_filled = sum(r["filled"] for r in results)
    print(f"SHARES ENRICHMENT — {total_filled} values filled")
    print(f"{'='*75}\n")

    # Header
    print(f"  {'Ticker':<8} {'Before':>7} {'Filled':>7} {'Missing':>8}  Sources")
    print(f"  {'-'*8} {'-'*7} {'-'*7} {'-'*8}  {'-'*25}")

    for r in sorted(results, key=lambda x: x.get("still_missing", 0), reverse=True):
        ticker = r["ticker"]
        before = r["before"]
        filled = r["filled"]
        still = r.get("still_missing", 0)
        sources = r.get("sources", {})

        src_str = ", ".join(f"{k}={v}" for k, v in sorted(sources.items())) if sources else "—"

        if still > 0:
            line = f"  {ticker:<8} {before:>7} {filled:>7} {RED}{still:>8}{RESET}  {src_str}"
        elif filled > 0:
            line = f"  {ticker:<8} {before:>7} {GREEN}{filled:>7}{RESET} {still:>8}  {src_str}"
        else:
            line = f"  {ticker:<8} {before:>7} {filled:>7} {still:>8}  {src_str}"

        print(line)

    # Overall coverage
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

    print(f"\n  Overall: {len(full)}/{len(rows)} companies fully covered")
    if gaps:
        print(f"  {RED}{len(gaps)} companies still have gaps:{RESET}")
        for r in gaps:
            print(f"    {r['ticker']:<8} {r['has']}/{r['total']} years filled ({r['miss']} missing)")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
