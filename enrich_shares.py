"""
enrich_shares.py — Backfill shares_outstanding from EDGAR companyfacts API.

Uses search_concepts() to find all share-related tags per company,
merges multiple concepts to maximize year coverage, applies stock
split adjustments, and updates filings rows.

Usage:
    DATABASE_URL=... python3 enrich_shares.py               # all companies
    DATABASE_URL=... python3 enrich_shares.py --ticker META  # one company
    DATABASE_URL=... python3 enrich_shares.py --force        # overwrite existing
    DATABASE_URL=... python3 enrich_shares.py --gaps-only    # only companies with missing data
"""

import os
import argparse
import psycopg2
import psycopg2.extras

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL = os.environ["DATABASE_URL"]

# Concept preference order (higher = preferred when same year has multiple values)
CONCEPT_PRIORITY = {
    "CommonStockSharesOutstanding": 100,
    "EntityCommonStockSharesOutstanding": 95,
    "CommonStockSharesIssued": 80,
    "CommonStockSharesIssuedAndOutstanding": 80,
    "WeightedAverageNumberOfDilutedSharesOutstanding": 60,
    "WeightedAverageNumberOfSharesOutstandingBasic": 50,
    "WeightedAverageNumberOfShareOutstandingBasicAndDiluted": 50,
}

# Labels/keywords to exclude (not actual share counts)
EXCLUDE_KEYWORDS = [
    "preferred", "exercise", "grant", "intrinsic", "forfeit",
    "option", "warrant", "convertible", "treasury", "repurchase",
    "vested", "nonvested", "restricted", "performance",
    "price", "cost", "fair value", "compensation",
]

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


def get_concept_priority(concept_name):
    """Score a concept name — higher means more preferred."""
    for key, score in CONCEPT_PRIORITY.items():
        if key.lower() in concept_name.lower():
            return score
    return 10  # unknown concepts get low priority


def is_valid_share_concept(concept):
    """Filter out concepts that aren't actual share counts."""
    label = str(getattr(concept, 'label', '') or concept).lower()
    name = str(getattr(concept, 'name', '') or str(concept)).lower()
    combined = label + " " + name

    for kw in EXCLUDE_KEYWORDS:
        if kw in combined:
            return False
    return True


def get_shares_from_facts(ticker):
    """
    Find all share-count concepts via search_concepts(), merge by fiscal year,
    preferring higher-priority concepts when multiple have data for same year.
    Returns dict: {fiscal_year: shares_value}
    """
    company = Company(ticker)
    try:
        facts = company.get_facts()
    except Exception as e:
        print(f"    [WARN] get_facts() failed: {e}")
        return {}

    # ── Find candidate concepts via search_concepts ───────────────────────────
    candidates = []
    for query in ["shares outstanding", "shares issued", "shares basic"]:
        try:
            results = facts.search_concepts(query)
            if results:
                candidates.extend(results)
        except Exception:
            continue

    if not candidates:
        print(f"    [WARN] search_concepts found nothing")
        # Fall back to direct time_series attempts
        return _fallback_time_series(facts)

    # Deduplicate by concept name
    seen = set()
    unique = []
    for c in candidates:
        name = str(getattr(c, 'name', c))
        if name not in seen:
            seen.add(name)
            unique.append(c)

    # Filter to valid share-count concepts
    valid = [c for c in unique if is_valid_share_concept(c)]
    print(f"    search_concepts: {len(candidates)} found, {len(valid)} valid after filtering")

    # ── Extract data from each valid concept ──────────────────────────────────
    # shares_by_year: {fy: (priority, value)}
    merged = {}

    for concept in valid:
        concept_name = str(getattr(concept, 'name', concept))
        priority = get_concept_priority(concept_name)

        # Try time_series with the concept name
        years_found = _extract_years_from_concept(facts, concept_name)

        if years_found:
            print(f"      {concept_name}: {len(years_found)} years "
                  f"({min(years_found)}–{max(years_found)}) priority={priority}")

            for fy, val in years_found.items():
                if fy not in merged or priority > merged[fy][0]:
                    merged[fy] = (priority, val)

    shares_by_year = {fy: val for fy, (_, val) in merged.items()}

    if shares_by_year:
        print(f"    merged: {len(shares_by_year)} years "
              f"({min(shares_by_year)}–{max(shares_by_year)})")

    return shares_by_year


def _extract_years_from_concept(facts, concept_name):
    """Try time_series() for a concept, return {fy: value} dict."""
    # Try with and without namespace prefix
    names_to_try = [concept_name]
    if ":" not in concept_name:
        names_to_try = [
            f"dei:{concept_name}",
            f"us-gaap:{concept_name}",
            concept_name,
        ]

    for name in names_to_try:
        try:
            df = facts.time_series(name, periods=40)
            if df is None or df.empty:
                continue

            if "fiscal_period" in df.columns:
                annual = df[df["fiscal_period"] == "FY"]
            else:
                annual = df

            years = {}
            for _, row in annual.iterrows():
                fy = None
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
                            if val > 0 and fy:
                                if fy not in years or val > years[fy]:
                                    years[fy] = val
                        except (ValueError, TypeError):
                            pass
                        break

            if years:
                return years

        except Exception:
            continue

    return {}


def _fallback_time_series(facts):
    """Direct time_series attempts if search_concepts returns nothing."""
    FALLBACK_CONCEPTS = [
        "dei:EntityCommonStockSharesOutstanding",
        "EntityCommonStockSharesOutstanding",
        "us-gaap:CommonStockSharesOutstanding",
        "CommonStockSharesOutstanding",
        "us-gaap:CommonStockSharesIssued",
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
        "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
    ]
    for concept in FALLBACK_CONCEPTS:
        years = _extract_years_from_concept(facts, concept)
        if years:
            print(f"    fallback hit: {concept} ({len(years)} years)")
            return years
    return {}


def enrich_company(cur, company_id, ticker, force=False):
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

    print(f"    {len(target_years)} years to fill: {target_years[0]}–{target_years[-1]}")

    shares_by_year = get_shares_from_facts(ticker)
    if not shares_by_year:
        print(f"    [WARN] no shares data found")
        return 0

    shares_by_year = apply_split_adjustments(ticker, shares_by_year)

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
    else:
        print(f"    0 new values (data didn't match target years)")

    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Backfill shares_outstanding from EDGAR facts API")
    parser.add_argument("--ticker", help="Enrich one company only")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing shares values")
    parser.add_argument("--gaps-only", action="store_true",
                        help="Only process companies with missing shares data")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    if args.gaps_only:
        cur.execute("""
            SELECT DISTINCT c.id, c.ticker, c.cik
            FROM companies c
            JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
            WHERE f.shares_outstanding IS NULL
            ORDER BY c.ticker
        """)
        companies = cur.fetchall()
        print(f"[INFO] {len(companies)} companies with gaps")
    else:
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

    # Coverage summary
    print(f"\n{'='*70}")
    print(f"SHARES ENRICHMENT — {total_updated} values updated")
    print(f"{'='*70}")

    cur.execute("""
        SELECT c.ticker,
               COUNT(*) AS total,
               COUNT(f.shares_outstanding) AS with_shares,
               COUNT(*) - COUNT(f.shares_outstanding) AS missing,
               MIN(CASE WHEN f.shares_outstanding IS NULL THEN f.fiscal_year END) AS first_gap,
               MAX(CASE WHEN f.shares_outstanding IS NULL THEN f.fiscal_year END) AS last_gap
        FROM companies c
        JOIN filings f ON f.company_id = c.id AND f.form_type = '10-K'
        GROUP BY c.id, c.ticker
        ORDER BY (COUNT(*) - COUNT(f.shares_outstanding)) DESC, c.ticker
    """)
    rows = cur.fetchall()

    print(f"\n  {'Ticker':<8} {'Has':>4} {'/ Tot':>6} {'Miss':>5} {'Gap Range':>14}")
    print(f"  {'-'*8} {'-'*4} {'-'*6} {'-'*5} {'-'*14}")
    for r in rows:
        missing = r["missing"]
        gap = ""
        if missing > 0:
            gap = f"{r['first_gap']}–{r['last_gap']}"
        flag = " !" if missing > 0 else ""
        print(f"  {r['ticker']:<8} {r['with_shares']:>4} / {r['total']:<4} {missing:>5} {gap:>14}{flag}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
