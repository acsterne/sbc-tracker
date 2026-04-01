"""
validate.py — Post-ingestion data validation and self-healing.

Runs benchmark checks, sanity rules, and flags suspect values.
Can be run standalone or called after ingestion.

Usage:
    DATABASE_URL=... python3 validate.py              # validate all companies
    DATABASE_URL=... python3 validate.py --ticker META # validate one company
    DATABASE_URL=... python3 validate.py --heal        # attempt to fix suspect values
"""

import os
import argparse
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ["DATABASE_URL"]


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


# ── Ground truth benchmarks ───────────────────────────────────────────────────
# (ticker, fiscal_year, concept, expected_value, tolerance_pct)

BENCHMARKS = [
    ("META",  2019, "sbc",     4836000000,   0.05),
    ("META",  2022, "sbc",     11992000000,  0.05),
    ("META",  2019, "revenue", 70697000000,  0.05),
    ("META",  2017, "revenue", 40653000000,  0.05),
    ("AAPL",  2024, "sbc",     11284000000,  0.05),
    ("AAPL",  2024, "revenue", 391035000000, 0.05),
    ("MSFT",  2024, "sbc",     9942000000,   0.05),
    ("MSFT",  2024, "revenue", 245122000000, 0.05),
    ("GOOGL", 2024, "sbc",     22370000000,  0.05),
    ("GOOGL", 2024, "revenue", 350018000000, 0.05),
    ("AMZN",  2024, "sbc",     24023000000,  0.05),
    ("AMZN",  2024, "revenue", 637959000000, 0.05),
    ("NFLX",  2024, "sbc",     892000000,    0.05),
    ("NFLX",  2024, "revenue", 39000000000,  0.10),
    ("SNAP",  2023, "sbc",     1161000000,   0.05),
    ("TSLA",  2024, "sbc",     1731000000,   0.05),
    ("CRM",   2024, "sbc",     3590000000,   0.10),
    ("SNOW",  2024, "sbc",     1240000000,   0.10),
    ("WDAY",  2024, "sbc",     1620000000,   0.10),
    ("NOW",   2024, "sbc",     1630000000,   0.10),
]

# Map concept name → metrics column name
CONCEPT_TO_COLUMN = {
    "sbc":              "sbc_annual",
    "revenue":          "revenue_annual",
    "net_income":       "net_income_annual",
    "operating_income": "ebitda_annual",  # closest we have
    "shares":           "shares_outstanding_eoy",
    "buybacks":         "buyback_spend_annual",
}

RED    = "\033[91m"
YEL    = "\033[93m"
GRN    = "\033[92m"
BOLD   = "\033[1m"
RST    = "\033[0m"


# ── Benchmark validation ─────────────────────────────────────────────────────

def run_benchmarks(cur, ticker_filter=None):
    """Check ingested values against known-correct benchmarks."""
    results = {"passed": 0, "failed": 0, "missing": 0, "details": []}

    for ticker, fy, concept, expected, tolerance in BENCHMARKS:
        if ticker_filter and ticker.upper() != ticker_filter.upper():
            continue

        col = CONCEPT_TO_COLUMN.get(concept, f"{concept}_annual")
        cur.execute(f"""
            SELECT m.{col} AS val
            FROM metrics m
            JOIN companies c ON c.id = m.company_id
            WHERE c.ticker = %s AND m.fiscal_year = %s
        """, (ticker, fy))
        row = cur.fetchone()
        actual = row["val"] if row else None

        if actual is None:
            results["missing"] += 1
            results["details"].append({
                "ticker": ticker, "year": fy, "concept": concept,
                "expected": expected, "actual": None, "status": "MISSING",
            })
            continue

        actual = int(actual)
        pct_off = abs(actual - expected) / expected if expected else 0
        passed = pct_off <= tolerance

        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1

        results["details"].append({
            "ticker": ticker, "year": fy, "concept": concept,
            "expected": expected, "actual": actual,
            "pct_off": pct_off, "status": "PASS" if passed else "FAIL",
        })

    return results


# ── Sanity rules ──────────────────────────────────────────────────────────────

def run_sanity_rules(cur, ticker_filter=None):
    """Apply sanity rules to every company's metrics."""
    flags = []

    where = ""
    params = {}
    if ticker_filter:
        where = "AND c.ticker = %(ticker)s"
        params["ticker"] = ticker_filter.upper()

    # Get all metrics with company info
    cur.execute(f"""
        SELECT c.ticker, c.ipo_year, m.*
        FROM metrics m
        JOIN companies c ON c.id = m.company_id
        WHERE m.sbc_annual IS NOT NULL {where}
        ORDER BY c.ticker, m.fiscal_year
    """, params)
    all_rows = cur.fetchall()

    # Group by company
    by_company = {}
    for r in all_rows:
        by_company.setdefault(r["ticker"], []).append(r)

    for ticker, rows in by_company.items():
        for i, r in enumerate(rows):
            fy  = r["fiscal_year"]
            sbc = r["sbc_annual"]
            rev = r["revenue_annual"]

            # ── RULE 1: YoY sanity ────────────────────────────────────────
            if i > 0:
                prev = rows[i - 1]
                # SBC: should not change > 200% YoY
                if prev["sbc_annual"] and sbc:
                    prev_sbc = int(prev["sbc_annual"])
                    if prev_sbc > 0:
                        sbc_change = abs(int(sbc) - prev_sbc) / prev_sbc
                        if sbc_change > 2.0:
                            flags.append({
                                "ticker": ticker, "year": fy, "concept": "sbc",
                                "rule": "yoy_change",
                                "value": int(sbc), "prev": prev_sbc,
                                "detail": f"SBC changed {sbc_change*100:.0f}% YoY",
                            })

                # Revenue: should not change > 500% YoY
                if prev["revenue_annual"] and rev:
                    prev_rev = int(prev["revenue_annual"])
                    if prev_rev > 0:
                        rev_change = abs(int(rev) - prev_rev) / prev_rev
                        if rev_change > 5.0:
                            flags.append({
                                "ticker": ticker, "year": fy, "concept": "revenue",
                                "rule": "yoy_change",
                                "value": int(rev), "prev": prev_rev,
                                "detail": f"Revenue changed {rev_change*100:.0f}% YoY",
                            })

            # ── RULE 2: Ratio sanity ──────────────────────────────────────
            if sbc and rev and int(rev) > 0:
                pct = int(sbc) / int(rev) * 100
                if pct > 80:
                    flags.append({
                        "ticker": ticker, "year": fy, "concept": "sbc",
                        "rule": "ratio_high",
                        "value": int(sbc),
                        "detail": f"SBC is {pct:.1f}% of revenue (>80% = likely wrong tag)",
                    })
                elif pct < 0.1:
                    flags.append({
                        "ticker": ticker, "year": fy, "concept": "sbc",
                        "rule": "ratio_low",
                        "value": int(sbc),
                        "detail": f"SBC is {pct:.2f}% of revenue (<0.1% = likely missing data)",
                    })

            # ── RULE 3: Absolute magnitude ────────────────────────────────
            if rev and sbc:
                rev_val = int(rev)
                sbc_val = int(sbc)
                if rev_val > 50_000_000_000 and sbc_val < 1_000_000_000:
                    flags.append({
                        "ticker": ticker, "year": fy, "concept": "sbc",
                        "rule": "magnitude",
                        "value": sbc_val,
                        "detail": f"Mega cap (rev ${rev_val/1e9:.0f}B) but SBC only ${sbc_val/1e6:.0f}M",
                    })
                elif rev_val > 5_000_000_000 and sbc_val < 100_000_000:
                    flags.append({
                        "ticker": ticker, "year": fy, "concept": "sbc",
                        "rule": "magnitude",
                        "value": sbc_val,
                        "detail": f"Large cap (rev ${rev_val/1e9:.1f}B) but SBC only ${sbc_val/1e6:.0f}M",
                    })

    return flags


# ── Null out suspect values ───────────────────────────────────────────────────

def heal_suspect_values(cur, flags):
    """
    For flagged values, null them out in the metrics table so the UI shows '—'
    instead of bad data. This is the 'self-healing' step.
    Returns count of values nulled.
    """
    healed = 0
    for f in flags:
        col = CONCEPT_TO_COLUMN.get(f["concept"])
        if not col:
            continue
        cur.execute(f"""
            UPDATE metrics SET {col} = NULL
            WHERE company_id = (SELECT id FROM companies WHERE ticker = %s)
              AND fiscal_year = %s
        """, (f["ticker"], f["year"]))
        if cur.rowcount > 0:
            healed += 1
            print(f"    [HEAL] {f['ticker']} FY{f['year']} {f['concept']}: "
                  f"nulled (was ${f['value']:,}) — {f['detail']}")
    return healed


# ── Report ────────────────────────────────────────────────────────────────────

def _fmt_dollar(v):
    if v is None:
        return "—"
    v = int(v)
    if abs(v) >= 1e9:
        return f"${v/1e9:.1f}B"
    if abs(v) >= 1e6:
        return f"${v/1e6:.0f}M"
    return f"${v:,}"


def print_report(bench_results, sanity_flags, healed_count):
    total_bench  = bench_results["passed"] + bench_results["failed"] + bench_results["missing"]
    total_sanity = len(sanity_flags)

    print(f"\n{'='*60}")
    print(f"{BOLD}VALIDATION REPORT{RST}")
    print(f"{'='*60}")
    print(f"  Benchmark checks:  {bench_results['passed']}/{total_bench} passed"
          f"  ({bench_results['failed']} failed, {bench_results['missing']} missing)")
    print(f"  Sanity rule flags: {total_sanity} values flagged")
    print(f"  Self-healed:       {healed_count} values nulled in DB")

    # Failed benchmarks
    failed = [d for d in bench_results["details"] if d["status"] != "PASS"]
    if failed:
        print(f"\n  {RED}FAILED BENCHMARKS:{RST}")
        for d in failed:
            actual_str = _fmt_dollar(d["actual"]) if d["actual"] else "MISSING"
            expected_str = _fmt_dollar(d["expected"])
            pct_str = f" ({d.get('pct_off', 0)*100:.0f}% off)" if d["actual"] else ""
            print(f"    {RED}{d['ticker']:<6} FY{d['year']} {d['concept']:<10}: "
                  f"got {actual_str}, expected {expected_str}{pct_str}{RST}")

    passed = [d for d in bench_results["details"] if d["status"] == "PASS"]
    if passed:
        print(f"\n  {GRN}PASSED BENCHMARKS:{RST}")
        for d in passed:
            print(f"    {GRN}{d['ticker']:<6} FY{d['year']} {d['concept']:<10}: "
                  f"{_fmt_dollar(d['actual'])} (expected {_fmt_dollar(d['expected'])}){RST}")

    # Sanity flags
    if sanity_flags:
        print(f"\n  {YEL}SUSPECT VALUES:{RST}")
        for f in sanity_flags[:30]:  # cap output
            print(f"    {YEL}{f['ticker']:<6} FY{f['year']} {f['concept']:<10}: "
                  f"{f['detail']}{RST}")
        if len(sanity_flags) > 30:
            print(f"    ... and {len(sanity_flags) - 30} more")

    print(f"{'='*60}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Validate ingested data")
    parser.add_argument("--ticker", help="Validate one company only")
    parser.add_argument("--heal", action="store_true",
                        help="Null out suspect values in the DB")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    print("Running benchmark checks...")
    bench = run_benchmarks(cur, ticker_filter=args.ticker)

    print("Running sanity rules...")
    flags = run_sanity_rules(cur, ticker_filter=args.ticker)

    healed = 0
    if args.heal and flags:
        print(f"\nHealing {len(flags)} suspect values...")
        healed = heal_suspect_values(cur, flags)
        conn.commit()
    elif flags and not args.heal:
        print(f"\n  {len(flags)} suspect values found. Run with --heal to null them.")

    print_report(bench, flags, healed)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
