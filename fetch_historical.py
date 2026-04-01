"""
fetch_historical.py — Historical 10-K ingestion using edgartools.

For each company, pulls every 10-K filing from EDGAR via edgartools,
extracts financials from parsed XBRL statements, validates against
benchmarks, and self-heals suspect values.

Usage:
    DATABASE_URL=... python3 fetch_historical.py                      # all companies
    DATABASE_URL=... python3 fetch_historical.py --ticker META        # one company
    DATABASE_URL=... python3 fetch_historical.py --ticker META --force
    DATABASE_URL=... python3 fetch_historical.py --reset-checkpoint   # start fresh
"""

import os
import json
import argparse
import psycopg2
import psycopg2.extras
from datetime import date, datetime, timezone

from edgar import Company, set_identity
set_identity("research@sbctracker.io")

DATABASE_URL    = os.environ["DATABASE_URL"]
START_YEAR      = 2009
CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "historical_checkpoint.json")
INGESTION_LOG   = os.path.join(os.path.dirname(__file__), "historical_ingestion.log")

ALL_CONCEPTS = ["sbc", "revenue", "gross_profit", "net_income",
                "operating_income", "da", "buybacks", "shares_outstanding"]


# ── DB ────────────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


# ── Checkpoint ────────────────────────────────────────────────────────────────

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f).get("completed", []))
    except Exception:
        return set()


def save_checkpoint(completed):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"completed": sorted(completed),
                    "last_updated": datetime.now(tz=timezone.utc).isoformat() + "Z",
                    "count": len(completed)}, f, indent=2)


# ── Per-filing log ────────────────────────────────────────────────────────────

def log_filing(ticker, fiscal_year, source, found, missing):
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = (f"{ts} | {ticker:<6} | FY{fiscal_year} | src={source:<12} | "
            f"found=[{','.join(found)}] | missing=[{','.join(missing)}]\n")
    with open(INGESTION_LOG, "a") as f:
        f.write(line)


# ── Extract data from one filing via edgartools ──────────────────────────────

def _get_value(df, period_col, standard_concepts=None, concept_contains=None):
    """
    Find a value in a statement dataframe.
    Tries standard_concept first, then concept name substring match.
    Filters to non-breakdown rows for consolidated totals.
    """
    if df is None or df.empty or period_col not in df.columns:
        return None

    # Filter to non-breakdown rows (consolidated totals, not segment subtotals)
    if "is_breakdown" in df.columns:
        filtered = df[df["is_breakdown"] == False]
    else:
        filtered = df

    if standard_concepts:
        for sc in standard_concepts:
            if "standard_concept" in filtered.columns:
                mask = filtered["standard_concept"] == sc
                if mask.any():
                    vals = filtered.loc[mask, period_col].dropna()
                    if not vals.empty:
                        return float(vals.iloc[0])

    if concept_contains:
        if "concept" in filtered.columns:
            for cc in concept_contains:
                mask = filtered["concept"].str.contains(cc, case=False, na=False)
                if mask.any():
                    vals = filtered.loc[mask, period_col].dropna()
                    if not vals.empty:
                        return float(vals.iloc[0])

    return None


def extract_filing_data(ticker, filing):
    """
    Extract all financial metrics from a single 10-K filing using edgartools.
    Returns dict with extracted values, or None if XBRL unavailable.
    """
    try:
        xbrl = filing.xbrl()
    except Exception as e:
        print(f"        xbrl() error: {e}")
        return None
    if not xbrl:
        return None

    period = str(filing.period_of_report)
    fiscal_year = int(period[:4])

    # Parse each statement; any may fail for older filings
    income_df = cashflow_df = balance_df = None
    try:
        inc = xbrl.statements.income_statement()
        income_df = inc.to_dataframe() if inc else None
    except Exception:
        pass
    try:
        cf = xbrl.statements.cash_flow_statement()
        cashflow_df = cf.to_dataframe() if cf else None
    except Exception:
        pass
    try:
        bs = xbrl.statements.balance_sheet()
        balance_df = bs.to_dataframe() if bs else None
    except Exception:
        pass

    if income_df is None and cashflow_df is None:
        return None

    def gv_inc(standard_concepts=None, concept_contains=None):
        return _get_value(income_df, period, standard_concepts, concept_contains)

    def gv_cf(standard_concepts=None, concept_contains=None):
        return _get_value(cashflow_df, period, standard_concepts, concept_contains)

    def gv_bs(standard_concepts=None, concept_contains=None):
        return _get_value(balance_df, period, standard_concepts, concept_contains)

    # SBC — cash flow statement (most reliable, always consolidated)
    sbc = gv_cf(
        standard_concepts=["StockBasedCompensationExpense"],
        concept_contains=["ShareBasedCompensation", "StockBasedCompensation",
                          "AllocatedShareBasedCompensationExpense"])

    # Revenue
    revenue = gv_inc(
        standard_concepts=["Revenue"],
        concept_contains=["RevenueFromContractWithCustomerExcludingAssessedTax",
                          "Revenues", "SalesRevenueNet", "RevenueNet"])

    # Operating Income
    op_income = gv_inc(
        standard_concepts=["OperatingIncomeLoss"],
        concept_contains=["OperatingIncomeLoss", "OperatingIncome"])

    # D&A — cash flow
    da = gv_cf(
        standard_concepts=["DepreciationExpense"],
        concept_contains=["DepreciationDepletionAndAmortization",
                          "DepreciationAndAmortization", "Depreciation"])

    # Net Income
    net_income = gv_inc(
        standard_concepts=["NetIncome"],
        concept_contains=["NetIncomeLoss", "ProfitLoss"])

    # Buybacks — cash flow (financing section)
    buybacks = gv_cf(
        concept_contains=["PaymentsForRepurchaseOfCommonStock",
                          "RepurchaseOfCommonStock",
                          "TreasuryStockValueAcquiredCostMethod"])
    # Buybacks are typically reported as negative in cash flow; store as positive
    if buybacks is not None and buybacks < 0:
        buybacks = abs(buybacks)

    # Gross Profit
    gross_profit = gv_inc(
        standard_concepts=["GrossProfit"],
        concept_contains=["GrossProfit"])

    # Shares Outstanding — balance sheet
    shares = gv_bs(
        concept_contains=["CommonStockSharesOutstanding",
                          "EntityCommonStockSharesOutstanding"])

    # EBITDA derived
    ebitda = None
    if op_income is not None and da is not None:
        ebitda = op_income + da

    data = {
        "fiscal_year":  fiscal_year,
        "period_end":   period,
        "sbc":          int(sbc)            if sbc is not None else None,
        "revenue":      int(revenue)        if revenue is not None else None,
        "gross_profit": int(gross_profit)   if gross_profit is not None else None,
        "net_income":   int(net_income)     if net_income is not None else None,
        "operating_income": int(op_income)  if op_income is not None else None,
        "da":           int(da)             if da is not None else None,
        "ebitda":       int(ebitda)         if ebitda is not None else None,
        "buybacks":     int(buybacks)       if buybacks is not None else None,
        "shares":       int(shares)         if shares is not None else None,
        "source":       "edgartools",
        "confidence":   "high",
    }

    found   = [k for k in ALL_CONCEPTS if data.get(k) is not None or
               (k == "sbc" and data.get("sbc") is not None) or
               (k == "shares_outstanding" and data.get("shares") is not None)]
    missing = [k for k in ALL_CONCEPTS if k not in found]

    return data


# ── Upsert one filing into DB ─────────────────────────────────────────────────

def upsert_filing(cur, company_id, data):
    """Insert or update a single filing row. Existing non-null values preserved."""
    period_end = date.fromisoformat(data["period_end"])
    cur.execute("""
        INSERT INTO filings (
            company_id, period_end, fiscal_year, fiscal_quarter, form_type,
            sbc_expense, revenue, gross_profit, net_income, buyback_spend,
            shares_outstanding,
            operating_income, depreciation_amortization, ebitda, ebitda_source,
            data_source, confidence
        ) VALUES (
            %(cid)s, %(pe)s, %(fy)s, NULL, '10-K',
            %(sbc)s, %(rev)s, %(gp)s, %(ni)s, %(bb)s,
            %(shr)s,
            %(oi)s, %(da)s, %(ebitda)s, %(ebitda_src)s,
            %(src)s, %(conf)s
        )
        ON CONFLICT (company_id, fiscal_year, form_type) DO UPDATE SET
            sbc_expense               = COALESCE(filings.sbc_expense,               EXCLUDED.sbc_expense),
            revenue                   = COALESCE(filings.revenue,                   EXCLUDED.revenue),
            gross_profit              = COALESCE(filings.gross_profit,              EXCLUDED.gross_profit),
            net_income                = COALESCE(filings.net_income,                EXCLUDED.net_income),
            buyback_spend             = COALESCE(filings.buyback_spend,             EXCLUDED.buyback_spend),
            shares_outstanding        = COALESCE(filings.shares_outstanding,        EXCLUDED.shares_outstanding),
            operating_income          = COALESCE(filings.operating_income,          EXCLUDED.operating_income),
            depreciation_amortization = COALESCE(filings.depreciation_amortization, EXCLUDED.depreciation_amortization),
            ebitda                    = COALESCE(filings.ebitda,                    EXCLUDED.ebitda),
            ebitda_source             = COALESCE(filings.ebitda_source,             EXCLUDED.ebitda_source),
            data_source               = COALESCE(filings.data_source,               EXCLUDED.data_source),
            confidence                = COALESCE(filings.confidence,                EXCLUDED.confidence),
            fetched_at                = NOW()
    """, {
        "cid":        company_id,
        "pe":         period_end,
        "fy":         data["fiscal_year"],
        "sbc":        data["sbc"],
        "rev":        data["revenue"],
        "gp":         data["gross_profit"],
        "ni":         data["net_income"],
        "bb":         data["buybacks"],
        "shr":        data["shares"],
        "oi":         data["operating_income"],
        "da":         data["da"],
        "ebitda":     data["ebitda"],
        "ebitda_src": "xbrl_derived" if data["ebitda"] is not None else None,
        "src":        data["source"],
        "conf":       data["confidence"],
    })


# ── Process one company ───────────────────────────────────────────────────────

def process_company(cur, company_id, ticker, cik, force=False):
    """Fetch all 10-K filings for a company via edgartools."""
    print(f"\n  [{ticker}] fetching 10-K filings via edgartools...")

    try:
        company = Company(ticker)
        filings = company.get_filings(form="10-K", amendments=False)
    except Exception as e:
        print(f"    [ERROR] could not load company: {e}")
        return {"total": 0, "with_sbc": 0, "failed": 0}

    filings_list = list(filings)
    print(f"    {len(filings_list)} filings found")

    stats = {"total": len(filings_list), "with_sbc": 0, "failed": 0}

    for filing in filings_list:
        period = str(filing.period_of_report or "")
        if not period:
            continue
        fiscal_year = int(period[:4])
        if fiscal_year < START_YEAR:
            continue

        # Skip if already complete (unless --force)
        if not force:
            cur.execute("""
                SELECT sbc_expense, shares_outstanding, revenue, net_income,
                       operating_income, depreciation_amortization
                FROM filings
                WHERE company_id = %s AND fiscal_year = %s AND form_type = '10-K'
            """, (company_id, fiscal_year))
            existing = cur.fetchone()
            if existing and all(existing[f] is not None for f in
                               ("sbc_expense", "shares_outstanding", "revenue",
                                "net_income", "operating_income",
                                "depreciation_amortization")):
                print(f"      FY{fiscal_year}: complete — skip")
                stats["with_sbc"] += 1
                continue

        print(f"      FY{fiscal_year} ({filing.filing_date})...", end=" ")

        try:
            data = extract_filing_data(ticker, filing)
        except Exception as e:
            print(f"ERROR: {e}")
            log_filing(ticker, fiscal_year, "error", [], ALL_CONCEPTS)
            stats["failed"] += 1
            continue

        if not data:
            print("no XBRL")
            log_filing(ticker, fiscal_year, "no_xbrl", [], ALL_CONCEPTS)
            stats["failed"] += 1
            continue

        # Print summary
        parts = []
        if data["sbc"]:
            parts.append(f"SBC=${data['sbc']/1e9:.1f}B")
        if data["revenue"]:
            parts.append(f"Rev=${data['revenue']/1e9:.1f}B")
        if data["shares"]:
            parts.append(f"Shares={data['shares']/1e9:.2f}B")
        if data["buybacks"]:
            parts.append(f"BB=${data['buybacks']/1e9:.1f}B")
        print(", ".join(parts) if parts else "no key data")

        upsert_filing(cur, company_id, data)
        cur.connection.commit()

        found   = [k for k in ALL_CONCEPTS if data.get(k if k != "shares_outstanding" else "shares")]
        missing = [k for k in ALL_CONCEPTS if k not in found]
        log_filing(ticker, fiscal_year, "edgartools", found, missing)

        if data["sbc"]:
            stats["with_sbc"] += 1
        else:
            stats["failed"] += 1

    # Recompute metrics
    from fetch_sbc import refresh_metrics
    refresh_metrics(cur, company_id)
    cur.connection.commit()

    # Year-by-year summary
    cur.execute("""
        SELECT fiscal_year, sbc_expense, shares_outstanding, buyback_spend, revenue
        FROM filings
        WHERE company_id = %s AND form_type = '10-K'
        ORDER BY fiscal_year
    """, (company_id,))
    rows = cur.fetchall()
    if rows:
        print(f"\n    [{ticker}] Year-by-year:")
        print(f"    {'Year':<6} {'SBC':>14} {'Revenue':>14} {'Shares':>16} {'Buybacks':>14}")
        print(f"    {'-'*6} {'-'*14} {'-'*14} {'-'*16} {'-'*14}")
        for r in rows:
            s = lambda v: f"${v/1e9:.1f}B" if v and abs(v) >= 1e9 else (f"${v/1e6:.0f}M" if v else "—")
            shr = f"{r['shares_outstanding']/1e9:.2f}B" if r['shares_outstanding'] else "—"
            print(f"    {r['fiscal_year']:<6} {s(r['sbc_expense']):>14} "
                  f"{s(r['revenue']):>14} {shr:>16} {s(r['buyback_spend']):>14}")

    # Validate
    from validate import run_benchmarks, run_sanity_rules, heal_suspect_values
    bench = run_benchmarks(cur, ticker_filter=ticker)
    flags = run_sanity_rules(cur, ticker_filter=ticker)
    healed = 0
    if flags:
        print(f"\n    [{ticker}] {len(flags)} suspect values — healing...")
        healed = heal_suspect_values(cur, flags)
        cur.connection.commit()
        flags_after = run_sanity_rules(cur, ticker_filter=ticker)
        needs_review = len(flags_after)
        if needs_review:
            print(f"    [{ticker}] {needs_review} still suspect after healing")
    else:
        needs_review = 0

    bp = bench["passed"]
    bf = bench["failed"] + bench["missing"]
    if bf:
        print(f"    [{ticker}] benchmarks: {bp} passed, {bf} failed")
    elif bp:
        print(f"    [{ticker}] benchmarks: {bp}/{bp} passed")

    print(f"    [{ticker}] done — {stats['with_sbc']}/{stats['total']} with SBC")

    stats["validation"] = {
        "bench": bench,
        "flags": len(flags),
        "healed": healed,
        "needs_review": needs_review,
    }
    return stats


# ── Summary report ────────────────────────────────────────────────────────────

def print_summary(all_stats):
    total_companies = len(all_stats)
    total_filings   = sum(s["total"]    for s in all_stats.values())
    total_sbc       = sum(s["with_sbc"] for s in all_stats.values())
    total_failed    = sum(s["failed"]   for s in all_stats.values())

    total_bench_pass = total_bench_fail = 0
    total_flags = total_healed = total_review = 0
    all_bench_details = []
    for s in all_stats.values():
        v = s.get("validation", {})
        b = v.get("bench", {})
        total_bench_pass += b.get("passed", 0)
        total_bench_fail += b.get("failed", 0) + b.get("missing", 0)
        total_flags      += v.get("flags", 0)
        total_healed     += v.get("healed", 0)
        total_review     += v.get("needs_review", 0)
        all_bench_details.extend(b.get("details", []))

    RED = "\033[91m"; GRN = "\033[92m"; YEL = "\033[93m"
    BOLD = "\033[1m"; RST = "\033[0m"

    print(f"\n{'='*65}")
    print(f"{BOLD}INGESTION + VALIDATION SUMMARY{RST}")
    print(f"{'='*65}")
    print(f"  Companies processed : {total_companies}")
    print(f"  Total 10-K filings  : {total_filings}")
    if total_filings:
        print(f"  Filings with SBC    : {total_sbc} ({total_sbc/total_filings*100:.0f}%)")
    print(f"  Failed/no data      : {total_failed}")
    total_bench = total_bench_pass + total_bench_fail
    if total_bench:
        print(f"\n  Benchmark checks    : {total_bench_pass}/{total_bench} passed")
    print(f"  Sanity flags        : {total_flags} values flagged")
    print(f"  Self-healed         : {total_healed} values nulled")
    print(f"  Needs review        : {total_review} still suspect")

    failed_bench = [d for d in all_bench_details if d["status"] != "PASS"]
    if failed_bench:
        print(f"\n  {RED}FAILED BENCHMARKS:{RST}")
        for d in failed_bench:
            actual = d.get("actual")
            a = "MISSING" if actual is None else (
                f"${actual/1e9:.1f}B" if abs(actual) >= 1e9 else f"${actual/1e6:.0f}M")
            e = d["expected"]
            ex = f"${e/1e9:.1f}B" if abs(e) >= 1e9 else f"${e/1e6:.0f}M"
            pct = f" ({d.get('pct_off',0)*100:.0f}% off)" if actual else ""
            print(f"    {RED}{d['ticker']:<6} FY{d['year']} {d['concept']:<10}: "
                  f"got {a}, expected {ex}{pct}{RST}")

    print(f"\n  Coverage:")
    for ticker, s in sorted(all_stats.items()):
        pct = s["with_sbc"] / s["total"] * 100 if s["total"] else 0
        flag = f"  {RED}<70%{RST}" if pct < 70 else ""
        print(f"    {ticker:<8} {s['with_sbc']:>3}/{s['total']:<3}  {pct:.0f}%{flag}")
    print(f"{'='*65}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Historical 10-K ingestion via edgartools")
    parser.add_argument("--ticker", help="Process one company (e.g. META)")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even when data already exists")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Clear checkpoint and start fresh")
    args = parser.parse_args()

    if args.reset_checkpoint and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("[INFO] Checkpoint cleared")

    completed = set() if args.ticker else load_checkpoint()
    if completed:
        print(f"[INFO] Resuming — {len(completed)} companies already done "
              f"(use --reset-checkpoint to restart)")

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT id, ticker, cik, ipo_year FROM companies ORDER BY ticker")
    companies = cur.fetchall()

    if args.ticker:
        companies = [c for c in companies
                     if c["ticker"].upper() == args.ticker.upper()]
        if not companies:
            print(f"[ERROR] {args.ticker} not found in companies table")
            return

    all_stats = {}
    for co in companies:
        ticker = co["ticker"]
        if ticker in completed:
            print(f"\n  [{ticker}] already completed — skipping")
            continue

        try:
            stats = process_company(cur, co["id"], ticker, co["cik"],
                                    force=args.force)
            all_stats[ticker] = stats
            if not args.ticker:
                completed.add(ticker)
                save_checkpoint(completed)
        except Exception as e:
            print(f"\n  [ERROR] {ticker}: {e}")
            try:
                cur.connection.rollback()
            except Exception:
                pass

    print_summary(all_stats)

    # Final consolidated validation
    if all_stats:
        try:
            from validate import run_benchmarks, run_sanity_rules, print_report
            print("\n  Final consolidated validation...")
            bench = run_benchmarks(cur)
            flags = run_sanity_rules(cur)
            print_report(bench, flags, healed_count=0)
        except Exception as e:
            print(f"[INFO] Final validation skipped: {e}")

    cur.close()
    conn.close()

    if not args.ticker and all_stats:
        print(f"[INFO] Checkpoint: {CHECKPOINT_FILE}")
        print(f"[INFO] Log: {INGESTION_LOG}")


if __name__ == "__main__":
    main()
