"""
fetch_sbc.py — pulls SBC and related financials from EDGAR for all tracked companies.
Run manually or via Railway cron. Fetches from 2009 onward.

Usage:
    DATABASE_URL=postgresql://... python3 fetch_sbc.py
    DATABASE_URL=postgresql://... python3 fetch_sbc.py --ticker SNAP   # single company
"""

import os
import sys
import time
import argparse
import requests
import psycopg2
import psycopg2.extras
from datetime import date
from companies import COMPANIES

DATABASE_URL = os.environ["DATABASE_URL"]
EDGAR_BASE = "https://data.sec.gov"
START_YEAR = 2009

# EDGAR requires a descriptive User-Agent — SEC will block generic agents
HEADERS = {
    "User-Agent": "SBC Tracker research@sbctracker.io",
    "Accept": "application/json",
}

# XBRL concept names we care about, in priority order (first match wins)
SBC_CONCEPTS = [
    "ShareBasedCompensation",
    "AllocatedShareBasedCompensationExpense",
    "EmployeeBenefitsAndShareBasedCompensation",
]
REVENUE_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
    "RevenueFromContractWithCustomerNetOfRefunds",
    "OtherRevenue",
]
GROSS_PROFIT_CONCEPTS = [
    "GrossProfit",
]
NET_INCOME_CONCEPTS = [
    "NetIncomeLoss",
    "ProfitLoss",
]
SHARES_OUTSTANDING_CONCEPTS = [
    "CommonStockSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
]
BUYBACK_SPEND_CONCEPTS = [
    "PaymentsForRepurchaseOfCommonStock",
    "TreasuryStockValueAcquiredCostMethod",
]
SHARES_REPURCHASED_CONCEPTS = [
    "TreasuryStockSharesAcquired",
    "StockRepurchasedDuringPeriodShares",
]


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor, connect_timeout=10)


def upsert_companies(cur):
    """Ensure all companies in companies.py exist in the DB."""
    for c in COMPANIES:
        cur.execute("""
            INSERT INTO companies (ticker, name, cik, sector, ipo_year)
            VALUES (%(ticker)s, %(name)s, %(cik)s, %(sector)s, %(ipo_year)s)
            ON CONFLICT (ticker) DO UPDATE SET
                name = EXCLUDED.name,
                cik = EXCLUDED.cik,
                sector = EXCLUDED.sector,
                ipo_year = EXCLUDED.ipo_year
        """, c)


def fetch_company_facts(cik):
    """Fetch the full XBRL facts JSON for a company from EDGAR."""
    url = f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik}.json"
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code == 404:
        print(f"  [WARN] No EDGAR facts found for CIK {cik}")
        return None
    r.raise_for_status()
    return r.json()


def extract_concept(facts, concepts, unit="USD"):
    """
    Given a list of concept names (in priority order), return a dict of
    {period_end_date_str: value} for annual (10-K) and quarterly (10-Q) filings.
    Returns (annual_dict, quarterly_dict).
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    for concept in concepts:
        if concept not in us_gaap:
            continue
        data = us_gaap[concept]
        units = data.get("units", {})
        if unit not in units:
            continue
        entries = units[unit]
        annual = {}
        quarterly = {}
        for e in entries:
            form = e.get("form", "")
            end = e.get("end", "")
            val = e.get("val")
            accn = e.get("accn", "")
            if val is None or not end:
                continue
            year = int(end[:4])
            if year < START_YEAR:
                continue
            # Use "frame" to distinguish instantaneous vs. period metrics.
            # For duration metrics (SBC, revenue), prefer entries with a frame
            # that matches the full period (CY####, CY####Q#I, etc.)
            if form == "10-K":
                # Prefer the most recent filing for the same period
                if end not in annual or accn > annual[end]["accn"]:
                    annual[end] = {"val": val, "accn": accn}
            elif form == "10-Q":
                if end not in quarterly or accn > quarterly[end]["accn"]:
                    quarterly[end] = {"val": val, "accn": accn}
        if annual or quarterly:
            print(f"    [XBRL] matched '{concept}': {len(annual)} annual, {len(quarterly)} quarterly rows")
            return (
                {k: v["val"] for k, v in annual.items()},
                {k: v["val"] for k, v in quarterly.items()},
            )
    print(f"    [XBRL] no match for concepts: {concepts}")
    return {}, {}


def extract_shares(facts, concepts):
    """
    Share counts are in 'shares' units, not 'USD'.
    Returns (annual_dict, quarterly_dict).
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    dei = facts.get("facts", {}).get("dei", {})
    all_sources = {**us_gaap, **dei}
    for concept in concepts:
        if concept not in all_sources:
            continue
        data = all_sources[concept]
        units = data.get("units", {})
        share_unit = next((u for u in units if "share" in u.lower()), None)
        if not share_unit:
            continue
        entries = units[share_unit]
        annual = {}
        quarterly = {}
        for e in entries:
            form = e.get("form", "")
            end = e.get("end", "")
            val = e.get("val")
            accn = e.get("accn", "")
            if val is None or not end:
                continue
            year = int(end[:4])
            if year < START_YEAR:
                continue
            if form == "10-K":
                if end not in annual or accn > annual[end]["accn"]:
                    annual[end] = {"val": val, "accn": accn}
            elif form == "10-Q":
                if end not in quarterly or accn > quarterly[end]["accn"]:
                    quarterly[end] = {"val": val, "accn": accn}
        if annual or quarterly:
            return (
                {k: v["val"] for k, v in annual.items()},
                {k: v["val"] for k, v in quarterly.items()},
            )
    return {}, {}


def save_filings(cur, company_id, facts):
    """Parse facts JSON and upsert filing rows."""
    # Extract each concept
    sbc_ann, sbc_qtd         = extract_concept(facts, SBC_CONCEPTS)
    rev_ann, rev_qtd         = extract_concept(facts, REVENUE_CONCEPTS)
    gp_ann, gp_qtd           = extract_concept(facts, GROSS_PROFIT_CONCEPTS)
    ni_ann, ni_qtd           = extract_concept(facts, NET_INCOME_CONCEPTS)
    bb_ann, bb_qtd           = extract_concept(facts, BUYBACK_SPEND_CONCEPTS)
    shr_ann, shr_qtd         = extract_shares(facts, SHARES_OUTSTANDING_CONCEPTS)
    shrep_ann, shrep_qtd     = extract_shares(facts, SHARES_REPURCHASED_CONCEPTS)

    # Collect all period_end dates across annual and quarterly
    all_annual_dates = set(sbc_ann) | set(rev_ann) | set(gp_ann) | set(ni_ann) | set(bb_ann) | set(shr_ann)
    all_quarterly_dates = set(sbc_qtd) | set(rev_qtd) | set(gp_qtd) | set(ni_qtd) | set(bb_qtd) | set(shr_qtd)

    def upsert_filing(period_end_str, form_type, sbc_d, rev_d, gp_d, ni_d, bb_d, shr_d, shrep_d):
        period_end = date.fromisoformat(period_end_str)
        fiscal_year = period_end.year
        # Approximate fiscal quarter from month
        m = period_end.month
        fq = None if form_type == "10-K" else ((m - 1) // 3 + 1)

        cur.execute("""
            INSERT INTO filings (
                company_id, period_end, fiscal_year, fiscal_quarter, form_type,
                sbc_expense, revenue, gross_profit, net_income,
                shares_outstanding, shares_repurchased, buyback_spend
            ) VALUES (
                %(company_id)s, %(period_end)s, %(fiscal_year)s, %(fiscal_quarter)s, %(form_type)s,
                %(sbc)s, %(rev)s, %(gp)s, %(ni)s,
                %(shr)s, %(shrep)s, %(bb)s
            )
            ON CONFLICT (company_id, period_end, form_type) DO UPDATE SET
                sbc_expense        = COALESCE(EXCLUDED.sbc_expense, filings.sbc_expense),
                revenue            = COALESCE(EXCLUDED.revenue, filings.revenue),
                gross_profit       = COALESCE(EXCLUDED.gross_profit, filings.gross_profit),
                net_income         = COALESCE(EXCLUDED.net_income, filings.net_income),
                shares_outstanding = COALESCE(EXCLUDED.shares_outstanding, filings.shares_outstanding),
                shares_repurchased = COALESCE(EXCLUDED.shares_repurchased, filings.shares_repurchased),
                buyback_spend      = COALESCE(EXCLUDED.buyback_spend, filings.buyback_spend),
                fetched_at         = NOW()
        """, {
            "company_id": company_id,
            "period_end": period_end,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fq,
            "form_type": form_type,
            "sbc":   sbc_d.get(period_end_str),
            "rev":   rev_d.get(period_end_str),
            "gp":    gp_d.get(period_end_str),
            "ni":    ni_d.get(period_end_str),
            "shr":   shr_d.get(period_end_str),
            "shrep": shrep_d.get(period_end_str),
            "bb":    bb_d.get(period_end_str),
        })

    for d in all_annual_dates:
        upsert_filing(d, "10-K", sbc_ann, rev_ann, gp_ann, ni_ann, bb_ann, shr_ann, shrep_ann)

    for d in all_quarterly_dates:
        upsert_filing(d, "10-Q", sbc_qtd, rev_qtd, gp_qtd, ni_qtd, bb_qtd, shr_qtd, shrep_qtd)


def refresh_metrics(cur, company_id):
    """
    Compute annual metrics from 10-K filings only.
    10-K data is authoritative — quarterly (10-Q) filings store YTD cumulative values
    which would double-count if summed.
    """
    # Wipe and recompute cleanly — avoids stale rows from previous runs
    cur.execute("DELETE FROM metrics WHERE company_id = %s", (company_id,))

    cur.execute("""
        SELECT fiscal_year,
               sbc_expense, revenue, gross_profit, net_income,
               buyback_spend, shares_repurchased, shares_outstanding
        FROM filings
        WHERE company_id = %s AND form_type = '10-K'
        ORDER BY fiscal_year
    """, (company_id,))
    annual_rows = {r["fiscal_year"]: r for r in cur.fetchall()}

    all_years = sorted(annual_rows.keys())
    prev_revenue = None

    for yr in all_years:
        row = annual_rows.get(yr)
        if not row:
            continue

        sbc     = row["sbc_expense"]
        rev     = row["revenue"]
        gp      = row["gross_profit"]
        shr     = row["shares_outstanding"]
        bb      = row["buyback_spend"]
        shrep   = row["shares_repurchased"]

        sbc_pct_rev  = (sbc / rev * 100)       if sbc and rev else None
        sbc_pct_gp   = (sbc / gp * 100)        if sbc and gp else None
        sbc_per_shr  = (sbc / shr)             if sbc and shr else None
        rev_growth   = ((rev - prev_revenue) / prev_revenue * 100) if rev and prev_revenue else None

        # Net dilution: approximate SBC shares as SBC $ / stock price is complex —
        # instead we use shares_repurchased directly if available.
        # Net dilution % = (shares repurchased offset against new SBC shares) / total shares
        # We store the raw shrep here; UI can show buyback spend vs SBC spend ratio as well.
        net_dil = None
        if shrep and shr:
            # This is shares bought back as % of total — a proxy for buyback intensity
            # True net dilution requires knowing SBC grant shares, which needs a separate EDGAR concept
            net_dil = (shrep / shr * 100)

        cur.execute("""
            INSERT INTO metrics (
                company_id, fiscal_year,
                sbc_annual, revenue_annual, gross_profit_annual, net_income_annual,
                buyback_spend_annual, shares_repurchased_annual, shares_outstanding_eoy,
                sbc_pct_revenue, sbc_pct_gross_profit, sbc_per_share,
                net_dilution_pct, revenue_growth_yoy, computed_at
            ) VALUES (
                %(company_id)s, %(fiscal_year)s,
                %(sbc)s, %(rev)s, %(gp)s, %(ni)s,
                %(bb)s, %(shrep)s, %(shr)s,
                %(sbc_pct_rev)s, %(sbc_pct_gp)s, %(sbc_per_shr)s,
                %(net_dil)s, %(rev_growth)s, NOW()
            )
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET
                sbc_annual              = EXCLUDED.sbc_annual,
                revenue_annual          = EXCLUDED.revenue_annual,
                gross_profit_annual     = EXCLUDED.gross_profit_annual,
                net_income_annual       = EXCLUDED.net_income_annual,
                buyback_spend_annual    = EXCLUDED.buyback_spend_annual,
                shares_repurchased_annual = EXCLUDED.shares_repurchased_annual,
                shares_outstanding_eoy  = EXCLUDED.shares_outstanding_eoy,
                sbc_pct_revenue         = EXCLUDED.sbc_pct_revenue,
                sbc_pct_gross_profit    = EXCLUDED.sbc_pct_gross_profit,
                sbc_per_share           = EXCLUDED.sbc_per_share,
                net_dilution_pct        = EXCLUDED.net_dilution_pct,
                revenue_growth_yoy      = EXCLUDED.revenue_growth_yoy,
                computed_at             = NOW()
        """, {
            "company_id": company_id, "fiscal_year": yr,
            "sbc": sbc, "rev": rev, "gp": gp, "ni": row["net_income"],
            "bb": bb, "shrep": shrep, "shr": shr,
            "sbc_pct_rev": sbc_pct_rev, "sbc_pct_gp": sbc_pct_gp,
            "sbc_per_shr": sbc_per_shr, "net_dil": net_dil, "rev_growth": rev_growth,
        })

        if rev:
            prev_revenue = rev


def fetch_company(cur, company, ticker_filter=None):
    ticker = company["ticker"]
    if ticker_filter and ticker.upper() != ticker_filter.upper():
        return

    print(f"  Fetching {ticker} ({company['name']})...")
    try:
        facts = fetch_company_facts(company["cik"])
        if not facts:
            return
        # Get DB company_id
        cur.execute("SELECT id FROM companies WHERE ticker = %s", (ticker,))
        row = cur.fetchone()
        if not row:
            print(f"  [WARN] {ticker} not found in DB — skipping")
            return
        company_id = row["id"]
        save_filings(cur, company_id, facts)
        refresh_metrics(cur, company_id)
        print(f"  {ticker} done.")
    except Exception as e:
        print(f"  [ERROR] {ticker}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Fetch a single company by ticker")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    print("Upserting companies...")
    upsert_companies(cur)
    conn.commit()

    print(f"Fetching EDGAR data (from {START_YEAR})...")
    for company in COMPANIES:
        fetch_company(cur, company, ticker_filter=args.ticker)
        conn.commit()
        time.sleep(0.5)  # Be polite to SEC servers (rate limit ~10 req/sec)

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
