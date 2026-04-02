"""
fetch_prices.py — Fetch historical stock prices from Yahoo Finance.

Populates stock_price_eoy, market_cap, and sbc_pct_market_cap in the metrics table.
Uses fiscal year end dates from EDGAR to look up the correct closing price.

Usage:
    DATABASE_URL=... python3 fetch_prices.py
    DATABASE_URL=... python3 fetch_prices.py --ticker META
"""

import os
import time
import argparse
import requests
import psycopg2
import psycopg2.extras
import yfinance as yf
import pandas as pd

DATABASE_URL = os.environ["DATABASE_URL"]

EDGAR_BASE = "https://data.sec.gov"
HEADERS = {
    "User-Agent": "SBC Tracker research@sbctracker.io",
    "Accept": "application/json",
}


def get_db():
    return psycopg2.connect(DATABASE_URL,
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=10)


def get_fy_end_dates(cik):
    """
    Get fiscal year end dates from EDGAR companyfacts.
    Returns dict: fiscal_year -> end_date string (e.g. '2025-09-27').
    """
    url = f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        time.sleep(0.11)
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return {}
        facts = r.json().get("facts", {}).get("us-gaap", {})
    except Exception:
        return {}

    # Use a common concept to find 10-K end dates
    fy_ends = {}
    for concept_name in ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                         "NetIncomeLoss", "Assets"]:
        entries = facts.get(concept_name, {}).get("units", {}).get("USD", [])
        for e in entries:
            if e.get("form") != "10-K":
                continue
            end = e.get("end", "")
            if not end:
                continue
            fy = int(end[:4])
            # Keep the latest filing's end date per fiscal year
            filed = e.get("filed", "")
            if fy not in fy_ends or filed > fy_ends[fy][1]:
                fy_ends[fy] = (end, filed)

    return {fy: end_date for fy, (end_date, _) in fy_ends.items()}


def fetch_prices_for_company(cur, company_id, ticker, cik):
    """Fetch historical prices from yfinance and update metrics."""
    # Get fiscal years we have metrics for
    cur.execute("""
        SELECT fiscal_year, sbc_annual, shares_outstanding_eoy
        FROM metrics
        WHERE company_id = %s
        ORDER BY fiscal_year
    """, (company_id,))
    metrics = cur.fetchall()
    if not metrics:
        return 0

    # Get FY end dates from EDGAR
    fy_end_dates = get_fy_end_dates(cik)

    # Fetch full monthly price history from yfinance
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="max", interval="1d")
        if hist.empty:
            print(f"    no price data from yfinance")
            return 0
        # Ensure timezone-naive index for comparison
        hist.index = hist.index.tz_localize(None)
    except Exception as e:
        print(f"    yfinance error: {e}")
        return 0

    updated = 0
    for m in metrics:
        fy = m["fiscal_year"]
        sbc = m["sbc_annual"]
        shares = m["shares_outstanding_eoy"]

        # Determine FY end date
        end_str = fy_end_dates.get(fy)
        if end_str:
            fy_end = pd.Timestamp(end_str)
        else:
            # Fallback: assume December 31
            fy_end = pd.Timestamp(f"{fy}-12-31")

        # Find closest trading day on or before FY end
        mask = hist.index <= fy_end
        if not mask.any():
            continue
        price = float(hist.loc[mask, "Close"].iloc[-1])

        # Compute market cap and SBC % of market cap
        market_cap = int(shares * price) if shares else None
        sbc_pct_mcap = round(sbc / market_cap * 100, 2) if sbc and market_cap else None

        cur.execute("""
            UPDATE metrics SET
                stock_price_eoy = %s,
                market_cap = %s,
                sbc_pct_market_cap = %s
            WHERE company_id = %s AND fiscal_year = %s
        """, (round(price, 2), market_cap, sbc_pct_mcap, company_id, fy))

        if cur.rowcount > 0:
            updated += 1

    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Fetch stock prices from Yahoo Finance")
    parser.add_argument("--ticker", help="Process one company only")
    args = parser.parse_args()

    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()

    if args.ticker:
        cur.execute("SELECT id, ticker, cik FROM companies WHERE ticker = %s",
                    (args.ticker.upper(),))
    else:
        cur.execute("SELECT id, ticker, cik FROM companies ORDER BY ticker")

    companies = cur.fetchall()
    if not companies:
        print("No companies found")
        return

    print(f"[INFO] Fetching prices for {len(companies)} companies\n")

    total = 0
    for i, co in enumerate(companies):
        ticker = co["ticker"]
        cik = co["cik"]
        print(f"  [{ticker}]", end=" ")
        try:
            n = fetch_prices_for_company(cur, co["id"], ticker, cik)
            conn.commit()
            print(f"{n} years updated")
            total += n
        except Exception as e:
            print(f"ERROR: {e}")
            conn.rollback()

        if i < len(companies) - 1:
            time.sleep(0.5)

    # Summary
    print(f"\n{'='*55}")
    print(f"PRICES — {total} values updated")
    print(f"{'='*55}\n")

    # Coverage report
    cur.execute("""
        SELECT c.ticker,
               COUNT(*) AS total_years,
               COUNT(m.stock_price_eoy) AS with_price,
               COUNT(*) - COUNT(m.stock_price_eoy) AS missing
        FROM metrics m
        JOIN companies c ON c.id = m.company_id
        GROUP BY c.id, c.ticker
        ORDER BY (COUNT(*) - COUNT(m.stock_price_eoy)) DESC, c.ticker
    """)
    for r in cur.fetchall():
        flag = " <<<" if r["missing"] > 0 else ""
        print(f"  {r['ticker']:<8} {r['with_price']}/{r['total_years']} years{flag}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
