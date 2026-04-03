"""
SBC Tracker — Flask app
Tracks stock-based compensation across major public tech companies using EDGAR data.
"""

import os
import json
from flask import Flask, render_template, request, jsonify
import psycopg2
import psycopg2.extras

app = Flask(__name__)
DATABASE_URL = os.environ["DATABASE_URL"]


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor, connect_timeout=10)


# ── Jinja filters ────────────────────────────────────────────────────────────

def fmt_compact(val):
    """Format large numbers as $1.7B, $250M, $1.5K etc."""
    if val is None:
        return "—"
    v = float(val)
    if abs(v) >= 1_000_000_000:
        return f"${v / 1_000_000_000:.1f}B"
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:.0f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:.0f}K"
    return f"${v:.0f}"

def fmt_pct(val, decimals=1):
    if val is None:
        return "—"
    return f"{float(val):.{decimals}f}%"

def fmt_pct2(val):
    return fmt_pct(val, decimals=2)

def fmt_number(val):
    if val is None:
        return "—"
    return f"{int(val):,}"

app.jinja_env.filters["compact"] = fmt_compact
app.jinja_env.filters["pct"] = fmt_pct
app.jinja_env.filters["pct2"] = fmt_pct2
app.jinja_env.filters["number"] = fmt_number


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """
    Leaderboard: latest year metrics for all companies, sortable.
    Default sort: SBC % of Revenue descending.
    """
    sort = request.args.get("sort", "sbc_pct_revenue")
    order = request.args.get("order", "desc")
    sector = request.args.get("sector", "")
    year = request.args.get("year", "")

    valid_sorts = {
        "sbc_pct_revenue", "sbc_pct_ebitda", "sbc_annual",
        "revenue_annual", "net_dilution_pct", "revenue_growth_yoy",
        "buyback_spend_annual", "name", "ticker", "sector", "fiscal_year",
    }
    if sort not in valid_sorts:
        sort = "sbc_pct_revenue"
    order_sql = "DESC" if order == "desc" else "ASC"

    conn = get_db()
    cur = conn.cursor()

    # Available years for filter
    cur.execute("SELECT DISTINCT fiscal_year FROM metrics ORDER BY fiscal_year DESC")
    available_years = [r["fiscal_year"] for r in cur.fetchall()]

    # Available sectors
    cur.execute("SELECT DISTINCT sector FROM companies ORDER BY sector")
    available_sectors = [r["sector"] for r in cur.fetchall()]

    # If no year filter, use the most recent year per company
    if year:
        year_filter = "AND m.fiscal_year = %(year)s"
    else:
        year_filter = """
            AND m.fiscal_year = (
                SELECT MAX(m2.fiscal_year) FROM metrics m2
                WHERE m2.company_id = m.company_id AND m2.sbc_annual IS NOT NULL
            )
        """

    sector_filter = "AND c.sector = %(sector)s" if sector else ""

    cur.execute(f"""
        SELECT
            c.ticker, c.name, c.sector, c.ipo_year,
            m.fiscal_year,
            m.sbc_annual, m.revenue_annual,
            m.buyback_spend_annual, m.shares_outstanding_eoy,
            m.sbc_pct_revenue, m.sbc_pct_ebitda, m.ebitda_annual, m.ebitda_negative,
            m.net_dilution_pct, m.sbc_per_share,
            m.revenue_growth_yoy
        FROM metrics m
        JOIN companies c ON c.id = m.company_id
        WHERE 1=1
            {year_filter}
            {sector_filter}
        ORDER BY {sort} {order_sql} NULLS LAST
    """, {"year": year or None, "sector": sector or None})
    rows = cur.fetchall()

    # Summary stats for the header cards
    cur.execute("""
        SELECT
            COUNT(DISTINCT m.company_id) AS company_count,
            SUM(sbc_annual) AS total_sbc,
            AVG(sbc_pct_revenue) AS avg_sbc_pct_rev,
            MAX(m.fiscal_year) AS latest_year,
            MAX(f.fetched_at) AS last_updated
        FROM metrics m
        JOIN filings f ON f.company_id = m.company_id
        WHERE m.fiscal_year = (
            SELECT MAX(m2.fiscal_year) FROM metrics m2 WHERE m2.company_id = m.company_id
        )
    """)
    summary = cur.fetchone()

    cur.close()
    conn.close()

    return render_template("index.html",
        rows=rows,
        sort=sort,
        order=order,
        sector=sector,
        year=year,
        available_years=available_years,
        available_sectors=available_sectors,
        summary=summary,
    )


@app.route("/company/<ticker>")
def company(ticker):
    """Company detail page: historical SBC trend + all metrics by year."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM companies WHERE ticker = %s", (ticker.upper(),))
    co = cur.fetchone()
    if not co:
        return "Company not found", 404

    # First 10-K year = single source of truth for IPO year display
    cur.execute("""
        SELECT MIN(fiscal_year) AS yr FROM filings
        WHERE company_id = %s AND form_type = '10-K' AND sbc_expense IS NOT NULL
    """, (co["id"],))
    row = cur.fetchone()
    first_10k_year = row["yr"] if row else co["ipo_year"]

    cur.execute("""
        SELECT
            m.fiscal_year,
            m.sbc_annual, m.revenue_annual, m.gross_profit_annual,
            m.net_income_annual, m.buyback_spend_annual,
            m.shares_outstanding_eoy, m.shares_repurchased_annual,
            m.sbc_pct_revenue, m.sbc_pct_ebitda, m.ebitda_annual, m.ebitda_negative,
            m.net_dilution_pct, m.sbc_per_share,
            m.revenue_growth_yoy, m.unrecognized_sbc_annual,
            m.stock_price_eoy, m.market_cap, m.sbc_pct_market_cap
        FROM metrics m
        WHERE m.company_id = %s AND m.sbc_annual IS NOT NULL
        ORDER BY m.fiscal_year ASC
    """, (co["id"],))
    history = cur.fetchall()

    # Data for chart.js — serialize to lists
    chart_years     = [r["fiscal_year"] for r in history]
    chart_sbc       = [float(r["sbc_annual"] or 0) / 1e6 for r in history]
    chart_rev       = [float(r["revenue_annual"] or 0) / 1e6 for r in history]
    chart_pct       = [float(r["sbc_pct_revenue"] or 0) for r in history]
    chart_bb        = [float(r["buyback_spend_annual"] or 0) / 1e6 for r in history]
    chart_ni        = [float(r["net_income_annual"] or 0) / 1e6 for r in history]
    chart_unrec     = [float(r["unrecognized_sbc_annual"] or 0) / 1e6 for r in history]
    chart_shares    = [float(r["shares_outstanding_eoy"] or 0) / 1e6 for r in history]
    chart_bb_shares = [float(r["shares_repurchased_annual"] or 0) / 1e6 for r in history]
    chart_price     = [float(r["stock_price_eoy"]) if r["stock_price_eoy"] else None for r in history]
    chart_mcap      = [float(r["market_cap"] or 0) / 1e9 for r in history]
    chart_sbc_pct_mcap = [float(r["sbc_pct_market_cap"]) if r["sbc_pct_market_cap"] else None for r in history]

    cur.close()
    conn.close()

    return render_template("company.html",
        co=co,
        history=history,
        first_10k_year=first_10k_year,
        chart_years=chart_years,
        chart_sbc=chart_sbc,
        chart_rev=chart_rev,
        chart_pct=chart_pct,
        chart_bb=chart_bb,
        chart_ni=chart_ni,
        chart_unrec=chart_unrec,
        chart_shares=chart_shares,
        chart_bb_shares=chart_bb_shares,
        chart_price=chart_price,
        chart_mcap=chart_mcap,
        chart_sbc_pct_mcap=chart_sbc_pct_mcap,
    )


@app.route("/api/debug/coverage")
def debug_coverage():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            c.ticker,
            MAX(m.fiscal_year) AS most_recent_fiscal_year,
            (SELECT m2.sbc_annual FROM metrics m2
             WHERE m2.company_id = c.id
             ORDER BY m2.fiscal_year DESC LIMIT 1) AS most_recent_sbc_total,
            COUNT(m.fiscal_year) AS total_years_in_db,
            COUNT(m.sbc_annual) AS total_years_with_sbc
        FROM companies c
        LEFT JOIN metrics m ON m.company_id = c.id
        GROUP BY c.id, c.ticker
        ORDER BY c.ticker
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify({"companies": [
        {
            "ticker": r["ticker"],
            "most_recent_fiscal_year": r["most_recent_fiscal_year"],
            "most_recent_sbc_total": float(r["most_recent_sbc_total"]) if r["most_recent_sbc_total"] else None,
            "total_years_with_sbc": r["total_years_with_sbc"],
            "total_years_in_db": r["total_years_in_db"],
        }
        for r in rows
    ]})


@app.route("/analysis")
def analysis():
    """Analysis page with 5 cross-company charts."""
    sector = request.args.get("sector", "")
    min_rev = request.args.get("min_rev", "0")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT sector FROM companies ORDER BY sector")
    available_sectors = [r["sector"] for r in cur.fetchall()]

    # Base filter: latest year per company where SBC is not null
    rev_filter = ""
    params = {}
    if min_rev and min_rev != "0":
        rev_filter = "AND m.revenue_annual >= %(min_rev)s"
        params["min_rev"] = int(min_rev)
    sector_filter = "AND c.sector = %(sector)s" if sector else ""
    if sector:
        params["sector"] = sector

    # All companies, latest year, with key metrics
    cur.execute(f"""
        SELECT
            c.ticker, c.name, c.sector,
            m.fiscal_year,
            m.sbc_annual, m.revenue_annual, m.buyback_spend_annual,
            m.sbc_pct_revenue, m.revenue_growth_yoy,
            m.shares_outstanding_eoy, m.net_income_annual,
            m.market_cap, m.sbc_pct_market_cap, m.stock_price_eoy
        FROM metrics m
        JOIN companies c ON c.id = m.company_id
        WHERE m.sbc_annual IS NOT NULL
          AND m.fiscal_year = (
              SELECT MAX(m2.fiscal_year) FROM metrics m2
              WHERE m2.company_id = m.company_id AND m2.sbc_annual IS NOT NULL
          )
          {rev_filter}
          {sector_filter}
        ORDER BY c.ticker
    """, params)
    latest = cur.fetchall()

    # Historical shares for cumulative dilution
    cur.execute(f"""
        SELECT c.ticker,
            FIRST_VALUE(m.shares_outstanding_eoy) OVER (
                PARTITION BY c.ticker ORDER BY m.fiscal_year ASC
            ) AS first_shares,
            LAST_VALUE(m.shares_outstanding_eoy) OVER (
                PARTITION BY c.ticker ORDER BY m.fiscal_year ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_shares
        FROM metrics m
        JOIN companies c ON c.id = m.company_id
        WHERE m.shares_outstanding_eoy IS NOT NULL
          AND m.sbc_annual IS NOT NULL
        ORDER BY c.ticker, m.fiscal_year
    """)
    dilution_rows = cur.fetchall()
    # Deduplicate to one row per ticker
    dilution = {}
    for r in dilution_rows:
        dilution[r["ticker"]] = {
            "first_shares": r["first_shares"],
            "last_shares": r["last_shares"],
        }

    cur.close()
    conn.close()

    # Build data for charts
    companies_data = []
    for r in latest:
        sbc = float(r["sbc_annual"] or 0)
        rev = float(r["revenue_annual"] or 0)
        bb  = float(r["buyback_spend_annual"] or 0)
        pct_rev = float(r["sbc_pct_revenue"] or 0)
        growth  = float(r["revenue_growth_yoy"] or 0)
        d = dilution.get(r["ticker"], {})

        companies_data.append({
            "ticker":       r["ticker"],
            "name":         r["name"],
            "sector":       r["sector"],
            "fiscal_year":  r["fiscal_year"],
            "sbc":          sbc,
            "revenue":      rev,
            "buybacks":     bb,
            "sbc_pct_rev":  pct_rev,
            "growth":       growth,
            "net_dilution_burden": ((sbc - bb) / rev * 100) if rev > 0 else 0,
            "buyback_offset": (bb / sbc * 100) if sbc > 0 else 0,
            "sbc_efficiency": (rev / sbc) if sbc > 0 else 0,
            "market_cap":     float(r["market_cap"] or 0),
            "sbc_pct_mcap":   float(r["sbc_pct_market_cap"] or 0),
            "cum_dilution":   ((float(d.get("last_shares", 0)) - float(d.get("first_shares", 0)))
                               / float(d["first_shares"]) * 100)
                              if d.get("first_shares") and float(d["first_shares"]) > 0 else None,
        })

    import json as _json
    return render_template("analysis.html",
        data=_json.dumps(companies_data),
        sector=sector,
        min_rev=min_rev,
        available_sectors=available_sectors,
    )


@app.route("/scatter")
def scatter():
    """
    Scatter plot: SBC % Revenue (Y) vs Revenue Growth (X).
    Lets users see which companies are 'earning' their SBC.
    """
    year = request.args.get("year", "")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT fiscal_year FROM metrics ORDER BY fiscal_year DESC")
    available_years = [r["fiscal_year"] for r in cur.fetchall()]

    if not year and available_years:
        year = str(available_years[0])

    if year:
        cur.execute("""
            SELECT
                c.ticker, c.name, c.sector,
                m.sbc_pct_revenue, m.revenue_growth_yoy,
                m.sbc_annual, m.revenue_annual, m.fiscal_year
            FROM metrics m
            JOIN companies c ON c.id = m.company_id
            WHERE m.fiscal_year = %s
              AND m.sbc_pct_revenue IS NOT NULL
              AND m.revenue_growth_yoy IS NOT NULL
            ORDER BY c.name
        """, (year,))
        points = cur.fetchall()
    else:
        points = []

    cur.close()
    conn.close()

    return render_template("scatter.html",
        points=points,
        year=year,
        available_years=available_years,
    )


@app.route("/peers")
def peers():
    """Peer comparison — one chart, toggle metric / year / peer group."""
    metric  = request.args.get("metric", "sbc_pct_revenue")
    year    = request.args.get("year", "")
    group   = request.args.get("group", "all")  # all or a sector name
    ticker  = request.args.get("ticker", "")     # optional highlight

    METRIC_DEFS = {
        "sbc_pct_revenue":    {"label": "SBC % Revenue",      "col": "m.sbc_pct_revenue",    "suffix": "%"},
        "sbc_pct_market_cap": {"label": "SBC % Market Cap",   "col": "m.sbc_pct_market_cap", "suffix": "%"},
        "sbc_pct_ebitda":     {"label": "SBC % EBITDA",       "col": "m.sbc_pct_ebitda",     "suffix": "%"},
        "net_dilution_pct":   {"label": "Net Dilution %",     "col": "m.net_dilution_pct",   "suffix": "%"},
        "revenue_growth_yoy": {"label": "Revenue Growth YoY", "col": "m.revenue_growth_yoy", "suffix": "%"},
        "sbc_per_share":      {"label": "SBC per Share ($)",  "col": "m.sbc_per_share",      "suffix": ""},
    }
    if metric not in METRIC_DEFS:
        metric = "sbc_pct_revenue"
    mdef = METRIC_DEFS[metric]

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT sector FROM companies ORDER BY sector")
    available_sectors = [r["sector"] for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT fiscal_year FROM metrics ORDER BY fiscal_year DESC")
    available_years = [r["fiscal_year"] for r in cur.fetchall()]
    if not year and available_years:
        year = str(available_years[0])

    sector_filter = ""
    params = {"year": int(year)} if year else {}
    if group and group != "all":
        sector_filter = "AND c.sector = %(sector)s"
        params["sector"] = group

    cur.execute(f"""
        SELECT c.ticker, c.name, c.sector,
               {mdef['col']} AS metric_val
        FROM metrics m
        JOIN companies c ON c.id = m.company_id
        WHERE m.fiscal_year = %(year)s
          AND {mdef['col']} IS NOT NULL
          {sector_filter}
        ORDER BY {mdef['col']} DESC
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    peers_data = [{"ticker": r["ticker"], "name": r["name"], "sector": r["sector"],
                    "val": float(r["metric_val"])} for r in rows]
    vals = [p["val"] for p in peers_data]
    peer_avg = sum(vals) / len(vals) if vals else 0
    sorted_vals = sorted(vals)
    peer_median = sorted_vals[len(sorted_vals) // 2] if sorted_vals else 0

    import json as _json
    return render_template("peers.html",
        peers_data=_json.dumps(peers_data),
        ticker=ticker, metric=metric, year=year, group=group,
        metric_label=mdef["label"], metric_suffix=mdef["suffix"],
        peer_avg=peer_avg, peer_median=peer_median, peer_count=len(peers_data),
        available_sectors=available_sectors, available_years=available_years,
        metric_defs={k: v["label"] for k, v in METRIC_DEFS.items()},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5001)
