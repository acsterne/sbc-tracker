# SBC Tracker

Flask + PostgreSQL app tracking stock-based compensation across public tech companies using SEC EDGAR data.

## Stack
- **Backend:** Python / Flask
- **Database:** PostgreSQL via psycopg2 (raw SQL, no ORM)
- **Frontend:** Jinja2 templates, vanilla JS, Chart.js via CDN, no build step
- **Hosting:** Railway (`railway.toml`, `Procfile`)
- **Data source:** SEC EDGAR XBRL API (`data.sec.gov/api/xbrl/companyfacts/`)

## Running locally
```bash
DATABASE_URL=postgresql://... python app.py
```

## Populating data
```bash
# All companies (takes ~5 min, sleeps 0.5s between companies to respect SEC rate limits)
DATABASE_URL=postgresql://... python3 fetch_sbc.py

# Single company
DATABASE_URL=postgresql://... python3 fetch_sbc.py --ticker SNAP
```

## Schema

### `companies`
Master list of tracked companies — ticker, name, CIK (SEC identifier), sector, IPO year.

### `filings`
One row per company per period (annual 10-K and quarterly 10-Q). Raw financials: SBC expense, revenue, gross profit, net income, shares outstanding, shares repurchased, buyback spend. Indexed by (company_id, period_end, form_type).

### `metrics`
Annual metrics computed from 10-K filings only (10-Q data is YTD cumulative and would double-count if summed). Stores pre-computed ratios: sbc_pct_revenue, sbc_pct_gross_profit, net_dilution_pct, sbc_per_share, revenue_growth_yoy. Refreshed by `fetch_sbc.py` after each fetch. Unique on (company_id, fiscal_year).

## Key architecture decisions
- **Raw SQL, no ORM** — consistent with other projects.
- **XBRL concept fallback chain** — each metric (SBC, revenue, etc.) tries a priority-ordered list of XBRL concept names since companies use different tags.
- **Precomputed metrics table** — ratios stored in DB, not computed on every request.
- **EDGAR rate limit** — SEC allows ~10 req/sec; we sleep 0.5s between companies to be safe.
- **EDGAR User-Agent required** — SEC blocks generic agents; use a descriptive User-Agent with contact email.

## Routes
| Path | Purpose |
|---|---|
| `/` | Leaderboard — all companies, latest year, sortable by any metric |
| `/company/<ticker>` | Company detail — historical chart + year-by-year table |
| `/scatter` | Scatter plot: SBC % Revenue (Y) vs Revenue Growth (X) |

## Adding companies
Edit `companies.py`. Find CIK at: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany

## General principles
- Keep it simple. Prefer direct solutions — raw SQL over abstraction.
- Don't guess CIK values — look them up on EDGAR.
- EDGAR XBRL data is available from ~2009 onward (XBRL mandate for large filers).
