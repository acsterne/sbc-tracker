# SBC Tracker

Flask + PostgreSQL app tracking stock-based compensation across public tech companies using SEC EDGAR data.

## Stack
- **Backend:** Python / Flask
- **Database:** PostgreSQL via psycopg2 (raw SQL, no ORM)
- **Frontend:** Jinja2 templates, vanilla JS, Chart.js via CDN, no build step
- **Hosting:** Railway (`railway.toml`, `Procfile`)
- **Data source:** SEC EDGAR XBRL API (`data.sec.gov/api/xbrl/companyfacts/`), edgartools library for historical ingestion and shares outstanding

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

# Brute-force historical ingestion (slower, more thorough — uses edgartools to enumerate and parse 10-K filings)
DATABASE_URL=postgresql://... python3 fetch_historical.py
DATABASE_URL=postgresql://... python3 fetch_historical.py --ticker SNAP
DATABASE_URL=postgresql://... python3 fetch_historical.py --force  # re-fetch even if data exists
DATABASE_URL=postgresql://... python3 fetch_historical.py --reset-checkpoint  # clear checkpoint and start fresh

# Backfill null shares_outstanding from 10-K cover page text (regex + DEI XBRL fallback)
DATABASE_URL=postgresql://... python3 enrich_shares.py
DATABASE_URL=postgresql://... python3 enrich_shares.py --ticker META
DATABASE_URL=postgresql://... python3 enrich_shares.py --all  # process all companies, even those with no gaps

# validate.py runs standalone for checking data quality after ingestion:
# Validate ingested data against ground-truth benchmarks + sanity rules
DATABASE_URL=postgresql://... python3 validate.py
DATABASE_URL=postgresql://... python3 validate.py --ticker META
DATABASE_URL=postgresql://... python3 validate.py --heal  # null suspect values in metrics
```

## Schema

### `companies`
Master list of tracked companies — ticker, name, CIK (SEC identifier), sector, IPO year.

### `filings`
One row per company per period (annual 10-K and quarterly 10-Q). Raw financials: SBC expense, revenue, gross profit, net income, shares outstanding, shares repurchased, buyback spend. Also stores SBC by function (sbc_cogs, sbc_rd, sbc_sm, sbc_ga), unrecognized_sbc (future expense from unvested awards), ebitda (plus operating_income and depreciation_amortization components for auditability), data_source, and confidence. Unique on (company_id, fiscal_year, form_type).

### `metrics`
Annual metrics computed from 10-K filings only (10-Q data is YTD cumulative and would double-count if summed). Stores pre-computed ratios: sbc_pct_revenue, sbc_pct_ebitda (replaces sbc_pct_gross_profit), ebitda_negative flag, net_dilution_pct, sbc_per_share, revenue_growth_yoy, unrecognized_sbc_annual. Refreshed by `fetch_sbc.py` after each fetch. Unique on (company_id, fiscal_year).

### `company_tags`
Stores which XBRL tag was dynamically selected per company per concept, how many periods it returned, and whether discovery succeeded or flagged `needs_html_parse`. Written on every ingestion run.

## Key architecture decisions
- **Raw SQL, no ORM** — consistent with other projects.
- **3-layer EDGAR fetcher** — `fetch_sbc.py` tries three sources in order: (1) XBRL companyfacts API, (2) XBRL instance document from filing index, (3) HTML/XBRL inline parse. Each layer fills gaps left by the previous. XBRL instance parsing uses BeautifulSoup XML parser (more tolerant of malformed XBRL in older filings).
- **Brute-force historical fetcher** — `fetch_historical.py` uses the `edgartools` library to enumerate and parse every 10-K filing (excludes 10-K/A amendments). Extracts data via `standard_concept` lookups on parsed XBRL statements (income, cash flow, balance sheet), falling back to concept name substring matching. Filters to non-breakdown rows for consolidated totals. Saves checkpoint state to resume after crashes. Prints a coverage report (ticker, year count, earliest/latest year, latest SBC/revenue) at the end.
- **Dynamic XBRL tag discovery** — before extracting data, `discover_tags()` scores every tag in the companyfacts JSON (annual period count + us-gaap namespace bonus + hardcoded-list bonus + concept-specific bonuses) and picks the best tag per concept. Discovered tag is prepended to the hardcoded fallback list so it wins for same-period conflicts; hardcoded tags fill gaps.
- **XBRL concept merge** — each metric (SBC, revenue, etc.) iterates a priority-ordered list of XBRL concept names and merges data across all matching concepts. Earlier concepts win for the same period; later concepts fill gaps. Handles companies that switch XBRL tags between years (e.g. Alphabet revenue).
- **Coverage matrix** — after a full ingestion run, `print_coverage_matrix()` prints a GREEN/YELLOW/RED matrix showing % of expected annual periods filled per company × concept; flags cells below 70%.
- **Upsert preserves existing data** — filings upsert uses `COALESCE(filings.field, EXCLUDED.field)` so existing non-null values are never overwritten. Only null fields get filled. To fix bad data: delete the company's rows first, then re-ingest.
- **Share enrichment (3-source cascade)** — `enrich_shares.py` fills NULL `shares_outstanding` in 10-K filings using three sources in priority order: (1) DEI `EntityCommonStockSharesOutstanding` from companyfacts API, (2) us-gaap `CommonStockSharesOutstanding` from companyfacts API (sums multi-class shares by accession, takes most recently filed), (3) 10-K cover page regex via edgartools (final fallback). Only updates NULL rows.
- **Validation layer** — `validate.py` checks ingested data against 20 ground-truth benchmarks (10 companies × SBC + revenue, 5-10% tolerance) and sanity rules (YoY change limits, SBC/revenue ratio bounds, magnitude floors by market cap tier). `--heal` flag nulls suspect values in the metrics table so the UI shows "—" instead of bad numbers.
- **Precomputed metrics table** — ratios stored in DB, not computed on every request.
- **EDGAR rate limit** — SEC allows ~10 req/sec; we sleep 0.5s between companies to be safe.
- **EDGAR User-Agent required** — SEC blocks generic agents; use a descriptive User-Agent with contact email.

## Routes
| Path | Purpose |
|---|---|
| `/` | Leaderboard — all companies, latest year, sortable by any metric, filterable by ticker/name |
| `/company/<ticker>` | Company detail — historical charts (profitability, dilution/ownership) + year-by-year table |
| `/scatter` | Scatter plot: SBC % Revenue (Y) vs Revenue Growth (X) |
| `/analysis` | Cross-company analysis — 5 tabbed charts (Paying for Growth, Worst Offenders, Buyback Offset, Cumulative Dilution, SBC Efficiency) with sector/revenue filters |
| `/api/debug/coverage` | JSON: per-company data coverage (most recent year, years with SBC data) |

## Adding companies
Edit `companies.py`. Find CIK at: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany

## General principles
- Keep it simple. Prefer direct solutions — raw SQL over abstraction.
- Don't guess CIK values — look them up on EDGAR.
- EDGAR XBRL data is available from ~2009 onward (XBRL mandate for large filers).
