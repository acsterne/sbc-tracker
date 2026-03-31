-- SBC Tracker Schema
-- Run once to initialize: psql $DATABASE_URL -f schema.sql

-- Companies we track
CREATE TABLE IF NOT EXISTS companies (
    id              SERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    cik             TEXT NOT NULL UNIQUE,   -- SEC CIK identifier (zero-padded to 10 digits)
    sector          TEXT,                   -- e.g. 'Mega Cap', 'SaaS', 'AI / Neo-Cloud'
    ipo_year        INT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- One row per company per fiscal period (annual or quarterly)
CREATE TABLE IF NOT EXISTS filings (
    id                      SERIAL PRIMARY KEY,
    company_id              INT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    period_end              DATE NOT NULL,          -- e.g. 2023-12-31
    fiscal_year             INT NOT NULL,           -- e.g. 2023
    fiscal_quarter          INT,                    -- 1-4; NULL = annual (10-K)
    form_type               TEXT NOT NULL,          -- '10-K' or '10-Q'

    -- Raw financials (in USD)
    sbc_expense             BIGINT,                 -- Stock-based compensation expense
    revenue                 BIGINT,
    gross_profit            BIGINT,
    net_income              BIGINT,

    -- Share data
    shares_outstanding      BIGINT,                 -- Basic shares outstanding at period end
    shares_repurchased      BIGINT,                 -- Shares bought back during period (from cash flow stmt)
    buyback_spend           BIGINT,                 -- Cash spent on buybacks during period

    -- Metadata
    accession_number        TEXT,                   -- EDGAR accession number for traceability
    fetched_at              TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(company_id, period_end, form_type)
);

-- Materialized metrics (computed from filings, refreshed after each fetch)
-- Storing these avoids recomputing on every page load
CREATE TABLE IF NOT EXISTS metrics (
    id                      SERIAL PRIMARY KEY,
    company_id              INT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    fiscal_year             INT NOT NULL,

    -- Annual aggregates (summed from quarterly or taken from 10-K)
    sbc_annual              BIGINT,
    revenue_annual          BIGINT,
    gross_profit_annual     BIGINT,
    net_income_annual       BIGINT,
    buyback_spend_annual    BIGINT,
    shares_repurchased_annual BIGINT,

    -- Period-end share count (from 10-K)
    shares_outstanding_eoy  BIGINT,

    -- Computed ratios
    sbc_pct_revenue         NUMERIC(8,4),           -- SBC / Revenue %
    sbc_pct_gross_profit    NUMERIC(8,4),           -- SBC / Gross Profit %
    net_dilution_pct        NUMERIC(8,4),           -- (SBC shares - buyback shares) / shares outstanding %
    sbc_per_share           NUMERIC(12,4),          -- SBC expense / shares outstanding

    -- YoY revenue growth (filled in after at least 2 years of data)
    revenue_growth_yoy      NUMERIC(8,4),

    computed_at             TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(company_id, fiscal_year)
);

-- Index for fast leaderboard queries
CREATE INDEX IF NOT EXISTS idx_metrics_year ON metrics(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_filings_company ON filings(company_id, period_end);
