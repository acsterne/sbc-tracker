-- Run this on existing DB to add new columns
-- Safe to run multiple times (IF NOT EXISTS)

ALTER TABLE filings
    ADD COLUMN IF NOT EXISTS data_source TEXT,
    ADD COLUMN IF NOT EXISTS confidence TEXT,
    ADD COLUMN IF NOT EXISTS unrecognized_sbc BIGINT,
    ADD COLUMN IF NOT EXISTS sbc_cogs BIGINT,
    ADD COLUMN IF NOT EXISTS sbc_rd BIGINT,
    ADD COLUMN IF NOT EXISTS sbc_sm BIGINT,
    ADD COLUMN IF NOT EXISTS sbc_ga BIGINT;

ALTER TABLE metrics
    ADD COLUMN IF NOT EXISTS unrecognized_sbc_annual BIGINT;

-- EBITDA components (run after initial migrations above)
ALTER TABLE filings
    ADD COLUMN IF NOT EXISTS operating_income BIGINT,
    ADD COLUMN IF NOT EXISTS depreciation_amortization BIGINT,
    ADD COLUMN IF NOT EXISTS ebitda BIGINT,
    ADD COLUMN IF NOT EXISTS ebitda_source TEXT;

ALTER TABLE metrics
    ADD COLUMN IF NOT EXISTS ebitda_annual BIGINT,
    ADD COLUMN IF NOT EXISTS sbc_pct_ebitda NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS ebitda_negative BOOLEAN;

-- Widen ratio columns from NUMERIC(8,4) to NUMERIC(12,4)
-- NUMERIC(8,4) maxes at 9999.9999 — SBC % of EBITDA can exceed that for loss-making companies
ALTER TABLE metrics ALTER COLUMN sbc_pct_revenue      TYPE NUMERIC(12,4);
ALTER TABLE metrics ALTER COLUMN sbc_pct_gross_profit  TYPE NUMERIC(12,4);
ALTER TABLE metrics ALTER COLUMN net_dilution_pct      TYPE NUMERIC(12,4);
ALTER TABLE metrics ALTER COLUMN revenue_growth_yoy    TYPE NUMERIC(12,4);
ALTER TABLE metrics ALTER COLUMN sbc_pct_ebitda        TYPE NUMERIC(12,4);

-- Deduplicate filings: keep best row per (company_id, fiscal_year, form_type)
-- Run BEFORE the constraint change below
DELETE FROM filings f
WHERE f.id NOT IN (
    SELECT DISTINCT ON (company_id, fiscal_year, form_type) id
    FROM filings
    ORDER BY company_id, fiscal_year, form_type,
        (CASE WHEN sbc_expense IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN net_income IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN shares_outstanding IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN buyback_spend IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN operating_income IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN depreciation_amortization IS NOT NULL THEN 1 ELSE 0 END) DESC
);

-- Switch unique constraint from (company_id, period_end, form_type)
-- to (company_id, fiscal_year, form_type) to prevent future duplicates
ALTER TABLE filings DROP CONSTRAINT IF EXISTS filings_company_id_period_end_form_type_key;
ALTER TABLE filings ADD CONSTRAINT filings_company_fiscal_year_form_type_key
    UNIQUE (company_id, fiscal_year, form_type);

-- Dynamic tag discovery (run after EBITDA migrations above)
CREATE TABLE IF NOT EXISTS company_tags (
    id            SERIAL PRIMARY KEY,
    company_id    INT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    concept       TEXT NOT NULL,
    tag_used      TEXT,
    namespace     TEXT,
    periods_found INT DEFAULT 0,
    source        TEXT DEFAULT 'dynamic',
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_id, concept)
);
