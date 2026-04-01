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

-- Dynamic tag discovery (run after EBITDA migrations above)
CREATE TABLE IF NOT EXISTS company_tags (
    id              SERIAL PRIMARY KEY,
    company_id      INT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    concept         TEXT NOT NULL,   -- 'sbc', 'revenue', 'gross_profit', 'net_income',
                                     --   'operating_income', 'da', 'buybacks'
    tag_used        TEXT,            -- XBRL tag name selected, e.g. 'ShareBasedCompensation'
    namespace       TEXT,            -- 'us-gaap' or extension namespace
    periods_found   INT DEFAULT 0,   -- actual 10-K periods extracted after merge
    source          TEXT DEFAULT 'dynamic',  -- 'dynamic', 'hardcoded_only', 'needs_html_parse'
    discovered_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_id, concept)
);
