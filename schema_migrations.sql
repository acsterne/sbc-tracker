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
