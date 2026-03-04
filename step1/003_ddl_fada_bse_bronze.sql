-- ============================================================
-- AutoQuant — FADA + BSE Bronze Tables (STEP 5 addition)
-- ============================================================

SET search_path TO autoquant;

-- ── FADA Monthly Report Bronze ──
CREATE TABLE IF NOT EXISTS raw_fada_report (
    id              BIGSERIAL PRIMARY KEY,
    report_period   VARCHAR(7) NOT NULL,            -- 'YYYY-MM'
    oem_name        VARCHAR(200) NOT NULL,          -- 'INDUSTRY_TOTAL' for segment totals
    segment         VARCHAR(10) NOT NULL,           -- 'PV', 'CV', '2W', '3W'
    volume          BIGINT NOT NULL DEFAULT 0,
    yoy_pct         DECIMAL(6,2),                   -- YoY percentage change
    data_type       VARCHAR(20) NOT NULL DEFAULT 'retail',  -- 'retail' or 'wholesale'
    source_page     SMALLINT,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(report_period, oem_name, segment, data_type)
);

CREATE INDEX IF NOT EXISTS idx_fada_period ON raw_fada_report (report_period);
CREATE INDEX IF NOT EXISTS idx_fada_oem ON raw_fada_report (oem_name);

COMMENT ON TABLE raw_fada_report IS 'Bronze: Monthly FADA report data — segment totals and OEM dispatches.';

-- ── BSE Wholesale Dispatch Bronze ──
CREATE TABLE IF NOT EXISTS raw_bse_wholesale (
    id              BIGSERIAL PRIMARY KEY,
    period          VARCHAR(7) NOT NULL,            -- 'YYYY-MM'
    ticker          VARCHAR(30) NOT NULL,           -- NSE ticker for cross-ref
    oem_name        VARCHAR(200) NOT NULL,
    segment         VARCHAR(10) NOT NULL,           -- 'PV', 'CV', '2W', 'TOTAL', 'ALL'
    volume          BIGINT NOT NULL DEFAULT 0,
    powertrain      VARCHAR(10) NOT NULL DEFAULT 'ALL',  -- 'ALL', 'EV', 'ICE'
    data_type       VARCHAR(20) NOT NULL DEFAULT 'wholesale',
    filing_date     VARCHAR(20),
    attachment_url  TEXT,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(period, ticker, segment, powertrain)
);

CREATE INDEX IF NOT EXISTS idx_bse_period ON raw_bse_wholesale (period);
CREATE INDEX IF NOT EXISTS idx_bse_ticker ON raw_bse_wholesale (ticker);

COMMENT ON TABLE raw_bse_wholesale IS 'Bronze: BSE corporate filings — OEM wholesale dispatch/sales numbers.';
