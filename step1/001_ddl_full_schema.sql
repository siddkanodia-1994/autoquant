-- ============================================================================
-- AutoQuant: India Auto Registrations & Demand Dashboard
-- Full PostgreSQL DDL — All Tables, Indexes, Constraints, Materialized Views
-- Version: 1.0.0
-- ============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy alias matching

-- ============================================================================
-- SCHEMA SETUP
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS autoquant;
SET search_path TO autoquant, public;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- dim_date: Calendar + Indian Financial Year dimensions
-- Covers 2016-01-01 through 2027-12-31
-- ---------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key           DATE        PRIMARY KEY,
    calendar_year      SMALLINT    NOT NULL,
    calendar_month     SMALLINT    NOT NULL CHECK (calendar_month BETWEEN 1 AND 12),
    calendar_quarter   SMALLINT    NOT NULL CHECK (calendar_quarter BETWEEN 1 AND 4),
    fy_year            VARCHAR(6)  NOT NULL,  -- e.g. 'FY26' (Apr 2025 – Mar 2026)
    fy_quarter         VARCHAR(8)  NOT NULL,  -- e.g. 'Q3FY26'
    fy_quarter_num     SMALLINT    NOT NULL CHECK (fy_quarter_num BETWEEN 1 AND 4),
    month_name         VARCHAR(9)  NOT NULL,
    day_of_week        SMALLINT    NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),  -- 0=Mon
    is_weekend         BOOLEAN     NOT NULL
);

CREATE INDEX idx_dim_date_fy ON dim_date (fy_year, fy_quarter_num);
CREATE INDEX idx_dim_date_cal ON dim_date (calendar_year, calendar_month);

-- ---------------------------------------------------------------------------
-- dim_oem: Listed + unlisted OEM entities
-- ---------------------------------------------------------------------------
CREATE TABLE dim_oem (
    oem_id             SERIAL       PRIMARY KEY,
    oem_name           VARCHAR(200) NOT NULL UNIQUE,
    nse_ticker         VARCHAR(30),          -- NULL for unlisted
    bse_code           VARCHAR(10),          -- NULL for unlisted
    is_listed          BOOLEAN      NOT NULL DEFAULT FALSE,
    is_in_scope        BOOLEAN      NOT NULL DEFAULT TRUE,
    primary_segments   TEXT[],               -- e.g. ARRAY['PV','CV']
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dim_oem_ticker ON dim_oem (nse_ticker) WHERE nse_ticker IS NOT NULL;
CREATE INDEX idx_dim_oem_listed ON dim_oem (is_listed, is_in_scope);

-- ---------------------------------------------------------------------------
-- dim_oem_alias: Source-specific maker name → canonical OEM mapping
-- ---------------------------------------------------------------------------
CREATE TABLE dim_oem_alias (
    alias_id           SERIAL       PRIMARY KEY,
    oem_id             INT          NOT NULL REFERENCES dim_oem(oem_id) ON DELETE CASCADE,
    source             VARCHAR(20)  NOT NULL,  -- 'VAHAN', 'FADA', 'BSE'
    alias_name         VARCHAR(300) NOT NULL,
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source, alias_name)
);

CREATE INDEX idx_dim_oem_alias_lookup ON dim_oem_alias (source, alias_name) WHERE is_active = TRUE;
CREATE INDEX idx_dim_oem_alias_oem ON dim_oem_alias (oem_id);

-- ---------------------------------------------------------------------------
-- dim_segment: PV / CV / 2W with optional sub-segments
-- ---------------------------------------------------------------------------
CREATE TABLE dim_segment (
    segment_id         SERIAL       PRIMARY KEY,
    segment_code       VARCHAR(5)   NOT NULL,  -- 'PV', 'CV', '2W'
    segment_name       VARCHAR(50)  NOT NULL,
    sub_segment        VARCHAR(50),            -- 'LCV', 'MHCV', 'Motorcycle', 'Scooter', etc.
    UNIQUE (segment_code, sub_segment)
);

-- ---------------------------------------------------------------------------
-- dim_vehicle_class_map: VAHAN vehicle class → segment mapping
-- ---------------------------------------------------------------------------
CREATE TABLE dim_vehicle_class_map (
    map_id             SERIAL       PRIMARY KEY,
    vahan_class_name   VARCHAR(200) NOT NULL UNIQUE,
    segment_id         INT          REFERENCES dim_segment(segment_id),  -- NULL if excluded
    is_excluded        BOOLEAN      NOT NULL DEFAULT FALSE,
    notes              TEXT
);

CREATE INDEX idx_vcm_segment ON dim_vehicle_class_map (segment_id) WHERE is_excluded = FALSE;

-- ---------------------------------------------------------------------------
-- dim_fuel: Fuel type → powertrain / dashboard bucket mapping
-- ---------------------------------------------------------------------------
CREATE TABLE dim_fuel (
    fuel_id            SERIAL       PRIMARY KEY,
    fuel_code          VARCHAR(50)  NOT NULL UNIQUE,
    powertrain         VARCHAR(10)  NOT NULL CHECK (powertrain IN ('ICE', 'EV', 'HYBRID')),
    dashboard_bucket   VARCHAR(5)   NOT NULL CHECK (dashboard_bucket IN ('ICE', 'EV')),
    fuel_group         VARCHAR(20)  NOT NULL  -- 'Petrol', 'Diesel', 'CNG', 'CNG/LPG', 'Hybrid', 'Electric', 'Other'
);

-- ---------------------------------------------------------------------------
-- dim_geo: Geography hierarchy (V1: national only; V2: state/RTO)
-- ---------------------------------------------------------------------------
CREATE TABLE dim_geo (
    geo_id             SERIAL       PRIMARY KEY,
    level              VARCHAR(10)  NOT NULL CHECK (level IN ('NATIONAL', 'STATE', 'RTO')),
    state_name         VARCHAR(100),
    rto_code           VARCHAR(20),
    rto_name           VARCHAR(200),
    vahan4_active      BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_geo_level ON dim_geo (level);
CREATE INDEX idx_dim_geo_state ON dim_geo (state_name) WHERE level = 'STATE';

-- ============================================================================
-- BRONZE TABLES (Raw / Immutable)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- raw_extraction_log: Audit trail for every extraction run
-- ---------------------------------------------------------------------------
CREATE TABLE raw_extraction_log (
    run_id             SERIAL       PRIMARY KEY,
    source             VARCHAR(30)  NOT NULL,  -- 'VAHAN', 'FADA', 'BSE_WHOLESALE'
    started_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at       TIMESTAMPTZ,
    status             VARCHAR(20)  NOT NULL DEFAULT 'RUNNING'
                       CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'VALIDATION_FAILED')),
    records_extracted  INT,
    error_message      TEXT,
    notes              TEXT
);

CREATE INDEX idx_rel_source_status ON raw_extraction_log (source, status, started_at DESC);

-- ---------------------------------------------------------------------------
-- raw_vahan_snapshot: Immutable copy of every VAHAN extraction
-- ---------------------------------------------------------------------------
CREATE TABLE raw_vahan_snapshot (
    id                 BIGSERIAL    PRIMARY KEY,
    run_id             INT          NOT NULL REFERENCES raw_extraction_log(run_id),
    data_period        VARCHAR(30)  NOT NULL,  -- e.g. '2026-03' or '2025-Q4'
    state_filter       VARCHAR(100),           -- 'ALL' for national
    vehicle_category   VARCHAR(50),
    vehicle_class      VARCHAR(200),
    fuel               VARCHAR(50),
    maker              VARCHAR(300),
    registration_count BIGINT       NOT NULL CHECK (registration_count >= 0),
    query_params       JSONB,                  -- Store exact query config for reproducibility
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rvs_run ON raw_vahan_snapshot (run_id);
CREATE INDEX idx_rvs_period ON raw_vahan_snapshot (data_period);
CREATE INDEX idx_rvs_maker ON raw_vahan_snapshot (maker);

-- ---------------------------------------------------------------------------
-- raw_fada_monthly: Parsed FADA monthly press release data
-- ---------------------------------------------------------------------------
CREATE TABLE raw_fada_monthly (
    id                 SERIAL       PRIMARY KEY,
    run_id             INT          NOT NULL REFERENCES raw_extraction_log(run_id),
    report_month       DATE         NOT NULL,  -- First day of month
    category           VARCHAR(50)  NOT NULL,  -- 'PV', 'CV', '2W', '3W', etc.
    oem_name           VARCHAR(300) NOT NULL,
    volume_current     BIGINT,
    volume_prior_year  BIGINT,
    yoy_pct            DECIMAL(8,2),
    market_share_pct   DECIMAL(6,2),
    fuel_type          VARCHAR(50),
    source_file        VARCHAR(300),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rfm_month ON raw_fada_monthly (report_month);
CREATE INDEX idx_rfm_oem ON raw_fada_monthly (oem_name, report_month);

-- ---------------------------------------------------------------------------
-- raw_oem_wholesale: Monthly wholesale volumes from BSE filings
-- ---------------------------------------------------------------------------
CREATE TABLE raw_oem_wholesale (
    id                 SERIAL       PRIMARY KEY,
    run_id             INT          NOT NULL REFERENCES raw_extraction_log(run_id),
    report_month       DATE         NOT NULL,
    oem_name           VARCHAR(300) NOT NULL,
    segment            VARCHAR(50),
    domestic_volume    BIGINT,
    export_volume      BIGINT,
    total_volume       BIGINT,
    source_url         VARCHAR(500),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_row_month ON raw_oem_wholesale (report_month);
CREATE INDEX idx_row_oem ON raw_oem_wholesale (oem_name, report_month);

-- ============================================================================
-- SILVER TABLES (Normalized / Transformed)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fact_daily_registrations: Core fact table — daily volumes by OEM/segment/fuel
-- ---------------------------------------------------------------------------
CREATE TABLE fact_daily_registrations (
    id                 BIGSERIAL    PRIMARY KEY,
    data_date          DATE         NOT NULL,
    geo_id             INT          NOT NULL DEFAULT 1 REFERENCES dim_geo(geo_id),
    oem_id             INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id         INT          NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id            INT          NOT NULL REFERENCES dim_fuel(fuel_id),
    registrations      BIGINT       NOT NULL CHECK (registrations >= 0),
    source             VARCHAR(20)  NOT NULL DEFAULT 'VAHAN',
    run_id             INT          REFERENCES raw_extraction_log(run_id),
    revision_num       INT          NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Primary query indexes
CREATE INDEX idx_fdr_date ON fact_daily_registrations (data_date);
CREATE INDEX idx_fdr_oem_date ON fact_daily_registrations (oem_id, data_date);
CREATE INDEX idx_fdr_segment_date ON fact_daily_registrations (segment_id, data_date);
CREATE INDEX idx_fdr_fuel_date ON fact_daily_registrations (fuel_id, data_date);
CREATE INDEX idx_fdr_composite ON fact_daily_registrations (data_date, oem_id, segment_id, fuel_id);

-- For revision tracking
CREATE INDEX idx_fdr_revision ON fact_daily_registrations (data_date, oem_id, segment_id, fuel_id, revision_num DESC);

-- ---------------------------------------------------------------------------
-- fact_monthly_registrations: Aggregated monthly — locked after reconciliation
-- ---------------------------------------------------------------------------
CREATE TABLE fact_monthly_registrations (
    id                 SERIAL       PRIMARY KEY,
    month_date         DATE         NOT NULL,  -- First day of month
    oem_id             INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id         INT          NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id            INT          NOT NULL REFERENCES dim_fuel(fuel_id),
    registrations      BIGINT       NOT NULL CHECK (registrations >= 0),
    source             VARCHAR(20)  NOT NULL,  -- 'VAHAN_DAILY_AGG', 'FADA', 'SIAM_HISTORICAL'
    is_locked          BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (month_date, oem_id, segment_id, fuel_id, source)
);

CREATE INDEX idx_fmr_month ON fact_monthly_registrations (month_date);
CREATE INDEX idx_fmr_oem ON fact_monthly_registrations (oem_id, month_date);

-- ---------------------------------------------------------------------------
-- fact_monthly_wholesale: OEM wholesale (factory dispatch) volumes
-- ---------------------------------------------------------------------------
CREATE TABLE fact_monthly_wholesale (
    id                 SERIAL       PRIMARY KEY,
    month_date         DATE         NOT NULL,
    oem_id             INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id         INT          NOT NULL REFERENCES dim_segment(segment_id),
    domestic_volume    BIGINT,
    export_volume      BIGINT,
    total_volume       BIGINT,
    source             VARCHAR(20)  NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (month_date, oem_id, segment_id, source)
);

CREATE INDEX idx_fmw_oem_month ON fact_monthly_wholesale (oem_id, month_date);

-- ============================================================================
-- GOLD TABLES (Analytics + Financial Proxy)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fact_asp_master: Segment-level ASP assumptions (versioned by effective_from)
-- ---------------------------------------------------------------------------
CREATE TABLE fact_asp_master (
    id                 SERIAL        PRIMARY KEY,
    segment_id         INT           NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id            INT           NOT NULL REFERENCES dim_fuel(fuel_id),
    effective_from     DATE          NOT NULL,
    effective_to       DATE,                    -- NULL = currently active
    asp_ex_factory_rupees DECIMAL(14,2) NOT NULL CHECK (asp_ex_factory_rupees > 0),
    asp_source         VARCHAR(30)   NOT NULL
                       CHECK (asp_source IN ('BACKCALC', 'ESTIMATED', 'ANNOUNCED', 'INDUSTRY_AVG')),
    confidence         VARCHAR(10)   NOT NULL
                       CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    notes              TEXT
);

CREATE INDEX idx_fam_segment_fuel ON fact_asp_master (segment_id, fuel_id, effective_from DESC);

-- Constraint: no overlapping date ranges for same segment+fuel
CREATE UNIQUE INDEX idx_fam_no_overlap ON fact_asp_master (segment_id, fuel_id, effective_from);

-- ---------------------------------------------------------------------------
-- est_quarterly_revenue: Demand-based implied revenue proxy
-- ---------------------------------------------------------------------------
CREATE TABLE est_quarterly_revenue (
    id                     SERIAL        PRIMARY KEY,
    oem_id                 INT           NOT NULL REFERENCES dim_oem(oem_id),
    fy_quarter             VARCHAR(8)    NOT NULL,  -- e.g. 'Q3FY26'
    estimate_date          DATE          NOT NULL,  -- When this estimate was computed
    reg_volume             BIGINT,
    wholesale_volume       BIGINT,
    export_volume          BIGINT,
    est_domestic_rev_cr    DECIMAL(15,2),  -- In ₹ Crores
    est_total_rev_cr       DECIMAL(15,2),
    est_rev_low_cr         DECIMAL(15,2),  -- -5% sensitivity
    est_rev_high_cr        DECIMAL(15,2),  -- +5% sensitivity
    data_completeness_pct  DECIMAL(5,2),   -- % of quarter days with data
    created_at             TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_eqr_oem_quarter ON est_quarterly_revenue (oem_id, fy_quarter);
CREATE INDEX idx_eqr_quarter ON est_quarterly_revenue (fy_quarter, estimate_date DESC);

-- ============================================================================
-- MATERIALIZED VIEW
-- ============================================================================

-- ---------------------------------------------------------------------------
-- mv_oem_monthly_summary: Pre-aggregated monthly view for dashboard queries
-- Refreshed after each ETL run
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_oem_monthly_summary AS
SELECT
    d.calendar_year,
    d.calendar_month,
    d.fy_year,
    d.fy_quarter,
    o.oem_name,
    o.nse_ticker,
    o.is_listed,
    s.segment_code,
    f.dashboard_bucket AS powertrain,
    f.fuel_group,
    SUM(fdr.registrations) AS total_registrations
FROM fact_daily_registrations fdr
JOIN dim_date d        ON fdr.data_date = d.date_key
JOIN dim_oem o         ON fdr.oem_id = o.oem_id
JOIN dim_segment s     ON fdr.segment_id = s.segment_id
JOIN dim_fuel f        ON fdr.fuel_id = f.fuel_id
WHERE fdr.revision_num = (
    SELECT MAX(fdr2.revision_num)
    FROM fact_daily_registrations fdr2
    WHERE fdr2.data_date = fdr.data_date
      AND fdr2.oem_id = fdr.oem_id
      AND fdr2.segment_id = fdr.segment_id
      AND fdr2.fuel_id = fdr.fuel_id
)
GROUP BY
    d.calendar_year, d.calendar_month, d.fy_year, d.fy_quarter,
    o.oem_name, o.nse_ticker, o.is_listed,
    s.segment_code, f.dashboard_bucket, f.fuel_group;

-- Indexes on materialized view
CREATE UNIQUE INDEX idx_mv_oms_unique ON mv_oem_monthly_summary
    (calendar_year, calendar_month, oem_name, segment_code, powertrain, fuel_group);
CREATE INDEX idx_mv_oms_ticker ON mv_oem_monthly_summary (nse_ticker);
CREATE INDEX idx_mv_oms_fy ON mv_oem_monthly_summary (fy_year, fy_quarter);
CREATE INDEX idx_mv_oms_segment ON mv_oem_monthly_summary (segment_code, powertrain);

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to refresh the materialized view (called after ETL runs)
CREATE OR REPLACE FUNCTION refresh_oem_monthly_summary()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_oem_monthly_summary;
END;
$$ LANGUAGE plpgsql;

-- Function to compute Indian Financial Year from a date
CREATE OR REPLACE FUNCTION get_fy_year(d DATE)
RETURNS VARCHAR(6) AS $$
BEGIN
    IF EXTRACT(MONTH FROM d) >= 4 THEN
        RETURN 'FY' || (EXTRACT(YEAR FROM d) + 1)::TEXT % 100;
    ELSE
        RETURN 'FY' || EXTRACT(YEAR FROM d)::TEXT % 100;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to compute FY quarter from a date
CREATE OR REPLACE FUNCTION get_fy_quarter(d DATE)
RETURNS VARCHAR(8) AS $$
DECLARE
    m INT := EXTRACT(MONTH FROM d)::INT;
    q INT;
BEGIN
    q := CASE
        WHEN m IN (4,5,6)   THEN 1
        WHEN m IN (7,8,9)   THEN 2
        WHEN m IN (10,11,12) THEN 3
        ELSE 4
    END;
    RETURN 'Q' || q || get_fy_year(d);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- UNMAPPED ENTITY STAGING (for alert pipeline)
-- ============================================================================

CREATE TABLE staging_unmapped_makers (
    id                 SERIAL       PRIMARY KEY,
    source             VARCHAR(20)  NOT NULL,
    raw_maker_name     VARCHAR(300) NOT NULL,
    first_seen_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_seen_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    occurrence_count   INT          NOT NULL DEFAULT 1,
    registration_volume BIGINT      DEFAULT 0,  -- Total volume under this name
    resolved           BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_oem_id    INT          REFERENCES dim_oem(oem_id),
    resolved_at        TIMESTAMPTZ,
    UNIQUE (source, raw_maker_name)
);

CREATE TABLE staging_unmapped_fuels (
    id                 SERIAL       PRIMARY KEY,
    source             VARCHAR(20)  NOT NULL,
    raw_fuel_name      VARCHAR(100) NOT NULL,
    first_seen_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_seen_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    occurrence_count   INT          NOT NULL DEFAULT 1,
    resolved           BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_fuel_id   INT          REFERENCES dim_fuel(fuel_id),
    resolved_at        TIMESTAMPTZ,
    UNIQUE (source, raw_fuel_name)
);

CREATE TABLE staging_unmapped_vehicle_classes (
    id                 SERIAL       PRIMARY KEY,
    source             VARCHAR(20)  NOT NULL,
    raw_class_name     VARCHAR(300) NOT NULL,
    first_seen_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_seen_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    occurrence_count   INT          NOT NULL DEFAULT 1,
    resolved           BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_map_id    INT          REFERENCES dim_vehicle_class_map(map_id),
    resolved_at        TIMESTAMPTZ,
    UNIQUE (source, raw_class_name)
);

-- ============================================================================
-- ROW-LEVEL SECURITY (Supabase)
-- ============================================================================
-- Enable RLS on all tables for Supabase. ETL writes via service_role key.
-- Dashboard reads via anon key with read-only policies.

ALTER TABLE dim_date ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_oem ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_oem_alias ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_segment ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_vehicle_class_map ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_fuel ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_geo ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_extraction_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_vahan_snapshot ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_fada_monthly ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_oem_wholesale ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_daily_registrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_monthly_registrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_monthly_wholesale ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_asp_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE est_quarterly_revenue ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging_unmapped_makers ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging_unmapped_fuels ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging_unmapped_vehicle_classes ENABLE ROW LEVEL SECURITY;

-- Anon (dashboard) read-only policies
CREATE POLICY "anon_read_dim_date" ON dim_date FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_dim_oem" ON dim_oem FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_dim_oem_alias" ON dim_oem_alias FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_dim_segment" ON dim_segment FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_dim_vehicle_class_map" ON dim_vehicle_class_map FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_dim_fuel" ON dim_fuel FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_dim_geo" ON dim_geo FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_fdr" ON fact_daily_registrations FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_fmr" ON fact_monthly_registrations FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_fmw" ON fact_monthly_wholesale FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_fam" ON fact_asp_master FOR SELECT USING (TRUE);
CREATE POLICY "anon_read_eqr" ON est_quarterly_revenue FOR SELECT USING (TRUE);

-- Service role (ETL) full access — Supabase service_role bypasses RLS by default

-- ============================================================================
-- END OF DDL
-- ============================================================================
