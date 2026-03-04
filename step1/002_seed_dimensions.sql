-- ============================================================================
-- AutoQuant: Dimension Table Seed Data
-- UPDATED with user-confirmed decisions:
--   1) Ampere → Others/Unlisted (not Greaves)
--   2) VIDA → Hero MotoCorp
--   3) Tata → Split into PV entity + CV entity (separate tracking)
--   4) Mahindra Last Mile Mobility alias removed (3W excluded)
--   5) BYD India PV → Separate unlisted OEM entry
--   6) Sub-segments fine for V1 (no model-level mapping)
-- ============================================================================
SET search_path TO autoquant, public;

-- ============================================================================
-- 1. dim_date: Generate 2016-01-01 through 2027-12-31
-- ============================================================================
INSERT INTO dim_date (date_key, calendar_year, calendar_month, calendar_quarter,
                      fy_year, fy_quarter, fy_quarter_num, month_name, day_of_week, is_weekend)
SELECT
    d::DATE AS date_key,
    EXTRACT(YEAR FROM d)::SMALLINT AS calendar_year,
    EXTRACT(MONTH FROM d)::SMALLINT AS calendar_month,
    EXTRACT(QUARTER FROM d)::SMALLINT AS calendar_quarter,
    CASE
        WHEN EXTRACT(MONTH FROM d) >= 4
        THEN 'FY' || LPAD(((EXTRACT(YEAR FROM d)::INT + 1) % 100)::TEXT, 2, '0')
        ELSE 'FY' || LPAD((EXTRACT(YEAR FROM d)::INT % 100)::TEXT, 2, '0')
    END AS fy_year,
    'Q' ||
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (4,5,6)    THEN '1'
        WHEN EXTRACT(MONTH FROM d) IN (7,8,9)    THEN '2'
        WHEN EXTRACT(MONTH FROM d) IN (10,11,12) THEN '3'
        ELSE '4'
    END ||
    CASE
        WHEN EXTRACT(MONTH FROM d) >= 4
        THEN 'FY' || LPAD(((EXTRACT(YEAR FROM d)::INT + 1) % 100)::TEXT, 2, '0')
        ELSE 'FY' || LPAD((EXTRACT(YEAR FROM d)::INT % 100)::TEXT, 2, '0')
    END AS fy_quarter,
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (4,5,6)    THEN 1
        WHEN EXTRACT(MONTH FROM d) IN (7,8,9)    THEN 2
        WHEN EXTRACT(MONTH FROM d) IN (10,11,12) THEN 3
        ELSE 4
    END::SMALLINT AS fy_quarter_num,
    TO_CHAR(d, 'Month')::VARCHAR(9) AS month_name,
    EXTRACT(ISODOW FROM d)::SMALLINT - 1 AS day_of_week,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2016-01-01'::DATE, '2027-12-31'::DATE, '1 day') AS d;

-- ============================================================================
-- 2. dim_segment: 3 top-level segments + sub-segments for future drill-down
-- ============================================================================
INSERT INTO dim_segment (segment_code, segment_name, sub_segment) VALUES
    ('PV', 'Passenger Vehicles',  NULL),
    ('PV', 'Passenger Vehicles',  'Hatchback'),
    ('PV', 'Passenger Vehicles',  'Sedan'),
    ('PV', 'Passenger Vehicles',  'UV/SUV'),
    ('PV', 'Passenger Vehicles',  'Van'),
    ('PV', 'Passenger Vehicles',  'MPV'),
    ('CV', 'Commercial Vehicles', NULL),
    ('CV', 'Commercial Vehicles', 'LCV'),
    ('CV', 'Commercial Vehicles', 'MHCV'),
    ('CV', 'Commercial Vehicles', 'Bus'),
    ('CV', 'Commercial Vehicles', 'SCV'),
    ('2W', 'Two-Wheelers',        NULL),
    ('2W', 'Two-Wheelers',        'Motorcycle'),
    ('2W', 'Two-Wheelers',        'Scooter'),
    ('2W', 'Two-Wheelers',        'Moped');

-- ============================================================================
-- 3. dim_fuel: All VAHAN fuel types → powertrain/bucket/group mapping
-- ============================================================================
INSERT INTO dim_fuel (fuel_code, powertrain, dashboard_bucket, fuel_group) VALUES
    ('ELECTRIC(BOV)',           'EV',     'EV',  'Electric'),
    ('FUEL CELL HYDROGEN',     'EV',     'EV',  'Electric'),
    ('SOLAR',                  'EV',     'EV',  'Electric'),
    ('PETROL',                 'ICE',    'ICE', 'Petrol'),
    ('ETHANOL',                'ICE',    'ICE', 'Petrol'),
    ('PETROL/ETHANOL',         'ICE',    'ICE', 'Petrol'),
    ('DIESEL',                 'ICE',    'ICE', 'Diesel'),
    ('CNG ONLY',               'ICE',    'ICE', 'CNG'),
    ('PETROL/CNG',             'ICE',    'ICE', 'CNG'),
    ('DUAL DIESEL/CNG',        'ICE',    'ICE', 'CNG'),
    ('DUAL DIESEL/BIO CNG',    'ICE',    'ICE', 'CNG'),
    ('LPG ONLY',               'ICE',    'ICE', 'CNG/LPG'),
    ('LNG',                    'ICE',    'ICE', 'CNG/LPG'),
    ('PETROL/LPG',             'ICE',    'ICE', 'CNG/LPG'),
    ('DUAL DIESEL/LNG',        'ICE',    'ICE', 'CNG/LPG'),
    ('PETROL/HYBRID',          'HYBRID', 'ICE', 'Hybrid'),
    ('DIESEL/HYBRID',          'HYBRID', 'ICE', 'Hybrid'),
    ('METHANOL',               'ICE',    'ICE', 'Other'),
    ('DI-METHYL ETHER',        'ICE',    'ICE', 'Other'),
    ('PETROL/METHANOL',        'ICE',    'ICE', 'Other'),
    ('NOT APPLICABLE',         'ICE',    'ICE', 'Other'),
    ('NOT AVAILABLE',          'ICE',    'ICE', 'Other');

-- ============================================================================
-- 4. dim_vehicle_class_map: VAHAN vehicle class → segment
-- ============================================================================
DO $$
DECLARE
    pv_id INT;
    cv_id INT;
    tw_id INT;
BEGIN
    SELECT segment_id INTO pv_id FROM dim_segment WHERE segment_code = 'PV' AND sub_segment IS NULL;
    SELECT segment_id INTO cv_id FROM dim_segment WHERE segment_code = 'CV' AND sub_segment IS NULL;
    SELECT segment_id INTO tw_id FROM dim_segment WHERE segment_code = '2W' AND sub_segment IS NULL;

    -- PASSENGER VEHICLES (PV)
    INSERT INTO dim_vehicle_class_map (vahan_class_name, segment_id, is_excluded, notes) VALUES
        ('MOTOR CAR',                                    pv_id, FALSE, NULL),
        ('MOTOR CAB',                                    pv_id, FALSE, 'Includes Ola/Uber fleet registrations'),
        ('OMNIBUS (PRIVATE USE)',                         pv_id, FALSE, 'Private use omnibus'),
        ('PRIVATE SERVICE VEHICLE (INDIVIDUAL USE)',      pv_id, FALSE, NULL),
        ('QUADRICYCLE (PRIVATE)',                         pv_id, FALSE, NULL),
        ('MOTOR CARAVAN',                                pv_id, FALSE, NULL),
        ('CAMPER VAN / TRAILER (PRIVATE USE)',            pv_id, FALSE, NULL);

    -- COMMERCIAL VEHICLES (CV)
    INSERT INTO dim_vehicle_class_map (vahan_class_name, segment_id, is_excluded, notes) VALUES
        ('GOODS CARRIER',                                cv_id, FALSE, 'LCV + MHCV goods transport'),
        ('BUS',                                          cv_id, FALSE, 'Stage carriage / contract buses'),
        ('OMNIBUS',                                      cv_id, FALSE, 'Commercial omnibus'),
        ('EDUCATIONAL INSTITUTION BUS',                  cv_id, FALSE, 'School/college buses'),
        ('PRIVATE SERVICE VEHICLE',                      cv_id, FALSE, 'Company/staff transport'),
        ('LUXURY CAB',                                   cv_id, FALSE, NULL),
        ('MAXI CAB',                                     cv_id, FALSE, NULL),
        ('CASH VAN',                                     cv_id, FALSE, NULL),
        ('BREAKDOWN VAN',                                cv_id, FALSE, NULL),
        ('AMBULANCE',                                    cv_id, FALSE, NULL),
        ('RECOVERY VEHICLE',                             cv_id, FALSE, NULL),
        ('SEMI-TRAILER (COMMERCIAL)',                    cv_id, FALSE, NULL),
        ('TRAILER (COMMERCIAL)',                         cv_id, FALSE, NULL),
        ('ARTICULATED VEHICLE',                          cv_id, FALSE, NULL),
        ('DUMPER',                                       cv_id, FALSE, NULL),
        ('MODULAR HYDRAULIC TRAILER',                    cv_id, FALSE, NULL);

    -- TWO-WHEELERS (2W)
    INSERT INTO dim_vehicle_class_map (vahan_class_name, segment_id, is_excluded, notes) VALUES
        ('M-CYCLE/SCOOTER',                              tw_id, FALSE, 'Primary 2W class'),
        ('M-CYCLE/SCOOTER-WITH SIDE CAR',                tw_id, FALSE, NULL),
        ('MOPED',                                        tw_id, FALSE, NULL),
        ('MOTORISED CYCLE (CC > 25CC)',                  tw_id, FALSE, NULL),
        ('MOTOR CYCLE/SCOOTER-USED FOR HIRE',            tw_id, FALSE, NULL),
        ('MOTOR CYCLE/SCOOTER-WITH TRAILER',             tw_id, FALSE, NULL),
        ('M-CYCLE/SCOOTER-SIDECAR(T)',                   tw_id, FALSE, NULL);

    -- EXCLUDED CLASSES
    INSERT INTO dim_vehicle_class_map (vahan_class_name, segment_id, is_excluded, notes) VALUES
        ('THREE WHEELER (GOODS)',                        NULL, TRUE, 'Excluded: 3W'),
        ('THREE WHEELER (PASSENGER)',                    NULL, TRUE, 'Excluded: 3W'),
        ('THREE WHEELER (PERSONAL)',                     NULL, TRUE, 'Excluded: 3W'),
        ('E-RICKSHAW(P)',                                NULL, TRUE, 'Excluded: E-Rickshaw'),
        ('E-RICKSHAW WITH CART (G)',                     NULL, TRUE, 'Excluded: E-Rickshaw'),
        ('AGRICULTURAL TRACTOR',                         NULL, TRUE, 'Excluded: Agri'),
        ('TRACTOR (COMMERCIAL)',                         NULL, TRUE, 'Excluded: Agri/Commercial'),
        ('TRACTOR-TROLLEY(COMMERCIAL)',                  NULL, TRUE, 'Excluded: Agri/Commercial'),
        ('TRAILER (AGRICULTURAL)',                       NULL, TRUE, 'Excluded: Agri'),
        ('CONSTRUCTION EQUIPMENT VEHICLE',               NULL, TRUE, 'Excluded: Construction'),
        ('CONSTRUCTION EQUIPMENT VEHICLE (COMMERCIAL)',  NULL, TRUE, 'Excluded: Construction'),
        ('POWER TILLER',                                 NULL, TRUE, 'Excluded: Agri'),
        ('POWER TILLER (COMMERCIAL)',                    NULL, TRUE, 'Excluded: Agri'),
        ('FORK LIFT',                                    NULL, TRUE, 'Excluded: Industrial'),
        ('CRANE MOUNTED VEHICLE',                        NULL, TRUE, 'Excluded: Industrial'),
        ('EXCAVATOR',                                    NULL, TRUE, 'Excluded: Construction'),
        ('BULLDOZER',                                    NULL, TRUE, 'Excluded: Construction'),
        ('ROAD ROLLER',                                  NULL, TRUE, 'Excluded: Construction'),
        ('HARVESTER',                                    NULL, TRUE, 'Excluded: Agri'),
        ('EARTH MOVING EQUIPMENT',                       NULL, TRUE, 'Excluded: Construction'),
        ('ADAPTED VEHICLE',                              NULL, TRUE, 'Excluded: Special'),
        ('ARMOURED/SPECIALISED VEHICLE',                 NULL, TRUE, 'Excluded: Special'),
        ('VINTAGE MOTOR VEHICLE',                        NULL, TRUE, 'Excluded: Special');
END $$;

-- ============================================================================
-- 5. dim_oem: Listed OEMs + BYD (unlisted, tracked separately) + Others/Unlisted
-- CHANGE: Tata split into PV + CV entities. BYD India added as separate entity.
-- ============================================================================
INSERT INTO dim_oem (oem_name, nse_ticker, bse_code, is_listed, is_in_scope, primary_segments) VALUES
    ('Maruti Suzuki India Ltd',           'MARUTI',      '532500', TRUE,  TRUE,  ARRAY['PV']),
    -- TATA: Split into PV and CV per user decision (post Jan-2025 demerger)
    ('Tata Motors Ltd (PV)',              'TATAMOTORS',  '500570', TRUE,  TRUE,  ARRAY['PV']),
    ('Tata Motors Ltd (CV)',              'TATAMOTORS',  '500570', TRUE,  TRUE,  ARRAY['CV']),
    ('Mahindra & Mahindra Ltd',           'M&M',         '500520', TRUE,  TRUE,  ARRAY['PV','CV']),
    ('Hyundai Motor India Ltd',           'HYUNDAI',     '544274', TRUE,  TRUE,  ARRAY['PV']),
    ('Bajaj Auto Ltd',                    'BAJAJ-AUTO',  '532977', TRUE,  TRUE,  ARRAY['2W']),
    ('Hero MotoCorp Ltd',                 'HEROMOTOCO',  '500182', TRUE,  TRUE,  ARRAY['2W']),
    ('TVS Motor Company Ltd',             'TVSMOTOR',    '532343', TRUE,  TRUE,  ARRAY['2W']),
    ('Eicher Motors Ltd',                 'EICHERMOT',   '505200', TRUE,  TRUE,  ARRAY['2W','CV']),
    ('Ashok Leyland Ltd',                 'ASHOKLEY',    '500477', TRUE,  TRUE,  ARRAY['CV']),
    ('Ola Electric Technologies Ltd',     'OLAELEC',     '544192', TRUE,  TRUE,  ARRAY['2W']),
    ('Ather Energy Ltd',                  'ATHER',       '544296', TRUE,  TRUE,  ARRAY['2W']),
    ('Force Motors Ltd',                  'FORCEMOT',    '500033', TRUE,  TRUE,  ARRAY['PV','CV']),
    ('SML Isuzu Ltd',                     'SMLISUZU',    '533419', TRUE,  TRUE,  ARRAY['CV']),
    ('Olectra Greentech Ltd',             'OLECTRA',     '532439', TRUE,  TRUE,  ARRAY['CV']),
    -- BYD India: Unlisted but tracked separately for PV EV visibility
    ('BYD India Pvt Ltd',                  NULL,          NULL,    FALSE, TRUE,  ARRAY['PV']),
    -- Catch-all for all other unlisted makers
    ('Others/Unlisted',                    NULL,          NULL,    FALSE, TRUE,  ARRAY['PV','CV','2W']);

-- ============================================================================
-- 6. dim_oem_alias: Source-specific maker name → OEM mapping
-- CHANGES:
--   - Tata PV aliases → 'Tata Motors Ltd (PV)'
--   - Tata CV aliases → 'Tata Motors Ltd (CV)'
--   - VIDA aliases → Hero MotoCorp
--   - Mahindra Last Mile Mobility → REMOVED (3W volumes excluded by vehicle class filter)
--   - BYD India PV aliases → 'BYD India Pvt Ltd'
--   - BYD Olectra (buses) stays with Olectra
-- ============================================================================
DO $$
DECLARE
    maruti_id    INT;
    tata_pv_id   INT;
    tata_cv_id   INT;
    mm_id        INT;
    hyundai_id   INT;
    bajaj_id     INT;
    hero_id      INT;
    tvs_id       INT;
    eicher_id    INT;
    ashok_id     INT;
    ola_id       INT;
    ather_id     INT;
    force_id     INT;
    sml_id       INT;
    olectra_id   INT;
    byd_id       INT;
    others_id    INT;
BEGIN
    SELECT oem_id INTO maruti_id   FROM dim_oem WHERE nse_ticker = 'MARUTI';
    SELECT oem_id INTO tata_pv_id  FROM dim_oem WHERE oem_name = 'Tata Motors Ltd (PV)';
    SELECT oem_id INTO tata_cv_id  FROM dim_oem WHERE oem_name = 'Tata Motors Ltd (CV)';
    SELECT oem_id INTO mm_id       FROM dim_oem WHERE nse_ticker = 'M&M';
    SELECT oem_id INTO hyundai_id  FROM dim_oem WHERE nse_ticker = 'HYUNDAI';
    SELECT oem_id INTO bajaj_id    FROM dim_oem WHERE nse_ticker = 'BAJAJ-AUTO';
    SELECT oem_id INTO hero_id     FROM dim_oem WHERE nse_ticker = 'HEROMOTOCO';
    SELECT oem_id INTO tvs_id      FROM dim_oem WHERE nse_ticker = 'TVSMOTOR';
    SELECT oem_id INTO eicher_id   FROM dim_oem WHERE nse_ticker = 'EICHERMOT';
    SELECT oem_id INTO ashok_id    FROM dim_oem WHERE nse_ticker = 'ASHOKLEY';
    SELECT oem_id INTO ola_id      FROM dim_oem WHERE nse_ticker = 'OLAELEC';
    SELECT oem_id INTO ather_id    FROM dim_oem WHERE nse_ticker = 'ATHER';
    SELECT oem_id INTO force_id    FROM dim_oem WHERE nse_ticker = 'FORCEMOT';
    SELECT oem_id INTO sml_id      FROM dim_oem WHERE nse_ticker = 'SMLISUZU';
    SELECT oem_id INTO olectra_id  FROM dim_oem WHERE nse_ticker = 'OLECTRA';
    SELECT oem_id INTO byd_id      FROM dim_oem WHERE oem_name = 'BYD India Pvt Ltd';
    SELECT oem_id INTO others_id   FROM dim_oem WHERE oem_name = 'Others/Unlisted';

    INSERT INTO dim_oem_alias (oem_id, source, alias_name, is_active) VALUES

    -- ── MARUTI SUZUKI ──
    (maruti_id,   'VAHAN', 'MARUTI SUZUKI INDIA LTD', TRUE),
    (maruti_id,   'VAHAN', 'MARUTI SUZUKI INDIA LIMITED', TRUE),
    (maruti_id,   'VAHAN', 'MARUTI UDYOG LTD', TRUE),
    (maruti_id,   'FADA',  'MARUTI SUZUKI', TRUE),
    (maruti_id,   'FADA',  'MARUTI SUZUKI INDIA LTD', TRUE),
    (maruti_id,   'BSE',   'MARUTI SUZUKI INDIA LTD', TRUE),

    -- ── TATA MOTORS PV (Passenger Vehicles entity) ──
    -- Post-demerger: PV aliases route to PV entity
    (tata_pv_id,  'VAHAN', 'TATA MOTORS PASSENGER VEHICLES LTD', TRUE),
    (tata_pv_id,  'VAHAN', 'TATA MOTORS PASSENGER VEHICLES LIMITED', TRUE),
    (tata_pv_id,  'FADA',  'TATA MOTORS PASSENGER VEHICLES LTD', TRUE),
    -- NOTE: 'TATA MOTORS LTD' on VAHAN may produce BOTH PV and CV.
    -- ETL transform must split by vehicle_class → segment to route correctly.
    -- These "ambiguous" aliases are tagged to PV entity but the transform
    -- layer uses segment_code from vehicle_class_map to override:
    --   If vehicle_class → PV → tata_pv_id
    --   If vehicle_class → CV → tata_cv_id
    (tata_pv_id,  'VAHAN', 'TATA MOTORS LTD', TRUE),
    (tata_pv_id,  'VAHAN', 'TATA MOTORS LIMITED', TRUE),

    -- ── TATA MOTORS CV (Commercial Vehicles entity) ──
    (tata_cv_id,  'VAHAN', 'TATA MARCOPOLO MOTORS LTD', TRUE),
    (tata_cv_id,  'FADA',  'TATA MOTORS', TRUE),
    (tata_cv_id,  'BSE',   'TATA MOTORS LTD', TRUE),
    -- NOTE: Transform layer handles the Tata split. When maker='TATA MOTORS LTD'
    -- and vehicle_class maps to CV → route to tata_cv_id.
    -- See ETL transform: resolve_tata_split()

    -- ── MAHINDRA & MAHINDRA ──
    -- REMOVED: 'MAHINDRA LAST MILE MOBILITY LTD' (3W/e-rickshaw — excluded by vehicle class)
    (mm_id,       'VAHAN', 'MAHINDRA & MAHINDRA LIMITED', TRUE),
    (mm_id,       'VAHAN', 'MAHINDRA & MAHINDRA LTD', TRUE),
    (mm_id,       'VAHAN', 'MAHINDRA VEHICLE MANUFACTURERS LTD', TRUE),
    (mm_id,       'VAHAN', 'MAHINDRA ELECTRIC MOBILITY LTD', TRUE),
    (mm_id,       'FADA',  'MAHINDRA & MAHINDRA', TRUE),
    (mm_id,       'FADA',  'M&M', TRUE),
    (mm_id,       'BSE',   'MAHINDRA & MAHINDRA LTD', TRUE),

    -- ── HYUNDAI ──
    (hyundai_id,  'VAHAN', 'HYUNDAI MOTOR INDIA LTD', TRUE),
    (hyundai_id,  'VAHAN', 'HYUNDAI MOTOR INDIA LIMITED', TRUE),
    (hyundai_id,  'FADA',  'HYUNDAI', TRUE),
    (hyundai_id,  'FADA',  'HYUNDAI MOTOR INDIA', TRUE),
    (hyundai_id,  'BSE',   'HYUNDAI MOTOR INDIA LTD', TRUE),

    -- ── BAJAJ AUTO ──
    (bajaj_id,    'VAHAN', 'BAJAJ AUTO LTD', TRUE),
    (bajaj_id,    'VAHAN', 'BAJAJ AUTO LIMITED', TRUE),
    (bajaj_id,    'VAHAN', 'CHETAK TECHNOLOGY LIMITED', TRUE),
    (bajaj_id,    'VAHAN', 'CHETAK TECHNOLOGY LTD', TRUE),
    (bajaj_id,    'FADA',  'BAJAJ AUTO', TRUE),
    (bajaj_id,    'BSE',   'BAJAJ AUTO LTD', TRUE),

    -- ── HERO MOTOCORP ──
    -- CHANGE: VIDA aliases now route HERE (Hero subsidiary)
    (hero_id,     'VAHAN', 'HERO MOTOCORP LTD', TRUE),
    (hero_id,     'VAHAN', 'HERO MOTOCORP LIMITED', TRUE),
    (hero_id,     'VAHAN', 'HERO ELECTRIC VEHICLES PVT LTD', TRUE),
    (hero_id,     'VAHAN', 'VIDA (HERO)', TRUE),
    (hero_id,     'VAHAN', 'VIDA', TRUE),
    (hero_id,     'FADA',  'HERO MOTOCORP', TRUE),
    (hero_id,     'BSE',   'HERO MOTOCORP LTD', TRUE),

    -- ── TVS MOTOR ──
    (tvs_id,      'VAHAN', 'TVS MOTOR COMPANY LTD', TRUE),
    (tvs_id,      'VAHAN', 'TVS MOTOR COMPANY LIMITED', TRUE),
    (tvs_id,      'FADA',  'TVS MOTOR', TRUE),
    (tvs_id,      'FADA',  'TVS MOTOR COMPANY', TRUE),
    (tvs_id,      'BSE',   'TVS MOTOR COMPANY LTD', TRUE),

    -- ── EICHER MOTORS (Royal Enfield 2W + VECV CV) ──
    (eicher_id,   'VAHAN', 'ROYAL-ENFIELD (UNIT OF EICHER LTD)', TRUE),
    (eicher_id,   'VAHAN', 'ROYAL ENFIELD', TRUE),
    (eicher_id,   'VAHAN', 'VE COMMERCIAL VEHICLES LTD', TRUE),
    (eicher_id,   'VAHAN', 'VE COMMERCIAL VEHICLES LTD (VOLVO BUSES DIVISION)', TRUE),
    (eicher_id,   'VAHAN', 'VE COMMERCIAL VEHICLES LIMITED', TRUE),
    (eicher_id,   'FADA',  'ROYAL ENFIELD', TRUE),
    (eicher_id,   'FADA',  'VECV', TRUE),
    (eicher_id,   'FADA',  'EICHER', TRUE),
    (eicher_id,   'BSE',   'EICHER MOTORS LTD', TRUE),

    -- ── ASHOK LEYLAND ──
    (ashok_id,    'VAHAN', 'ASHOK LEYLAND LTD', TRUE),
    (ashok_id,    'VAHAN', 'ASHOK LEYLAND LIMITED', TRUE),
    (ashok_id,    'VAHAN', 'SWITCH MOBILITY AUTOMOTIVE LTD', TRUE),
    (ashok_id,    'VAHAN', 'SWITCH MOBILITY AUTOMOTIVE LIMITED', TRUE),
    (ashok_id,    'FADA',  'ASHOK LEYLAND', TRUE),
    (ashok_id,    'BSE',   'ASHOK LEYLAND LTD', TRUE),

    -- ── OLA ELECTRIC ──
    (ola_id,      'VAHAN', 'OLA ELECTRIC TECHNOLOGIES PVT LTD', TRUE),
    (ola_id,      'VAHAN', 'OLA ELECTRIC TECHNOLOGIES PRIVATE LIMITED', TRUE),
    (ola_id,      'VAHAN', 'OLA ELECTRIC TECHNOLOGIES LTD', TRUE),
    (ola_id,      'FADA',  'OLA ELECTRIC', TRUE),
    (ola_id,      'BSE',   'OLA ELECTRIC TECHNOLOGIES LTD', TRUE),

    -- ── ATHER ENERGY ──
    (ather_id,    'VAHAN', 'ATHER ENERGY LTD', TRUE),
    (ather_id,    'VAHAN', 'ATHER ENERGY PVT LTD', TRUE),
    (ather_id,    'VAHAN', 'ATHER ENERGY PRIVATE LIMITED', TRUE),
    (ather_id,    'FADA',  'ATHER ENERGY', TRUE),
    (ather_id,    'BSE',   'ATHER ENERGY LTD', TRUE),

    -- ── FORCE MOTORS ──
    (force_id,    'VAHAN', 'FORCE MOTORS LIMITED', TRUE),
    (force_id,    'VAHAN', 'FORCE MOTORS LTD', TRUE),
    (force_id,    'FADA',  'FORCE MOTORS', TRUE),
    (force_id,    'BSE',   'FORCE MOTORS LTD', TRUE),

    -- ── SML ISUZU ──
    (sml_id,      'VAHAN', 'SML ISUZU LTD', TRUE),
    (sml_id,      'VAHAN', 'SML ISUZU LIMITED', TRUE),
    (sml_id,      'FADA',  'SML ISUZU', TRUE),
    (sml_id,      'BSE',   'SML ISUZU LTD', TRUE),

    -- ── OLECTRA GREENTECH (EV buses) ──
    (olectra_id,  'VAHAN', 'OLECTRA GREENTECH LTD', TRUE),
    (olectra_id,  'VAHAN', 'OLECTRA GREENTECH LIMITED', TRUE),
    (olectra_id,  'VAHAN', 'BYD OLECTRA MOBILITY PVT LTD', TRUE),
    (olectra_id,  'FADA',  'OLECTRA', TRUE),
    (olectra_id,  'BSE',   'OLECTRA GREENTECH LTD', TRUE),

    -- ── BYD INDIA (PV — tracked separately, unlisted) ──
    (byd_id,      'VAHAN', 'BYD INDIA PVT LTD', TRUE),
    (byd_id,      'VAHAN', 'BYD INDIA PRIVATE LIMITED', TRUE),
    (byd_id,      'VAHAN', 'BYD AUTOMOBILES INDIA PVT LTD', TRUE),
    (byd_id,      'FADA',  'BYD', TRUE),
    (byd_id,      'FADA',  'BYD INDIA', TRUE),

    -- ── OTHERS/UNLISTED ──
    (others_id,   'VAHAN', 'HONDA MOTORCYCLE AND SCOOTER INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'HONDA MOTORCYCLE & SCOOTER INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'SUZUKI MOTORCYCLE INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'SUZUKI MOTORCYCLE INDIA PRIVATE LIMITED', TRUE),
    (others_id,   'VAHAN', 'INDIA YAMAHA MOTOR PVT LTD', TRUE),
    (others_id,   'VAHAN', 'INDIA YAMAHA MOTOR PRIVATE LIMITED', TRUE),
    (others_id,   'VAHAN', 'TOYOTA KIRLOSKAR MOTOR PVT LTD', TRUE),
    (others_id,   'VAHAN', 'TOYOTA KIRLOSKAR MOTOR PRIVATE LIMITED', TRUE),
    (others_id,   'VAHAN', 'KIA INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'KIA INDIA PRIVATE LIMITED', TRUE),
    (others_id,   'VAHAN', 'HONDA CARS INDIA LTD', TRUE),
    (others_id,   'VAHAN', 'HONDA CARS INDIA LIMITED', TRUE),
    (others_id,   'VAHAN', 'DAIMLER INDIA COMMERCIAL VEHICLES PVT LTD', TRUE),
    (others_id,   'VAHAN', 'MERCEDES-BENZ INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'BMW INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'VOLKSWAGEN INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'SKODA AUTO INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'MG MOTOR INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'CITROEN INDIA', TRUE),
    (others_id,   'VAHAN', 'RENAULT INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'NISSAN MOTOR INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'ISUZU MOTORS INDIA PVT LTD', TRUE),
    (others_id,   'VAHAN', 'AMPERE VEHICLES PVT LTD', TRUE),
    (others_id,   'VAHAN', 'REVOLT MOTORS', TRUE),
    (others_id,   'VAHAN', 'OKINAWA AUTOTECH PVT LTD', TRUE),
    (others_id,   'VAHAN', 'BGAUSS', TRUE),
    (others_id,   'VAHAN', 'PURE EV', TRUE),
    (others_id,   'VAHAN', 'SIMPLE ENERGY', TRUE),
    (others_id,   'VAHAN', 'RIVER', TRUE),
    (others_id,   'VAHAN', 'JBM AUTO LTD', TRUE),
    (others_id,   'VAHAN', 'PMI ELECTRO MOBILITY SOLUTIONS PVT LTD', TRUE);

END $$;

-- ============================================================================
-- 7. dim_geo: National entry for V1
-- ============================================================================
INSERT INTO dim_geo (level, state_name, rto_code, rto_name, vahan4_active) VALUES
    ('NATIONAL', 'All India', NULL, NULL, TRUE);

-- ============================================================================
-- 8. fact_asp_master: Initial ASP assumptions (FY26 starting values)
-- ============================================================================
DO $$
DECLARE
    pv_seg  INT;
    cv_seg  INT;
    tw_seg  INT;
    petrol_id    INT;
    diesel_id    INT;
    cng_id       INT;
    hybrid_p_id  INT;
    ev_id        INT;
BEGIN
    SELECT segment_id INTO pv_seg FROM dim_segment WHERE segment_code = 'PV' AND sub_segment IS NULL;
    SELECT segment_id INTO cv_seg FROM dim_segment WHERE segment_code = 'CV' AND sub_segment IS NULL;
    SELECT segment_id INTO tw_seg FROM dim_segment WHERE segment_code = '2W' AND sub_segment IS NULL;

    SELECT fuel_id INTO petrol_id   FROM dim_fuel WHERE fuel_code = 'PETROL';
    SELECT fuel_id INTO diesel_id   FROM dim_fuel WHERE fuel_code = 'DIESEL';
    SELECT fuel_id INTO cng_id      FROM dim_fuel WHERE fuel_code = 'CNG ONLY';
    SELECT fuel_id INTO hybrid_p_id FROM dim_fuel WHERE fuel_code = 'PETROL/HYBRID';
    SELECT fuel_id INTO ev_id       FROM dim_fuel WHERE fuel_code = 'ELECTRIC(BOV)';

    INSERT INTO fact_asp_master (segment_id, fuel_id, effective_from, effective_to, asp_ex_factory_rupees, asp_source, confidence, notes) VALUES
    (pv_seg, petrol_id,   '2025-04-01', NULL, 750000.00,   'ESTIMATED', 'MEDIUM', 'Blended PV petrol ASP: hatchback ~5L, sedan ~8L, SUV ~12L weighted'),
    (pv_seg, diesel_id,   '2025-04-01', NULL, 1050000.00,  'ESTIMATED', 'MEDIUM', 'Blended PV diesel ASP: sedan ~10L, SUV ~15L weighted'),
    (pv_seg, cng_id,      '2025-04-01', NULL, 820000.00,   'ESTIMATED', 'MEDIUM', 'PV CNG: Maruti CNG models dominate'),
    (pv_seg, hybrid_p_id, '2025-04-01', NULL, 1600000.00,  'ESTIMATED', 'LOW',    'PV Hybrid: Innova HyCross, Hyryder, Grand Vitara weighted'),
    (pv_seg, ev_id,       '2025-04-01', NULL, 1200000.00,  'ESTIMATED', 'LOW',    'PV EV: Nexon EV ~15L, Tiago EV ~10L, XUV400 ~16L weighted'),
    (cv_seg, diesel_id,   '2025-04-01', NULL, 1500000.00,  'ESTIMATED', 'LOW',    'Blended CV diesel: LCV ~8L, MHCV ~25L, Bus ~35L weighted'),
    (cv_seg, cng_id,      '2025-04-01', NULL, 1200000.00,  'ESTIMATED', 'LOW',    'CV CNG: mostly LCV (Tata Ace CNG, Ashok Leyland Dost CNG)'),
    (cv_seg, ev_id,       '2025-04-01', NULL, 10000000.00, 'ESTIMATED', 'LOW',    'CV EV: Electric buses (Olectra/Switch/Tata) dominate. ~1Cr+ each.'),
    (tw_seg, petrol_id,   '2025-04-01', NULL, 95000.00,    'ESTIMATED', 'MEDIUM', 'Blended 2W petrol: motorcycle ~85K, scooter ~80K, premium ~2L weighted'),
    (tw_seg, ev_id,       '2025-04-01', NULL, 110000.00,   'ESTIMATED', 'MEDIUM', 'Blended 2W EV: Ola ~1.1L, Ather ~1.3L, TVS iQube ~1.2L, Chetak ~1.5L weighted');
END $$;

-- ============================================================================
-- VERIFY SEED COUNTS
-- ============================================================================
-- Expected:
--   dim_date:              ~4,383 rows (12 years)
--   dim_segment:           15 rows
--   dim_fuel:              22 rows
--   dim_vehicle_class_map: 46 rows
--   dim_oem:               17 rows (15 listed-entities + BYD + Others)
--   dim_oem_alias:         ~100+ rows
--   dim_geo:               1 row
--   fact_asp_master:       10 rows
