-- ============================================================================
-- Bronze Tables Complete Test Script
-- ============================================================================
-- Purpose: Create tables, insert sample data, verify, and cleanup
-- Tables: oe_facilities_raw, oe_generation_hourly_raw, 
--         om_weather_hourly_raw, om_air_quality_hourly_raw
-- ============================================================================

-- ============================================================================
-- STEP 1: Create all Bronze tables
-- ============================================================================

-- 1.1 Create oe_facilities_raw
DROP TABLE IF EXISTS lh.bronze.oe_facilities_raw;

CREATE TABLE IF NOT EXISTS lh.bronze.oe_facilities_raw (
    facility_code STRING NOT NULL,
    facility_name STRING NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    network_id STRING,
    network_region STRING,
    total_capacity_mw DOUBLE,
    total_capacity_registered_mw DOUBLE,
    total_capacity_maximum_mw DOUBLE,
    fuel_technology STRING,
    unit_count INT,
    operational_status STRING,
    facility_created_at TIMESTAMP,
    facility_updated_at TIMESTAMP,
    unit_codes STRING,
    ts_utc TIMESTAMP NOT NULL,
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw OpenNEM facilities registry - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);

-- 1.2 Create oe_generation_hourly_raw
DROP TABLE IF EXISTS lh.bronze.oe_generation_hourly_raw;

CREATE TABLE IF NOT EXISTS lh.bronze.oe_generation_hourly_raw (
    ts_utc TIMESTAMP NOT NULL,
    duid STRING NOT NULL,
    generation_mw DOUBLE,
    capacity_factor DOUBLE,
    data_quality_code STRING,
    data_source STRING,
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw OpenNEM solar generation hourly time series data - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);

-- 1.3 Create om_weather_hourly_raw
DROP TABLE IF EXISTS lh.bronze.om_weather_hourly_raw;

CREATE TABLE IF NOT EXISTS lh.bronze.om_weather_hourly_raw (
    ts_utc TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    temperature_2m DOUBLE,
    relative_humidity_2m DOUBLE,
    pressure_msl DOUBLE,
    wind_speed_10m DOUBLE,
    wind_direction_10m DOUBLE,
    shortwave_radiation DOUBLE,
    direct_normal_irradiance DOUBLE,
    diffuse_radiation DOUBLE,
    cloud_cover DOUBLE,
    cloud_cover_low DOUBLE,
    precipitation DOUBLE,
    data_completeness_score DOUBLE,
    api_source_model STRING,
    api_request_id STRING,
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw Open-Meteo weather hourly observations - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);

-- 1.4 Create om_air_quality_hourly_raw
DROP TABLE IF EXISTS lh.bronze.om_air_quality_hourly_raw;

CREATE TABLE IF NOT EXISTS lh.bronze.om_air_quality_hourly_raw (
    ts_utc TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    pm2_5 DOUBLE,
    pm10 DOUBLE,
    aerosol_optical_depth DOUBLE,
    dust DOUBLE,
    nitrogen_dioxide DOUBLE,
    ozone DOUBLE,
    european_aqi INT,
    data_completeness_score DOUBLE,
    cams_domain STRING,
    api_request_id STRING,
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw Open-Meteo air quality hourly observations - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);

-- ============================================================================
-- STEP 2: Insert Sample Data
-- ============================================================================

-- 2.1 Insert sample data into oe_facilities_raw
INSERT INTO lh.bronze.oe_facilities_raw VALUES
(
    'AVLSF',
    'Avonlie Solar Farm',
    -35.1234,
    148.5678,
    'NEM',
    'NSW1',
    245.0,
    240.0,
    245.0,
    'solar_utility',
    1,
    'operating',
    TIMESTAMP '2021-06-15 00:00:00',
    TIMESTAMP '2024-01-10 00:00:00',
    'AVLSF1',
    TIMESTAMP '2025-01-15 12:00:00',
    TIMESTAMP '2025-01-15 12:05:00',
    'opennem_api_v3',
    'abc123def456'
),
(
    'BERYLSF',
    'Beryl Solar Farm',
    -28.9876,
    150.1234,
    'NEM',
    'QLD1',
    100.0,
    95.0,
    100.0,
    'solar_utility',
    1,
    'operating',
    TIMESTAMP '2020-03-20 00:00:00',
    TIMESTAMP '2024-01-10 00:00:00',
    'BERYLSF1',
    TIMESTAMP '2025-01-15 12:00:00',
    TIMESTAMP '2025-01-15 12:05:00',
    'opennem_api_v3',
    'xyz789ghi012'
);

-- 2.2 Insert sample data into oe_generation_hourly_raw
INSERT INTO lh.bronze.oe_generation_hourly_raw VALUES
(TIMESTAMP '2025-01-15 00:00:00', 'AVLSF1', 0.0, 0.0, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 01:05:00', 'opennem_api_v3', 'hash001'),
(TIMESTAMP '2025-01-15 01:00:00', 'AVLSF1', 0.0, 0.0, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 02:05:00', 'opennem_api_v3', 'hash002'),
(TIMESTAMP '2025-01-15 02:00:00', 'AVLSF1', 5.2, 0.022, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 03:05:00', 'opennem_api_v3', 'hash003'),
(TIMESTAMP '2025-01-15 03:00:00', 'AVLSF1', 42.8, 0.178, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 04:05:00', 'opennem_api_v3', 'hash004'),
(TIMESTAMP '2025-01-15 04:00:00', 'AVLSF1', 112.5, 0.469, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 05:05:00', 'opennem_api_v3', 'hash005'),
(TIMESTAMP '2025-01-15 05:00:00', 'AVLSF1', 185.3, 0.772, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 06:05:00', 'opennem_api_v3', 'hash006'),
(TIMESTAMP '2025-01-15 06:00:00', 'AVLSF1', 220.8, 0.920, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 07:05:00', 'opennem_api_v3', 'hash007'),
(TIMESTAMP '2025-01-15 07:00:00', 'AVLSF1', 198.4, 0.827, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 08:05:00', 'opennem_api_v3', 'hash008'),
(TIMESTAMP '2025-01-15 00:00:00', 'BERYLSF1', 0.0, 0.0, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 01:05:00', 'opennem_api_v3', 'hash101'),
(TIMESTAMP '2025-01-15 01:00:00', 'BERYLSF1', 0.0, 0.0, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 02:05:00', 'opennem_api_v3', 'hash102'),
(TIMESTAMP '2025-01-15 02:00:00', 'BERYLSF1', 2.1, 0.022, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 03:05:00', 'opennem_api_v3', 'hash103'),
(TIMESTAMP '2025-01-15 03:00:00', 'BERYLSF1', 18.5, 0.195, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 04:05:00', 'opennem_api_v3', 'hash104');

-- 2.3 Insert sample data into om_weather_hourly_raw
INSERT INTO lh.bronze.om_weather_hourly_raw VALUES
(TIMESTAMP '2025-01-15 00:00:00', -35.125, 148.5, 22.5, 65.0, 1013.2, 3.2, 180.0, 0.0, 0.0, 0.0, 15.0, 10.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 01:00:00', 'openmeteo_api_v1', 'weather_hash_001'),
(TIMESTAMP '2025-01-15 01:00:00', -35.125, 148.5, 21.8, 68.0, 1013.5, 2.8, 185.0, 0.0, 0.0, 0.0, 18.0, 12.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 02:00:00', 'openmeteo_api_v1', 'weather_hash_002'),
(TIMESTAMP '2025-01-15 02:00:00', -35.125, 148.5, 21.2, 70.0, 1013.8, 2.5, 190.0, 45.0, 120.0, 35.0, 20.0, 15.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 03:00:00', 'openmeteo_api_v1', 'weather_hash_003'),
(TIMESTAMP '2025-01-15 03:00:00', -35.125, 148.5, 22.8, 62.0, 1014.0, 3.5, 175.0, 285.0, 650.0, 210.0, 25.0, 18.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 04:00:00', 'openmeteo_api_v1', 'weather_hash_004'),
(TIMESTAMP '2025-01-15 04:00:00', -35.125, 148.5, 25.2, 55.0, 1013.5, 4.2, 170.0, 520.0, 780.0, 380.0, 30.0, 20.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 05:00:00', 'openmeteo_api_v1', 'weather_hash_005'),
(TIMESTAMP '2025-01-15 05:00:00', -35.125, 148.5, 27.5, 48.0, 1013.0, 4.8, 165.0, 720.0, 850.0, 520.0, 25.0, 18.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 06:00:00', 'openmeteo_api_v1', 'weather_hash_006'),
(TIMESTAMP '2025-01-15 06:00:00', -35.125, 148.5, 29.2, 42.0, 1012.8, 5.2, 160.0, 820.0, 890.0, 610.0, 20.0, 15.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 07:00:00', 'openmeteo_api_v1', 'weather_hash_007'),
(TIMESTAMP '2025-01-15 07:00:00', -35.125, 148.5, 30.1, 40.0, 1012.5, 5.5, 155.0, 780.0, 870.0, 580.0, 22.0, 16.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 08:00:00', 'openmeteo_api_v1', 'weather_hash_008');

-- 2.4 Insert sample data into om_air_quality_hourly_raw
INSERT INTO lh.bronze.om_air_quality_hourly_raw VALUES
(TIMESTAMP '2025-01-15 00:00:00', -35.125, 148.5, 8.2, 15.3, 0.12, 2.1, 12.5, 45.8, 25, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 01:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_001'),
(TIMESTAMP '2025-01-15 01:00:00', -35.125, 148.5, 7.8, 14.8, 0.11, 2.0, 11.8, 44.2, 23, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 02:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_002'),
(TIMESTAMP '2025-01-15 02:00:00', -35.125, 148.5, 7.5, 14.2, 0.10, 1.8, 11.2, 42.5, 22, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 03:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_003'),
(TIMESTAMP '2025-01-15 03:00:00', -35.125, 148.5, 7.2, 13.8, 0.10, 1.7, 10.8, 41.0, 21, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 04:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_004'),
(TIMESTAMP '2025-01-15 04:00:00', -35.125, 148.5, 6.8, 13.2, 0.09, 1.5, 10.2, 39.5, 20, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 05:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_005'),
(TIMESTAMP '2025-01-15 05:00:00', -35.125, 148.5, 6.5, 12.8, 0.09, 1.4, 9.8, 38.2, 19, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 06:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_006'),
(TIMESTAMP '2025-01-15 06:00:00', -35.125, 148.5, 6.2, 12.5, 0.08, 1.3, 9.5, 37.0, 18, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 07:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_007'),
(TIMESTAMP '2025-01-15 07:00:00', -35.125, 148.5, 6.5, 13.0, 0.09, 1.4, 10.0, 38.5, 19, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 08:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_008');

-- ============================================================================
-- STEP 3: Verify Data and Partitions
-- ============================================================================

-- 3.1 Count records in each table
SELECT 'oe_facilities_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.oe_facilities_raw
UNION ALL
SELECT 'oe_generation_hourly_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.oe_generation_hourly_raw
UNION ALL
SELECT 'om_weather_hourly_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.om_weather_hourly_raw
UNION ALL
SELECT 'om_air_quality_hourly_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.om_air_quality_hourly_raw;

-- 3.2 Verify partition columns (check for days(ts_utc))
-- This query shows the partition spec for each table
SHOW CREATE TABLE lh.bronze.oe_facilities_raw;
SHOW CREATE TABLE lh.bronze.oe_generation_hourly_raw;
SHOW CREATE TABLE lh.bronze.om_weather_hourly_raw;
SHOW CREATE TABLE lh.bronze.om_air_quality_hourly_raw;

-- 3.3 Sample queries to verify data quality

-- Check facilities
SELECT facility_code, facility_name, latitude, longitude, total_capacity_mw, _source, _hash
FROM lh.bronze.oe_facilities_raw
ORDER BY facility_code;

-- Check generation data
SELECT ts_utc, duid, generation_mw, capacity_factor, data_quality_code, _source
FROM lh.bronze.oe_generation_hourly_raw
WHERE duid = 'AVLSF1'
ORDER BY ts_utc
LIMIT 5;

-- Check weather data
SELECT ts_utc, temperature_2m, shortwave_radiation, direct_normal_irradiance, cloud_cover, _source
FROM lh.bronze.om_weather_hourly_raw
ORDER BY ts_utc
LIMIT 5;

-- Check air quality data
SELECT ts_utc, pm2_5, pm10, aerosol_optical_depth, european_aqi, _source
FROM lh.bronze.om_air_quality_hourly_raw
ORDER BY ts_utc
LIMIT 5;

-- 3.4 Verify metadata columns exist and are populated
SELECT 
    'oe_facilities_raw' AS table_name,
    COUNT(*) AS total_records,
    COUNT(_ingest_time) AS has_ingest_time,
    COUNT(_source) AS has_source,
    COUNT(_hash) AS has_hash
FROM lh.bronze.oe_facilities_raw
UNION ALL
SELECT 
    'oe_generation_hourly_raw' AS table_name,
    COUNT(*) AS total_records,
    COUNT(_ingest_time) AS has_ingest_time,
    COUNT(_source) AS has_source,
    COUNT(_hash) AS has_hash
FROM lh.bronze.oe_generation_hourly_raw
UNION ALL
SELECT 
    'om_weather_hourly_raw' AS table_name,
    COUNT(*) AS total_records,
    COUNT(_ingest_time) AS has_ingest_time,
    COUNT(_source) AS has_source,
    COUNT(_hash) AS has_hash
FROM lh.bronze.om_weather_hourly_raw
UNION ALL
SELECT 
    'om_air_quality_hourly_raw' AS table_name,
    COUNT(*) AS total_records,
    COUNT(_ingest_time) AS has_ingest_time,
    COUNT(_source) AS has_source,
    COUNT(_hash) AS has_hash
FROM lh.bronze.om_air_quality_hourly_raw;

-- ============================================================================
-- STEP 4: Cleanup (Run after testing is complete)
-- ============================================================================

-- UNCOMMENT THE FOLLOWING LINES TO DELETE TEST DATA
-- DELETE FROM lh.bronze.oe_facilities_raw WHERE _source = 'opennem_api_v3' AND facility_code IN ('AVLSF', 'BERYLSF');
-- DELETE FROM lh.bronze.oe_generation_hourly_raw WHERE _source = 'opennem_api_v3' AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' AND ts_utc < TIMESTAMP '2025-01-16 00:00:00';
-- DELETE FROM lh.bronze.om_weather_hourly_raw WHERE _source = 'openmeteo_api_v1' AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' AND ts_utc < TIMESTAMP '2025-01-16 00:00:00';
-- DELETE FROM lh.bronze.om_air_quality_hourly_raw WHERE _source = 'openmeteo_airquality_api_v1' AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' AND ts_utc < TIMESTAMP '2025-01-16 00:00:00';

-- Or drop tables completely (use with caution):
-- DROP TABLE IF EXISTS lh.bronze.oe_facilities_raw;
-- DROP TABLE IF EXISTS lh.bronze.oe_generation_hourly_raw;
-- DROP TABLE IF EXISTS lh.bronze.om_weather_hourly_raw;
-- DROP TABLE IF EXISTS lh.bronze.om_air_quality_hourly_raw;

-- ============================================================================
-- END OF TEST SCRIPT
-- ============================================================================
