-- Open-Meteo Air Quality Observations (Raw Bronze Layer)
-- Source: Open-Meteo Air Quality API
-- Table: lh.bronze.om_air_quality_hourly_raw
-- Partition: days(ts_utc)

CREATE TABLE IF NOT EXISTS lh.bronze.om_air_quality_hourly_raw (
    -- Time and location
    ts_utc TIMESTAMP NOT NULL COMMENT 'Observation timestamp in UTC',
    latitude DOUBLE NOT NULL COMMENT 'Rounded to CAMS grid resolution',
    longitude DOUBLE NOT NULL COMMENT 'Rounded to CAMS grid resolution',
    
    -- Particulate matter (PRIMARY impact on solar irradiance)
    pm2_5 DOUBLE COMMENT 'Fine particulate matter PM2.5 (μg/m³)',
    pm10 DOUBLE COMMENT 'Coarse particulate matter PM10 (μg/m³)',
    
    -- Atmospheric optical properties (CRITICAL for solar)
    aerosol_optical_depth DOUBLE COMMENT 'Aerosol Optical Depth (dimensionless, direct solar impact)',
    dust DOUBLE COMMENT 'Dust concentration (μg/m³) - Saharan/mineral dust',
    
    -- Key gaseous pollutants (secondary priority)
    nitrogen_dioxide DOUBLE COMMENT 'NO2 concentration (μg/m³)',
    ozone DOUBLE COMMENT 'O3 concentration (μg/m³)',
    
    -- Air quality index (derived metric)
    european_aqi INT COMMENT 'European Air Quality Index',
    
    -- Data quality and source
    data_completeness_score DOUBLE COMMENT 'Data completeness score (0-1 scale)',
    cams_domain STRING COMMENT 'CAMS domain: cams_global, cams_europe',
    
    -- Processing metadata from API
    api_request_id STRING COMMENT 'API request identifier for traceability',
    
    -- Metadata columns (Bronze layer standard)
    _ingest_time TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp (when data landed in lakehouse)',
    _source STRING NOT NULL COMMENT 'Data source identifier (e.g., openmeteo_airquality_api_v1)',
    _hash STRING COMMENT 'Hash of record for deduplication (SHA256 of key fields)'
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw Open-Meteo air quality hourly observations - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);
