-- Open-Meteo Weather Observations (Raw Bronze Layer)
-- Source: Open-Meteo API
-- Table: lh.bronze.om_weather_hourly_raw
-- Partition: days(ts_utc)

CREATE TABLE IF NOT EXISTS lh.bronze.om_weather_hourly_raw (
    -- Time and location
    ts_utc TIMESTAMP NOT NULL COMMENT 'Observation timestamp in UTC',
    latitude DOUBLE NOT NULL COMMENT 'Rounded to 0.25° grid (Open-Meteo resolution)',
    longitude DOUBLE NOT NULL COMMENT 'Rounded to 0.25° grid (Open-Meteo resolution)',
    
    -- Core solar forecasting variables
    temperature_2m DOUBLE COMMENT 'Temperature at 2m height (°C)',
    relative_humidity_2m DOUBLE COMMENT 'Relative humidity at 2m height (%)',
    pressure_msl DOUBLE COMMENT 'Mean sea level pressure (hPa)',
    
    -- Wind (minimal set)
    wind_speed_10m DOUBLE COMMENT 'Wind speed at 10m height (m/s)',
    wind_direction_10m DOUBLE COMMENT 'Wind direction at 10m height (degrees)',
    
    -- Solar radiation (ESSENTIAL - highest priority)
    shortwave_radiation DOUBLE COMMENT 'Global Horizontal Irradiance (W/m²)',
    direct_normal_irradiance DOUBLE COMMENT 'Direct Normal Irradiance (W/m²)',
    diffuse_radiation DOUBLE COMMENT 'Diffuse radiation (W/m²)',
    
    -- Cloud cover (major impact on solar)
    cloud_cover DOUBLE COMMENT 'Total cloud cover (%)',
    cloud_cover_low DOUBLE COMMENT 'Low cloud cover (%)',
    
    -- Precipitation (affects panel performance)
    precipitation DOUBLE COMMENT 'Precipitation (mm)',
    
    -- Data quality
    data_completeness_score DOUBLE COMMENT 'Data completeness score (0-1 scale)',
    api_source_model STRING COMMENT 'Source model: ERA5, ERA5-Land, etc.',
    
    -- Processing metadata from API
    api_request_id STRING COMMENT 'API request identifier for traceability',
    
    -- Metadata columns (Bronze layer standard)
    _ingest_time TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp (when data landed in lakehouse)',
    _source STRING NOT NULL COMMENT 'Data source identifier (e.g., openmeteo_api_v1)',
    _hash STRING COMMENT 'Hash of record for deduplication (SHA256 of key fields)'
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw Open-Meteo weather hourly observations - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);
