-- OpenNEM Solar Generation Time Series (Raw Bronze Layer)
-- Source: OpenNEM API
-- Table: lh.bronze.oe_generation_hourly_raw
-- Partition: days(ts_utc)

CREATE TABLE IF NOT EXISTS lh.bronze.oe_generation_hourly_raw (
    -- Time and identification
    ts_utc TIMESTAMP NOT NULL COMMENT 'Settlement/observation timestamp in UTC',
    duid STRING NOT NULL COMMENT 'Individual unit ID (AVLSF1, BERYLSF1, etc.)',
    
    -- Generation data (core metrics only)
    generation_mw DOUBLE COMMENT 'Actual power output in MW',
    capacity_factor DOUBLE COMMENT 'generation_mw / registered_capacity',
    
    -- Data quality and source tracking
    data_quality_code STRING COMMENT 'ACTUAL, ESTIMATED, SUBSTITUTED',
    data_source STRING COMMENT 'AEMO_SCADA, OPENNEM',
    
    -- Metadata columns (Bronze layer standard)
    _ingest_time TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp (when data landed in lakehouse)',
    _source STRING NOT NULL COMMENT 'Data source identifier (e.g., opennem_api_v1)',
    _hash STRING COMMENT 'Hash of record for deduplication (SHA256 of key fields)'
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw OpenNEM solar generation hourly time series data - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);
