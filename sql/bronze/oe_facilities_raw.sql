-- OpenNEM Facilities Registry (Raw Bronze Layer)
-- Source: OpenNEM API
-- Table: lh.bronze.oe_facilities_raw
-- Partition: days(ts_utc)

CREATE TABLE IF NOT EXISTS lh.bronze.oe_facilities_raw (
    -- Primary identification
    facility_code STRING NOT NULL COMMENT 'DUID equivalent (AVLSF, BERYLSF, etc.)',
    facility_name STRING NOT NULL COMMENT 'Full facility name',
    
    -- Location information (essential for weather matching)
    latitude DOUBLE NOT NULL COMMENT 'Facility latitude',
    longitude DOUBLE NOT NULL COMMENT 'Facility longitude',
    
    -- Network and region (essential for market analysis)
    network_id STRING COMMENT 'NEM, WEM',
    network_region STRING COMMENT 'NSW1, VIC1, QLD1, SA1, WEM',
    
    -- Capacity information (essential for forecasting)
    total_capacity_mw DOUBLE COMMENT 'Nameplate capacity',
    total_capacity_registered_mw DOUBLE COMMENT 'Registered with AEMO',
    total_capacity_maximum_mw DOUBLE COMMENT 'Maximum export capability',
    
    -- Technology classification (for filtering solar only)
    fuel_technology STRING COMMENT 'solar_utility, solar_rooftop',
    unit_count INT COMMENT 'Number of units at facility',
    
    -- Operational status
    operational_status STRING COMMENT 'operating, commissioning, retired',
    
    -- Data tracking
    facility_created_at TIMESTAMP COMMENT 'Facility creation timestamp from source',
    facility_updated_at TIMESTAMP COMMENT 'Facility last update timestamp from source',
    
    -- Unit codes for generation data mapping
    unit_codes STRING COMMENT 'Comma-separated DUIDs: AVLSF1 or NEWENSF1,NEWENSF2',
    
    -- Timestamp for partitioning
    ts_utc TIMESTAMP NOT NULL COMMENT 'UTC timestamp for partitioning (snapshot time)',
    
    -- Metadata columns (Bronze layer standard)
    _ingest_time TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp (when data landed in lakehouse)',
    _source STRING NOT NULL COMMENT 'Data source identifier (e.g., opennem_api_v1)',
    _hash STRING COMMENT 'Hash of record for deduplication (SHA256 of key fields)'
)
USING iceberg
PARTITIONED BY (days(ts_utc))
COMMENT 'Raw OpenNEM facilities registry data - Bronze layer'
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
);
