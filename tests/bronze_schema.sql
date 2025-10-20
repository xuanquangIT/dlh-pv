-- Solar Facilities Registry (Simplified and Optimized)
CREATE TABLE bronze.nem_facilities (
    -- Primary identification
    facility_code STRING NOT NULL, -- DUID equivalent (AVLSF, BERYLSF, etc.)
    facility_name STRING NOT NULL,
    
    -- Location information (essential for weather matching)
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    
    -- Network and region (essential for market analysis)
    network_id STRING, -- NEM, WEM
    network_region STRING, -- NSW1, VIC1, QLD1, SA1, WEM
    
    -- Capacity information (essential for forecasting)
    total_capacity_mw DOUBLE, -- Nameplate capacity
    total_capacity_registered_mw DOUBLE, -- Registered with AEMO
    total_capacity_maximum_mw DOUBLE, -- Maximum export capability
    
    -- Technology classification (for filtering solar only)
    fuel_technology STRING, -- solar_utility, solar_rooftop
    unit_count INT, -- Number of units at facility
    
    -- Operational status
    operational_status STRING, -- operating, commissioning, retired
    
    -- Data tracking
    facility_created_at TIMESTAMP,
    facility_updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    
    -- Unit codes for generation data mapping
    unit_codes STRING -- Comma-separated DUIDs: "AVLSF1" or "NEWENSF1,NEWENSF2"
)
PARTITIONED BY (network_region, fuel_technology)


-- Solar Generation Time Series (Minimal Essential Fields)
CREATE TABLE bronze.solar_generation (
    -- Time and identification
    settlement_timestamp TIMESTAMP NOT NULL,
    duid STRING NOT NULL, -- Individual unit ID (AVLSF1, BERYLSF1, etc.)
    
    -- Generation data (core metrics only)
    generation_mw DOUBLE, -- Actual power output
    capacity_factor DOUBLE, -- generation_mw / registered_capacity
    
    -- Data quality and source tracking
    data_quality_code STRING, -- ACTUAL, ESTIMATED, SUBSTITUTED
    data_source STRING, -- AEMO_SCADA, OPENNEM
    
    -- Batch processing
    ingestion_timestamp TIMESTAMP,
    
    -- Efficient partitioning
    year INT,
    month INT,
    settlement_date DATE
)
PARTITIONED BY (year, month)


-- Weather Observations (Essential Variables Only)
CREATE TABLE bronze.weather (
    -- Time and location
    observation_timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL, -- Rounded to 0.25° grid (Open-Meteo resolution)
    longitude DOUBLE NOT NULL,
    
    -- Core solar forecasting variables (avoid rate limit waste)
    temperature_2m DOUBLE, -- °C
    relative_humidity_2m DOUBLE, -- %
    pressure_msl DOUBLE, -- hPa
    
    -- Wind (minimal set)
    wind_speed_10m DOUBLE, -- m/s
    wind_direction_10m DOUBLE, -- degrees
    
    -- Solar radiation (ESSENTIAL - highest priority)
    shortwave_radiation DOUBLE, -- W/m² (Global Horizontal Irradiance)
    direct_normal_irradiance DOUBLE, -- W/m²
    diffuse_radiation DOUBLE, -- W/m²
    
    -- Cloud cover (major impact on solar)
    cloud_cover DOUBLE, -- % total
    cloud_cover_low DOUBLE, -- % low clouds
    
    -- Precipitation (affects panel performance)
    precipitation DOUBLE, -- mm
    
    -- Data quality
    data_completeness_score DOUBLE, -- 0-1 scale
    api_source_model STRING, -- ERA5, ERA5-Land
    
    -- Processing metadata
    api_request_id STRING,
    ingestion_timestamp TIMESTAMP,
    
    -- Partitioning
    year INT,
    month INT,
    observation_date DATE
)
PARTITIONED BY (year, month)


-- Air Quality Observations (Essential for Solar Analysis)
CREATE TABLE bronze.air_quality (
    -- Time and location
    observation_timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL, -- Rounded to CAMS grid resolution  
    longitude DOUBLE NOT NULL,
    
    -- Particulate matter (PRIMARY impact on solar irradiance)
    pm2_5 DOUBLE, -- μg/m³ - Fine particles, highest impact
    pm10 DOUBLE, -- μg/m³ - Coarse particles
    
    -- Atmospheric optical properties (CRITICAL for solar)
    aerosol_optical_depth DOUBLE, -- Dimensionless, direct solar impact
    dust DOUBLE, -- μg/m³ - Saharan/mineral dust
    
    -- Key gaseous pollutants (secondary priority)
    nitrogen_dioxide DOUBLE, -- μg/m³
    ozone DOUBLE, -- μg/m³
    
    -- Air quality index (derived metric)
    european_aqi INT,
    
    -- Data quality and source
    data_completeness_score DOUBLE,
    cams_domain STRING, -- cams_global, cams_europe
    
    -- Processing metadata
    api_request_id STRING,
    ingestion_timestamp TIMESTAMP,
    
    -- Partitioning
    year INT,
    month INT,
    observation_date DATE
)
PARTITIONED BY (year, month)
