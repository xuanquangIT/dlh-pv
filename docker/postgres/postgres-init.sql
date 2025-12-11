-- Create application databases owned by the configured POSTGRES_USER
-- Note: docker-compose injects POSTGRES_USER/POSTGRES_PASSWORD via environment when initdb runs
CREATE DATABASE iceberg OWNER "${POSTGRES_USER}";
CREATE DATABASE iceberg_catalog OWNER "${POSTGRES_USER}";
CREATE DATABASE mlflow OWNER "${POSTGRES_USER}";
GRANT ALL PRIVILEGES ON DATABASE iceberg TO "${POSTGRES_USER}";
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO "${POSTGRES_USER}";
GRANT ALL PRIVILEGES ON DATABASE mlflow TO "${POSTGRES_USER}";

-- Create dedicated Iceberg catalog user
CREATE USER iceberg_user WITH PASSWORD 'iceberg_pass';
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO iceberg_user;

-- Connect to iceberg_catalog and grant schema privileges
\c iceberg_catalog;
GRANT ALL ON SCHEMA public TO iceberg_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iceberg_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iceberg_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO iceberg_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO iceberg_user;

-- Create Iceberg JDBC catalog schema tables
-- These tables are required for Trino's JDBC catalog to function
CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location TEXT,
    previous_metadata_location TEXT,
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value VARCHAR(255),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

-- Grant privileges on Iceberg catalog tables
GRANT ALL PRIVILEGES ON iceberg_tables TO iceberg_user;
GRANT ALL PRIVILEGES ON iceberg_namespace_properties TO iceberg_user;
GRANT ALL PRIVILEGES ON iceberg_tables TO "${POSTGRES_USER}";
GRANT ALL PRIVILEGES ON iceberg_namespace_properties TO "${POSTGRES_USER}";
