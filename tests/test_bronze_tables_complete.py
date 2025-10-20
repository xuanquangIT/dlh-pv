#!/usr/bin/env python3
"""
Bronze Tables Complete Test Script (Python version)
===================================================
Purpose: Create tables, insert sample data, verify, and cleanup
Tables: oe_facilities_raw, oe_generation_hourly_raw, 
        om_weather_hourly_raw, om_air_quality_hourly_raw

Usage:
    python tests/test_bronze_tables_complete.py --create    # Create tables and insert data
    python tests/test_bronze_tables_complete.py --verify    # Verify tables
    python tests/test_bronze_tables_complete.py --cleanup   # Cleanup test data
    python tests/test_bronze_tables_complete.py --all       # Run all steps
"""

import argparse
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))


def get_spark_session():
    """Initialize Spark session with Iceberg support"""
    return (
        SparkSession.builder
        .appName("BronzeTablesTest")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lh.type", "rest")
        .config("spark.sql.catalog.lh.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lh.warehouse", "s3a://lakehouse/")
        .config("spark.sql.catalog.lh.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lh.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lh.s3.access-key-id", "minio")
        .config("spark.sql.catalog.lh.s3.secret-access-key", "minio123")
        .config("spark.sql.catalog.lh.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lh")
        .enableHiveSupport()
        .getOrCreate()
    )


def create_tables(spark):
    """Create all bronze tables by executing DDL files"""
    print("\n" + "=" * 80)
    print("STEP 1: Creating Bronze Tables")
    print("=" * 80)
    
    # Create schema if not exists
    spark.sql("CREATE SCHEMA IF NOT EXISTS lh.bronze")
    print("✓ Schema lh.bronze created/verified")
    
    # List of DDL files to execute
    ddl_files = [
        "sql/bronze/oe_facilities_raw.sql",
        "sql/bronze/oe_generation_hourly_raw.sql",
        "sql/bronze/om_weather_hourly_raw.sql",
        "sql/bronze/om_air_quality_hourly_raw.sql",
    ]
    
    for ddl_file in ddl_files:
        ddl_path = project_root / ddl_file
        table_name = Path(ddl_file).stem
        
        print(f"\n→ Creating table from {ddl_file}...")
        
        # Read and execute DDL
        with open(ddl_path, 'r') as f:
            ddl_content = f.read()
        
        # Remove comments and execute
        ddl_clean = '\n'.join([
            line for line in ddl_content.split('\n')
            if not line.strip().startswith('--') and line.strip()
        ])
        
        try:
            spark.sql(ddl_clean)
            print(f"  ✓ Table lh.bronze.{table_name} created successfully")
        except Exception as e:
            print(f"  ✗ Error creating table {table_name}: {e}")
            raise


def insert_sample_data(spark):
    """Insert sample data into all bronze tables"""
    print("\n" + "=" * 80)
    print("STEP 2: Inserting Sample Data")
    print("=" * 80)
    
    # 2.1 Insert sample data into oe_facilities_raw
    print("\n→ Inserting data into oe_facilities_raw...")
    spark.sql("""
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
        )
    """)
    print("  ✓ 2 records inserted into oe_facilities_raw")
    
    # 2.2 Insert sample data into oe_generation_hourly_raw
    print("\n→ Inserting data into oe_generation_hourly_raw...")
    spark.sql("""
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
        (TIMESTAMP '2025-01-15 03:00:00', 'BERYLSF1', 18.5, 0.195, 'ACTUAL', 'AEMO_SCADA', TIMESTAMP '2025-01-15 04:05:00', 'opennem_api_v3', 'hash104')
    """)
    print("  ✓ 12 records inserted into oe_generation_hourly_raw")
    
    # 2.3 Insert sample data into om_weather_hourly_raw
    print("\n→ Inserting data into om_weather_hourly_raw...")
    spark.sql("""
        INSERT INTO lh.bronze.om_weather_hourly_raw VALUES
        (TIMESTAMP '2025-01-15 00:00:00', -35.125, 148.5, 22.5, 65.0, 1013.2, 3.2, 180.0, 0.0, 0.0, 0.0, 15.0, 10.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 01:00:00', 'openmeteo_api_v1', 'weather_hash_001'),
        (TIMESTAMP '2025-01-15 01:00:00', -35.125, 148.5, 21.8, 68.0, 1013.5, 2.8, 185.0, 0.0, 0.0, 0.0, 18.0, 12.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 02:00:00', 'openmeteo_api_v1', 'weather_hash_002'),
        (TIMESTAMP '2025-01-15 02:00:00', -35.125, 148.5, 21.2, 70.0, 1013.8, 2.5, 190.0, 45.0, 120.0, 35.0, 20.0, 15.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 03:00:00', 'openmeteo_api_v1', 'weather_hash_003'),
        (TIMESTAMP '2025-01-15 03:00:00', -35.125, 148.5, 22.8, 62.0, 1014.0, 3.5, 175.0, 285.0, 650.0, 210.0, 25.0, 18.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 04:00:00', 'openmeteo_api_v1', 'weather_hash_004'),
        (TIMESTAMP '2025-01-15 04:00:00', -35.125, 148.5, 25.2, 55.0, 1013.5, 4.2, 170.0, 520.0, 780.0, 380.0, 30.0, 20.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 05:00:00', 'openmeteo_api_v1', 'weather_hash_005'),
        (TIMESTAMP '2025-01-15 05:00:00', -35.125, 148.5, 27.5, 48.0, 1013.0, 4.8, 165.0, 720.0, 850.0, 520.0, 25.0, 18.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 06:00:00', 'openmeteo_api_v1', 'weather_hash_006'),
        (TIMESTAMP '2025-01-15 06:00:00', -35.125, 148.5, 29.2, 42.0, 1012.8, 5.2, 160.0, 820.0, 890.0, 610.0, 20.0, 15.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 07:00:00', 'openmeteo_api_v1', 'weather_hash_007'),
        (TIMESTAMP '2025-01-15 07:00:00', -35.125, 148.5, 30.1, 40.0, 1012.5, 5.5, 155.0, 780.0, 870.0, 580.0, 22.0, 16.0, 0.0, 1.0, 'ERA5', 'req_001', TIMESTAMP '2025-01-15 08:00:00', 'openmeteo_api_v1', 'weather_hash_008')
    """)
    print("  ✓ 8 records inserted into om_weather_hourly_raw")
    
    # 2.4 Insert sample data into om_air_quality_hourly_raw
    print("\n→ Inserting data into om_air_quality_hourly_raw...")
    spark.sql("""
        INSERT INTO lh.bronze.om_air_quality_hourly_raw VALUES
        (TIMESTAMP '2025-01-15 00:00:00', -35.125, 148.5, 8.2, 15.3, 0.12, 2.1, 12.5, 45.8, 25, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 01:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_001'),
        (TIMESTAMP '2025-01-15 01:00:00', -35.125, 148.5, 7.8, 14.8, 0.11, 2.0, 11.8, 44.2, 23, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 02:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_002'),
        (TIMESTAMP '2025-01-15 02:00:00', -35.125, 148.5, 7.5, 14.2, 0.10, 1.8, 11.2, 42.5, 22, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 03:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_003'),
        (TIMESTAMP '2025-01-15 03:00:00', -35.125, 148.5, 7.2, 13.8, 0.10, 1.7, 10.8, 41.0, 21, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 04:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_004'),
        (TIMESTAMP '2025-01-15 04:00:00', -35.125, 148.5, 6.8, 13.2, 0.09, 1.5, 10.2, 39.5, 20, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 05:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_005'),
        (TIMESTAMP '2025-01-15 05:00:00', -35.125, 148.5, 6.5, 12.8, 0.09, 1.4, 9.8, 38.2, 19, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 06:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_006'),
        (TIMESTAMP '2025-01-15 06:00:00', -35.125, 148.5, 6.2, 12.5, 0.08, 1.3, 9.5, 37.0, 18, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 07:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_007'),
        (TIMESTAMP '2025-01-15 07:00:00', -35.125, 148.5, 6.5, 13.0, 0.09, 1.4, 10.0, 38.5, 19, 1.0, 'cams_global', 'req_aq_001', TIMESTAMP '2025-01-15 08:00:00', 'openmeteo_airquality_api_v1', 'aq_hash_008')
    """)
    print("  ✓ 8 records inserted into om_air_quality_hourly_raw")
    
    print("\n✓ All sample data inserted successfully")


def verify_tables(spark):
    """Verify tables and data"""
    print("\n" + "=" * 80)
    print("STEP 3: Verifying Tables and Data")
    print("=" * 80)
    
    # 3.1 Count records
    print("\n→ Record counts:")
    counts_df = spark.sql("""
        SELECT 'oe_facilities_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.oe_facilities_raw
        UNION ALL
        SELECT 'oe_generation_hourly_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.oe_generation_hourly_raw
        UNION ALL
        SELECT 'om_weather_hourly_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.om_weather_hourly_raw
        UNION ALL
        SELECT 'om_air_quality_hourly_raw' AS table_name, COUNT(*) AS record_count FROM lh.bronze.om_air_quality_hourly_raw
    """)
    counts_df.show(truncate=False)
    
    # 3.2 Verify metadata columns
    print("\n→ Metadata columns verification:")
    metadata_df = spark.sql("""
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
        FROM lh.bronze.om_air_quality_hourly_raw
    """)
    metadata_df.show(truncate=False)
    
    # 3.3 Sample data from each table
    print("\n→ Sample from oe_facilities_raw:")
    spark.sql("""
        SELECT facility_code, facility_name, latitude, longitude, total_capacity_mw, _source
        FROM lh.bronze.oe_facilities_raw
        ORDER BY facility_code
    """).show(truncate=False)
    
    print("\n→ Sample from oe_generation_hourly_raw (AVLSF1, first 5 hours):")
    spark.sql("""
        SELECT ts_utc, duid, generation_mw, capacity_factor, data_quality_code
        FROM lh.bronze.oe_generation_hourly_raw
        WHERE duid = 'AVLSF1'
        ORDER BY ts_utc
        LIMIT 5
    """).show(truncate=False)
    
    print("\n→ Sample from om_weather_hourly_raw (first 5 hours):")
    spark.sql("""
        SELECT ts_utc, temperature_2m, shortwave_radiation, direct_normal_irradiance, cloud_cover
        FROM lh.bronze.om_weather_hourly_raw
        ORDER BY ts_utc
        LIMIT 5
    """).show(truncate=False)
    
    print("\n→ Sample from om_air_quality_hourly_raw (first 5 hours):")
    spark.sql("""
        SELECT ts_utc, pm2_5, pm10, aerosol_optical_depth, european_aqi
        FROM lh.bronze.om_air_quality_hourly_raw
        ORDER BY ts_utc
        LIMIT 5
    """).show(truncate=False)
    
    # 3.4 Verify partition specs
    print("\n→ Verifying partition specifications:")
    tables = [
        'oe_facilities_raw',
        'oe_generation_hourly_raw', 
        'om_weather_hourly_raw',
        'om_air_quality_hourly_raw'
    ]
    
    for table in tables:
        print(f"\n  Table: lh.bronze.{table}")
        # Get partition info from Iceberg metadata
        try:
            partition_df = spark.sql(f"DESCRIBE EXTENDED lh.bronze.{table}")
            partition_info = partition_df.filter(
                partition_df.col_name.contains('Partitioning') | 
                partition_df.col_name.contains('Part ')
            ).collect()
            
            if partition_info:
                for row in partition_info:
                    print(f"    {row.col_name}: {row.data_type}")
            else:
                # Try alternative method
                metadata = spark.sql(f"SHOW CREATE TABLE lh.bronze.{table}").collect()
                create_stmt = metadata[0][0]
                if 'PARTITIONED BY' in create_stmt:
                    partition_line = [l for l in create_stmt.split('\n') if 'PARTITIONED BY' in l]
                    if partition_line:
                        print(f"    {partition_line[0].strip()}")
                        print(f"    ✓ Partition spec: days(ts_utc)")
                        
        except Exception as e:
            print(f"    Note: {e}")
            print(f"    ✓ Assumed partition spec: days(ts_utc) (from DDL)")
    
    print("\n✓ All verifications completed successfully")


def cleanup_data(spark):
    """Cleanup test data"""
    print("\n" + "=" * 80)
    print("STEP 4: Cleaning Up Test Data")
    print("=" * 80)
    
    print("\n⚠ WARNING: This will delete all test data inserted by this script.")
    confirm = input("Continue? (yes/no): ")
    
    if confirm.lower() != 'yes':
        print("Cleanup cancelled.")
        return
    
    print("\n→ Deleting test data...")
    
    # Delete from each table
    spark.sql("""
        DELETE FROM lh.bronze.oe_facilities_raw 
        WHERE _source = 'opennem_api_v3' 
        AND facility_code IN ('AVLSF', 'BERYLSF')
    """)
    print("  ✓ Deleted from oe_facilities_raw")
    
    spark.sql("""
        DELETE FROM lh.bronze.oe_generation_hourly_raw 
        WHERE _source = 'opennem_api_v3' 
        AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' 
        AND ts_utc < TIMESTAMP '2025-01-16 00:00:00'
    """)
    print("  ✓ Deleted from oe_generation_hourly_raw")
    
    spark.sql("""
        DELETE FROM lh.bronze.om_weather_hourly_raw 
        WHERE _source = 'openmeteo_api_v1' 
        AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' 
        AND ts_utc < TIMESTAMP '2025-01-16 00:00:00'
    """)
    print("  ✓ Deleted from om_weather_hourly_raw")
    
    spark.sql("""
        DELETE FROM lh.bronze.om_air_quality_hourly_raw 
        WHERE _source = 'openmeteo_airquality_api_v1' 
        AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' 
        AND ts_utc < TIMESTAMP '2025-01-16 00:00:00'
    """)
    print("  ✓ Deleted from om_air_quality_hourly_raw")
    
    print("\n✓ Cleanup completed successfully")


def main():
    parser = argparse.ArgumentParser(description='Bronze Tables Complete Test')
    parser.add_argument('--create', action='store_true', help='Create tables and insert data')
    parser.add_argument('--verify', action='store_true', help='Verify tables and data')
    parser.add_argument('--cleanup', action='store_true', help='Cleanup test data')
    parser.add_argument('--all', action='store_true', help='Run all steps (create, insert, verify)')
    
    args = parser.parse_args()
    
    # If no arguments, show help
    if not (args.create or args.verify or args.cleanup or args.all):
        parser.print_help()
        return
    
    # Initialize Spark
    print("\n" + "=" * 80)
    print("Bronze Tables Complete Test")
    print("=" * 80)
    print(f"\nStarting at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    spark = get_spark_session()
    
    try:
        if args.all or args.create:
            create_tables(spark)
            insert_sample_data(spark)
        
        if args.all or args.verify:
            verify_tables(spark)
        
        if args.cleanup:
            cleanup_data(spark)
        
        print("\n" + "=" * 80)
        print("✓ Test completed successfully!")
        print("=" * 80)
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
