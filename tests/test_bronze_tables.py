"""
Bronze Layer Table Integration Test
====================================
Tests the creation and basic operations of all bronze layer tables:
- lh.bronze.oe_facilities_raw
- lh.bronze.oe_generation_hourly_raw
- lh.bronze.om_weather_hourly_raw
- lh.bronze.om_air_quality_hourly_raw

This script:
1. Creates all tables using DDL files
2. Inserts sample data for testing
3. Validates table structure and partitioning
4. Cleans up test data
"""

import os
import hashlib
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType


def get_spark_session():
    """Initialize Spark session with Iceberg support"""
    spark = SparkSession.builder \
        .appName("Bronze Layer Table Test") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lh.type", "rest") \
        .config("spark.sql.catalog.lh.uri", os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")) \
        .config("spark.sql.catalog.lh.warehouse", os.getenv("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse")) \
        .getOrCreate()
    return spark


def compute_hash(*fields):
    """Compute SHA256 hash for given fields"""
    content = "|".join(str(f) for f in fields)
    return hashlib.sha256(content.encode()).hexdigest()


def create_tables(spark):
    """Create all bronze tables using DDL files"""
    print("=" * 80)
    print("STEP 1: Creating Bronze Tables")
    print("=" * 80)
    
    ddl_files = [
        "sql/bronze/oe_facilities_raw.sql",
        "sql/bronze/oe_generation_hourly_raw.sql",
        "sql/bronze/om_weather_hourly_raw.sql",
        "sql/bronze/om_air_quality_hourly_raw.sql"
    ]
    
    for ddl_file in ddl_files:
        file_path = os.path.join("/home/pvlakehouse/dlh-pv", ddl_file)
        print(f"\n📄 Executing: {ddl_file}")
        
        with open(file_path, 'r') as f:
            ddl = f.read()
        
        spark.sql(ddl)
        table_name = ddl_file.split('/')[-1].replace('.sql', '')
        print(f"✅ Table lh.bronze.{table_name} created successfully")
    
    print("\n✅ All tables created successfully!\n")


def insert_sample_facilities(spark):
    """Insert sample facility data"""
    print("=" * 80)
    print("STEP 2: Inserting Sample Data - Facilities")
    print("=" * 80)
    
    now = datetime.utcnow()
    base_ts = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    facilities_data = [
        (
            "AVLSF", "Avonlie Solar Farm", -34.52, 146.23, "NEM", "NSW1",
            100.0, 95.0, 98.0, "solar_utility", 1, "operating",
            base_ts - timedelta(days=365), base_ts - timedelta(days=10),
            "AVLSF1",
            base_ts,
            now, "opennem_api_v1", compute_hash("AVLSF", base_ts)
        ),
        (
            "BERYLSF", "Beryl Solar Farm", -33.88, 150.68, "NEM", "NSW1",
            87.0, 87.0, 87.0, "solar_utility", 1, "operating",
            base_ts - timedelta(days=500), base_ts - timedelta(days=5),
            "BERYLSF1",
            base_ts,
            now, "opennem_api_v1", compute_hash("BERYLSF", base_ts)
        ),
        (
            "CLERMSF", "Clermont Solar Farm", -22.83, 147.64, "NEM", "QLD1",
            120.0, 115.0, 120.0, "solar_utility", 2, "operating",
            base_ts - timedelta(days=400), base_ts - timedelta(days=3),
            "CLERMSF1,CLERMSF2",
            base_ts,
            now, "opennem_api_v1", compute_hash("CLERMSF", base_ts)
        )
    ]
    
    columns = [
        "facility_code", "facility_name", "latitude", "longitude", "network_id", "network_region",
        "total_capacity_mw", "total_capacity_registered_mw", "total_capacity_maximum_mw",
        "fuel_technology", "unit_count", "operational_status",
        "facility_created_at", "facility_updated_at", "unit_codes",
        "ts_utc", "_ingest_time", "_source", "_hash"
    ]
    
    df = spark.createDataFrame(facilities_data, columns)
    df.writeTo("lh.bronze.oe_facilities_raw").append()
    
    count = spark.table("lh.bronze.oe_facilities_raw").count()
    print(f"✅ Inserted {count} facility records")
    
    # Show sample
    print("\n📊 Sample records:")
    spark.table("lh.bronze.oe_facilities_raw").select(
        "facility_code", "facility_name", "network_region", "total_capacity_mw", "ts_utc"
    ).show(truncate=False)


def insert_sample_generation(spark):
    """Insert sample generation data"""
    print("=" * 80)
    print("STEP 3: Inserting Sample Data - Generation")
    print("=" * 80)
    
    now = datetime.utcnow()
    base_ts = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    generation_data = []
    
    # Generate 24 hours of data for 3 facilities
    duids = ["AVLSF1", "BERYLSF1", "CLERMSF1"]
    capacities = [95.0, 87.0, 115.0]
    
    for hour in range(24):
        ts = base_ts + timedelta(hours=hour)
        
        for duid, capacity in zip(duids, capacities):
            # Simulate solar generation curve (peak at noon)
            if 6 <= hour <= 18:  # Daylight hours
                peak_factor = 1 - abs(12 - hour) / 6  # Peak at noon
                generation = capacity * 0.8 * peak_factor
            else:
                generation = 0.0
            
            capacity_factor = generation / capacity if capacity > 0 else 0.0
            
            generation_data.append((
                ts, duid, generation, capacity_factor,
                "ACTUAL", "OPENNEM",
                now, "opennem_api_v1", compute_hash(duid, ts)
            ))
    
    columns = [
        "ts_utc", "duid", "generation_mw", "capacity_factor",
        "data_quality_code", "data_source",
        "_ingest_time", "_source", "_hash"
    ]
    
    df = spark.createDataFrame(generation_data, columns)
    df.writeTo("lh.bronze.oe_generation_hourly_raw").append()
    
    count = spark.table("lh.bronze.oe_generation_hourly_raw").count()
    print(f"✅ Inserted {count} generation records")
    
    # Show sample
    print("\n📊 Sample records (midday peak):")
    spark.sql("""
        SELECT ts_utc, duid, 
               ROUND(generation_mw, 2) as generation_mw, 
               ROUND(capacity_factor, 2) as capacity_factor
        FROM lh.bronze.oe_generation_hourly_raw
        WHERE HOUR(ts_utc) BETWEEN 11 AND 13
        ORDER BY ts_utc, duid
        LIMIT 10
    """).show(truncate=False)


def insert_sample_weather(spark):
    """Insert sample weather data"""
    print("=" * 80)
    print("STEP 4: Inserting Sample Data - Weather")
    print("=" * 80)
    
    now = datetime.utcnow()
    base_ts = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    weather_data = []
    
    # Weather for 3 facility locations, 24 hours
    locations = [
        (-34.5, 146.25),  # AVLSF (rounded to 0.25° grid)
        (-33.75, 150.75),  # BERYLSF
        (-22.75, 147.75)   # CLERMSF
    ]
    
    for hour in range(24):
        ts = base_ts + timedelta(hours=hour)
        
        for lat, lon in locations:
            # Simulate weather patterns
            temp = 20 + 5 * (1 - abs(12 - hour) / 12)  # Peak temp at noon
            humidity = 60 - 10 * (1 - abs(12 - hour) / 12)
            
            if 6 <= hour <= 18:
                shortwave = 800 * (1 - abs(12 - hour) / 6)  # Peak at noon
                dni = 600 * (1 - abs(12 - hour) / 6)
                diffuse = 200 * (1 - abs(12 - hour) / 6)
                cloud_cover = 20.0
            else:
                shortwave = 0.0
                dni = 0.0
                diffuse = 0.0
                cloud_cover = 10.0
            
            weather_data.append((
                ts, lat, lon,
                temp, humidity, 1013.25,
                5.0, 180.0,
                shortwave, dni, diffuse,
                cloud_cover, 10.0,
                0.0,
                1.0, "ERA5",
                f"req_{ts.strftime('%Y%m%d%H')}",
                now, "openmeteo_api_v1", compute_hash(lat, lon, ts)
            ))
    
    columns = [
        "ts_utc", "latitude", "longitude",
        "temperature_2m", "relative_humidity_2m", "pressure_msl",
        "wind_speed_10m", "wind_direction_10m",
        "shortwave_radiation", "direct_normal_irradiance", "diffuse_radiation",
        "cloud_cover", "cloud_cover_low",
        "precipitation",
        "data_completeness_score", "api_source_model",
        "api_request_id",
        "_ingest_time", "_source", "_hash"
    ]
    
    df = spark.createDataFrame(weather_data, columns)
    df.writeTo("lh.bronze.om_weather_hourly_raw").append()
    
    count = spark.table("lh.bronze.om_weather_hourly_raw").count()
    print(f"✅ Inserted {count} weather records")
    
    # Show sample
    print("\n📊 Sample records (solar radiation at noon):")
    spark.sql("""
        SELECT ts_utc, latitude, longitude,
               ROUND(temperature_2m, 1) as temp_c,
               ROUND(shortwave_radiation, 1) as ghi,
               ROUND(direct_normal_irradiance, 1) as dni
        FROM lh.bronze.om_weather_hourly_raw
        WHERE HOUR(ts_utc) = 12
        ORDER BY ts_utc, latitude
    """).show(truncate=False)


def insert_sample_air_quality(spark):
    """Insert sample air quality data"""
    print("=" * 80)
    print("STEP 5: Inserting Sample Data - Air Quality")
    print("=" * 80)
    
    now = datetime.utcnow()
    base_ts = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    air_quality_data = []
    
    # Air quality for same 3 locations, 24 hours
    locations = [
        (-34.5, 146.25),
        (-33.75, 150.75),
        (-22.75, 147.75)
    ]
    
    for hour in range(24):
        ts = base_ts + timedelta(hours=hour)
        
        for lat, lon in locations:
            # Simulate air quality patterns
            pm2_5 = 5.0 + 2.0 * (hour / 24)  # Slight increase during day
            pm10 = 10.0 + 3.0 * (hour / 24)
            aod = 0.05 + 0.02 * (hour / 24)
            
            air_quality_data.append((
                ts, lat, lon,
                pm2_5, pm10,
                aod, 2.0,
                15.0, 50.0,
                25,
                1.0, "cams_global",
                f"req_{ts.strftime('%Y%m%d%H')}",
                now, "openmeteo_cams_v1", compute_hash(lat, lon, ts)
            ))
    
    columns = [
        "ts_utc", "latitude", "longitude",
        "pm2_5", "pm10",
        "aerosol_optical_depth", "dust",
        "nitrogen_dioxide", "ozone",
        "european_aqi",
        "data_completeness_score", "cams_domain",
        "api_request_id",
        "_ingest_time", "_source", "_hash"
    ]
    
    df = spark.createDataFrame(air_quality_data, columns)
    df.writeTo("lh.bronze.om_air_quality_hourly_raw").append()
    
    count = spark.table("lh.bronze.om_air_quality_hourly_raw").count()
    print(f"✅ Inserted {count} air quality records")
    
    # Show sample
    print("\n📊 Sample records:")
    spark.sql("""
        SELECT ts_utc, latitude, longitude,
               ROUND(pm2_5, 2) as pm2_5,
               ROUND(aerosol_optical_depth, 3) as aod,
               european_aqi
        FROM lh.bronze.om_air_quality_hourly_raw
        WHERE HOUR(ts_utc) IN (0, 6, 12, 18)
        ORDER BY ts_utc, latitude
        LIMIT 12
    """).show(truncate=False)


def verify_partitioning(spark):
    """Verify partition specs for all tables"""
    print("=" * 80)
    print("STEP 6: Verifying Partition Specifications")
    print("=" * 80)
    
    tables = [
        "lh.bronze.oe_facilities_raw",
        "lh.bronze.oe_generation_hourly_raw",
        "lh.bronze.om_weather_hourly_raw",
        "lh.bronze.om_air_quality_hourly_raw"
    ]
    
    for table in tables:
        print(f"\n📋 Table: {table}")
        
        # Get partition spec
        metadata = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
        
        # Check for partition info
        partition_found = False
        for row in metadata:
            if "Partitioning" in str(row) or "Part " in str(row):
                print(f"   ✅ {row}")
                partition_found = True
        
        if not partition_found:
            # Try alternate method
            print("   ℹ️  Checking partitioning using table metadata...")
            try:
                partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
                if partitions:
                    print(f"   ✅ Partitions exist: {len(partitions)} partition(s)")
                    for p in partitions[:3]:  # Show first 3
                        print(f"      - {p}")
            except Exception as e:
                print(f"   ℹ️  Manual check: {str(e)[:100]}")
        
        # Count records
        count = spark.table(table).count()
        print(f"   📊 Record count: {count}")


def cleanup_test_data(spark):
    """Clean up all test data"""
    print("\n" + "=" * 80)
    print("STEP 7: Cleanup Test Data")
    print("=" * 80)
    
    tables = [
        "lh.bronze.oe_facilities_raw",
        "lh.bronze.oe_generation_hourly_raw",
        "lh.bronze.om_weather_hourly_raw",
        "lh.bronze.om_air_quality_hourly_raw"
    ]
    
    print("\n⚠️  CLEANUP OPTIONS:")
    print("1. Drop all tables (complete cleanup)")
    print("2. Truncate tables (keep schema)")
    print("3. Skip cleanup (keep data for manual inspection)")
    
    choice = input("\nEnter your choice (1/2/3): ").strip()
    
    if choice == "1":
        print("\n🗑️  Dropping all tables...")
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"   ✅ Dropped {table}")
        print("\n✅ All tables dropped successfully!")
        
    elif choice == "2":
        print("\n🧹 Truncating all tables...")
        for table in tables:
            spark.sql(f"TRUNCATE TABLE {table}")
            print(f"   ✅ Truncated {table}")
        print("\n✅ All tables truncated successfully!")
        
    else:
        print("\n⏭️  Skipping cleanup - data retained for inspection")
        print("\n💡 To manually cleanup later, run:")
        for table in tables:
            print(f"   spark.sql('DROP TABLE {table}')")


def main():
    """Main test execution"""
    print("\n" + "=" * 80)
    print("BRONZE LAYER TABLE INTEGRATION TEST")
    print("=" * 80)
    print(f"Test started at: {datetime.utcnow().isoformat()}")
    print("=" * 80 + "\n")
    
    try:
        spark = get_spark_session()
        
        # Create tables
        create_tables(spark)
        
        # Insert sample data
        insert_sample_facilities(spark)
        insert_sample_generation(spark)
        insert_sample_weather(spark)
        insert_sample_air_quality(spark)
        
        # Verify partitioning
        verify_partitioning(spark)
        
        # Cleanup
        cleanup_test_data(spark)
        
        print("\n" + "=" * 80)
        print("✅ TEST COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ TEST FAILED!")
        print("=" * 80)
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
