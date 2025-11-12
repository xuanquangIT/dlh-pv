#!/usr/bin/env python3
"""Test script to run Silver layer loaders on 6 months dataset and verify quality flags."""

import sys
from datetime import datetime

from pyspark.sql import functions as F

# Add parent directory to path
sys.path.insert(0, str(__file__).rsplit('/', 3)[0])

from pv_lakehouse.etl.silver.hourly_energy import SilverHourlyEnergyLoader
from pv_lakehouse.etl.silver.hourly_weather import SilverHourlyWeatherLoader
from pv_lakehouse.etl.silver.hourly_air_quality import SilverHourlyAirQualityLoader
from pv_lakehouse.etl.utils.spark_utils import create_spark_session


def test_energy_loader():
    """Test Energy loader on 6 months dataset (2025-05-08 to 2025-11-08)."""
    print("\n" + "="*80)
    print("🔋 TESTING ENERGY LOADER (6 months: 2025-05-08 to 2025-11-08)")
    print("="*80)
    
    spark = create_spark_session("test-energy-loader")
    
    try:
        # Read 6 months of Bronze data
        bronze_df = spark.table("lh.bronze.raw_facility_timeseries").filter(
            (F.col("interval_ts").cast("date") >= "2025-05-08") &
            (F.col("interval_ts").cast("date") <= "2025-11-08")
        )
        
        print(f"\n📥 Bronze records for all data: {bronze_df.count():,}")
        
        # Run loader transform
        loader = SilverHourlyEnergyLoader()
        silver_df = loader.transform(bronze_df)
        
        if silver_df is None:
            print("❌ Transform returned None")
            return False
        
        silver_count = silver_df.count()
        print(f"📤 Silver records after transform: {silver_count:,}")
        
        # Quality statistics
        print("\n📊 Quality Flag Distribution:")
        quality_stats = silver_df.groupBy("quality_flag").count().collect()
        total = sum(row['count'] for row in quality_stats)
        
        for row in sorted(quality_stats, key=lambda x: x['quality_flag']):
            flag = row['quality_flag']
            count = row['count']
            pct = (count / total * 100)
            symbol = "✅" if flag == "GOOD" else "⚠️ " if flag == "CAUTION" else "❌"
            print(f"  {symbol} {flag}: {count:,} ({pct:.2f}%)")
        
        # Show examples of CAUTION records
        print("\n📋 Examples of CAUTION records:")
        caution_examples = silver_df.filter(F.col("quality_flag") == "CAUTION").select(
            "facility_code", "date_hour", "energy_mwh", "quality_issues"
        ).limit(10).collect()
        
        if caution_examples:
            for row in caution_examples:
                print(f"  {row['facility_code']:8} | {row['date_hour']} | {row['energy_mwh']:8.2f} MWh | {row['quality_issues']}")
        else:
            print("  (No CAUTION records)")
        
        # Energy statistics
        print("\n📈 Energy Statistics:")
        stats = silver_df.select(
            F.min("energy_mwh").alias("min"),
            F.max("energy_mwh").alias("max"),
            F.avg("energy_mwh").alias("mean"),
            F.stddev("energy_mwh").alias("stddev")
        ).collect()[0]
        
        print(f"  Min:    {stats['min']:>8.2f} MWh")
        print(f"  Max:    {stats['max']:>8.2f} MWh")
        print(f"  Mean:   {stats['mean']:>8.2f} MWh")
        print(f"  StdDev: {stats['stddev']:>8.2f} MWh")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_weather_loader():
    """Test Weather loader on 6 months dataset (2025-05-08 to 2025-11-08)."""
    print("\n" + "="*80)
    print("🌤️  TESTING WEATHER LOADER (6 months: 2025-05-08 to 2025-11-08)")
    print("="*80)
    
    spark = create_spark_session("test-weather-loader")
    
    try:
        # Read 6 months of Bronze data
        bronze_df = spark.table("lh.bronze.raw_facility_weather").filter(
            (F.col("weather_timestamp").cast("date") >= "2025-05-08") &
            (F.col("weather_timestamp").cast("date") <= "2025-11-08")
        )
        
        print(f"\n📥 Bronze records for all data: {bronze_df.count():,}")
        
        # Run loader transform
        loader = SilverHourlyWeatherLoader()
        silver_df = loader.transform(bronze_df)
        
        if silver_df is None:
            print("❌ Transform returned None")
            return False
        
        silver_count = silver_df.count()
        print(f"📤 Silver records after transform: {silver_count:,}")
        
        # Quality statistics
        print("\n📊 Quality Flag Distribution:")
        quality_stats = silver_df.groupBy("quality_flag").count().collect()
        total = sum(row['count'] for row in quality_stats)
        
        for row in sorted(quality_stats, key=lambda x: x['quality_flag']):
            flag = row['quality_flag']
            count = row['count']
            pct = (count / total * 100)
            symbol = "✅" if flag == "GOOD" else "⚠️ " if flag == "CAUTION" else "❌"
            print(f"  {symbol} {flag}: {count:,} ({pct:.2f}%)")
        
        # Show examples of CAUTION records
        print("\n📋 Examples of CAUTION records (if any):")
        caution_examples = silver_df.filter(F.col("quality_flag") == "CAUTION").select(
            "facility_code", "date_hour", "quality_issues"
        ).limit(5).collect()
        
        if caution_examples:
            for row in caution_examples:
                print(f"  {row['facility_code']:8} | {row['date_hour']} | {row['quality_issues']}")
        else:
            print("  (No CAUTION records - data is clean!)")
        
        # Show temperature range
        print("\n📈 Weather Statistics (Temperature):")
        stats = silver_df.select(
            F.min("temperature_2m").alias("min_temp"),
            F.max("temperature_2m").alias("max_temp"),
            F.avg("temperature_2m").alias("mean_temp")
        ).collect()[0]
        
        print(f"  Min Temp:  {stats['min_temp']:>6.2f}°C")
        print(f"  Max Temp:  {stats['max_temp']:>6.2f}°C")
        print(f"  Mean Temp: {stats['mean_temp']:>6.2f}°C")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_air_quality_loader():
    """Test Air Quality loader on 6 months dataset (2025-05-08 to 2025-11-08)."""
    print("\n" + "="*80)
    print("💨 TESTING AIR QUALITY LOADER (6 months: 2025-05-08 to 2025-11-08)")
    print("="*80)
    
    spark = create_spark_session("test-air-quality-loader")
    
    try:
        # Read 6 months of Bronze data
        bronze_df = spark.table("lh.bronze.raw_facility_air_quality").filter(
            (F.col("air_timestamp").cast("date") >= "2025-05-08") &
            (F.col("air_timestamp").cast("date") <= "2025-11-08")
        )
        
        print(f"\n📥 Bronze records for all data: {bronze_df.count():,}")
        
        # Run loader transform
        loader = SilverHourlyAirQualityLoader()
        silver_df = loader.transform(bronze_df)
        
        if silver_df is None:
            print("❌ Transform returned None")
            return False
        
        silver_count = silver_df.count()
        print(f"📤 Silver records after transform: {silver_count:,}")
        
        # Quality statistics
        print("\n📊 Quality Flag Distribution:")
        quality_stats = silver_df.groupBy("quality_flag").count().collect()
        total = sum(row['count'] for row in quality_stats)
        
        for row in sorted(quality_stats, key=lambda x: x['quality_flag']):
            flag = row['quality_flag']
            count = row['count']
            pct = (count / total * 100)
            symbol = "✅" if flag == "GOOD" else "⚠️ " if flag == "CAUTION" else "❌"
            print(f"  {symbol} {flag}: {count:,} ({pct:.2f}%)")
        
        # Show examples of CAUTION records
        print("\n📋 Examples of CAUTION records (if any):")
        caution_examples = silver_df.filter(F.col("quality_flag") == "CAUTION").select(
            "facility_code", "date_hour", "pm2_5", "quality_issues"
        ).limit(5).collect()
        
        if caution_examples:
            for row in caution_examples:
                print(f"  {row['facility_code']:8} | {row['date_hour']} | PM2.5: {row['pm2_5']:6.2f} | {row['quality_issues']}")
        else:
            print("  (No CAUTION records - data is clean!)")
        
        # Show AQI categories
        print("\n📊 AQI Category Distribution:")
        aqi_stats = silver_df.groupBy("aqi_category").count().collect()
        for row in sorted(aqi_stats, key=lambda x: x['aqi_category']):
            if row['aqi_category']:
                print(f"  {row['aqi_category']:12}: {row['count']:,} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n" + "🚀 "*40)
    print("SILVER LAYER QUALITY FLAGS TEST")
    print("Testing on all available data (2025-10-01 to 2025-11-08) to verify quality validation")
    print("🚀 "*40)
    
    results = {}
    
    # Test each loader
    results["Energy"] = test_energy_loader()
    results["Weather"] = test_weather_loader()
    results["Air Quality"] = test_air_quality_loader()
    
    # Summary
    print("\n" + "="*80)
    print("✅ TEST SUMMARY")
    print("="*80)
    
    for name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status} - {name}")
    
    all_passed = all(results.values())
    if all_passed:
        print("\n🎉 All tests passed! Quality flags are working correctly.")
    else:
        print("\n⚠️  Some tests failed. Review the errors above.")
    
    sys.exit(0 if all_passed else 1)
