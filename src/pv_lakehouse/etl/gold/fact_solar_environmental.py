"""Gold loader for fact_solar_environmental."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import (
    broadcast_small_dim,
    build_aqi_lookup,
    build_hourly_fact_base,
    compute_date_key,
    dec,
    is_empty,
    require_sources,
)


class GoldFactSolarEnvironmentalLoader(BaseGoldLoader):
    """
    Produce Gold fact rows combining solar energy production with environmental conditions.
    
    This fact table integrates:
    - Energy production metrics (from Silver hourly_energy)
    - Weather conditions (from Silver hourly_weather)
    - Air quality metrics (from Silver hourly_air_quality)
    
    Grain: 1 row = 1 hour at 1 facility
    """

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="date_hour",  # Filter by data hour, not update time
            required_columns=[
                "facility_code",
                "facility_name",
                "network_code",
                "network_region",
                "date_hour",
                "energy_mwh",
                "power_avg_mw",
                "intervals_count",
                "is_valid",
                "quality_flag",
                "completeness_pct",
            ],
        ),
        "hourly_weather": SourceTableConfig(
            table_name="lh.silver.clean_hourly_weather",
            timestamp_column="date_hour",  # Filter by data hour, not update time
            required_columns=[
                "facility_code",
                "date_hour",
                "shortwave_radiation",
                "direct_radiation",
                "diffuse_radiation",
                "direct_normal_irradiance",
                "temperature_2m",
                "dew_point_2m",
                "cloud_cover",
                "cloud_cover_low",
                "cloud_cover_mid",
                "cloud_cover_high",
                "precipitation",
                "sunshine_duration",
                "wind_speed_10m",
                "wind_direction_10m",
                "wind_gusts_10m",
                "pressure_msl",
            ],
        ),
        "hourly_air_quality": SourceTableConfig(
            table_name="lh.silver.clean_hourly_air_quality",
            timestamp_column="date_hour",  # Filter by data hour, not update time
            required_columns=[
                "facility_code",
                "date_hour",
                "pm2_5",
                "pm10",
                "dust",
                "nitrogen_dioxide",
                "ozone",
                "sulphur_dioxide",
                "carbon_monoxide",
                "uv_index",
                "uv_index_clear_sky",
                "aqi_value",
            ],
        ),
        "dim_facility": SourceTableConfig(
            table_name="lh.gold.dim_facility",
            required_columns=["facility_key", "facility_code"],
        ),
        "dim_date": SourceTableConfig(
            table_name="lh.gold.dim_date",
            required_columns=["date_key", "full_date"],
        ),
        "dim_time": SourceTableConfig(
            table_name="lh.gold.dim_time",
            required_columns=["time_key", "hour"],
        ),
        "dim_aqi_category": SourceTableConfig(
            table_name="lh.gold.dim_aqi_category",
            required_columns=["aqi_category_key", "aqi_range_min", "aqi_range_max"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_solar_environmental": GoldTableConfig(
            iceberg_table="lh.gold.fact_solar_environmental",
            s3_base_path="s3a://lakehouse/gold/fact_solar_environmental",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        """Transform Silver hourly data into Gold fact table with environmental context."""
        
        # Validate required dimension tables exist
        required = require_sources(
            {
                "dim_facility": sources.get("dim_facility"),
                "dim_date": sources.get("dim_date"),
                "dim_time": sources.get("dim_time"),
                "dim_aqi_category": sources.get("dim_aqi_category"),
            },
            {
                "dim_facility": "fact_solar_environmental",
                "dim_date": "fact_solar_environmental",
                "dim_time": "fact_solar_environmental",
                "dim_aqi_category": "fact_solar_environmental",
            },
        )
        
        dim_facility = broadcast_small_dim(required["dim_facility"])
        dim_date = broadcast_small_dim(required["dim_date"])
        dim_time = broadcast_small_dim(required["dim_time"])
        dim_aqi_category = required["dim_aqi_category"]  # Not broadcasted for cross join

        # Start with energy data as the base (required)
        hourly_energy = sources.get("hourly_energy")
        if is_empty(hourly_energy):
            return None

        # Build base fact structure with date_key and time_key
        base = build_hourly_fact_base(hourly_energy, timestamp_column="date_hour")
        if is_empty(base):
            return None

        # Build AQI lookup with category keys
        hourly_air_quality = sources.get("hourly_air_quality")
        aqi_lookup = build_aqi_lookup(hourly_air_quality, dim_aqi_category)

        # Get weather data
        hourly_weather = sources.get("hourly_weather")

        # Join with dimension tables to get surrogate keys
        # CRITICAL: All joins on (facility_code, date_hour_ts_utc) to maintain hourly grain
        
        # 0) Chuẩn hóa timestamp (UTC → local nếu cần)
        # date_hour is already timestamp(6) with time zone from Silver, just alias it
        fact = base.withColumn("date_hour_ts_utc", F.col("date_hour"))
        # Nếu toàn bộ phân tích dùng UTC, dùng date_hour_ts_utc luôn
        # Nếu muốn local (ví dụ Asia/Ho_Chi_Minh), bật dòng sau:
        # fact = fact.withColumn("date_hour_ts_local", F.from_utc_timestamp("date_hour_ts_utc", F.lit("Asia/Ho_Chi_Minh")))
        # rồi thay mọi join/time-extract dùng date_hour_ts_local
        
        # 1. Join with facility dimension (broadcast join - small dimension)
        fact = fact.join(dim_facility, on="facility_code", how="left")

        # 2) Join dim_date bằng ngày local/UTC nhất quán
        # Rename dim_date columns to avoid ambiguity with base table
        dim_date_selected = dim_date.select(
            F.col("full_date").alias("dim_full_date"),
            F.col("date_key").alias("dim_date_key"),
            F.col("year"),
            F.col("month"),
            F.col("day_of_month"),
            F.col("day_of_week"),
            F.col("week"),
            F.col("quarter"),
            F.col("is_weekend"),
            F.col("season"),
        )
        
        fact = fact.join(
            dim_date_selected,
            F.to_date(F.col("date_hour_ts_utc")) == F.col("dim_full_date"),  # hoặc ts_local nếu dùng local
            how="left"
        )
        
        # Replace date_key with the one from dimension (more authoritative)
        fact = fact.drop("date_key").withColumn("date_key", F.col("dim_date_key"))

        # 3. Join with time dimension (broadcast join - 24 rows)
        # Rename time_key from dim_time to avoid collision
        dim_time_selected = dim_time.select(
            F.col("time_key").alias("dim_time_key"),
            F.col("hour"),
            F.col("minute"),
            F.col("is_peak_hour"),
            F.col("time_of_day"),
        )
        
        fact = fact.join(
            dim_time_selected,
            F.hour(F.col("date_hour_ts_utc")) == F.col("hour"),
            how="left"
        )
        
        # Replace time_key with the one from dimension
        fact = fact.drop("time_key").withColumn("time_key", F.col("dim_time_key"))

        # 4. Join with weather data (left join to preserve all energy records)
        if not is_empty(hourly_weather):
            # 3) WEATHER: tính RH và chuyển đổi đơn vị đúng
            # Prepare weather data with consistent timestamp
            # date_hour is already timestamp(6) with time zone, just alias it
            weather = hourly_weather.withColumn(
                "date_hour_ts_utc", 
                F.col("date_hour")
            )
            
            # Calculate relative humidity from dew point and temperature
            # Formula: RH = 100 * (exp((17.625*TD)/(243.04+TD))/exp((17.625*T)/(243.04+T)))
            # where TD is dew point and T is temperature in Celsius
            # Clamp to [0, 100]
            weather = weather.withColumn(
                "humidity_2m_raw",
                F.when(
                    F.col("dew_point_2m").isNotNull() & F.col("temperature_2m").isNotNull(),
                    100.0 * F.exp((F.lit(17.625)*F.col("dew_point_2m"))/(F.lit(243.04)+F.col("dew_point_2m"))) /
                    F.exp((F.lit(17.625)*F.col("temperature_2m"))/(F.lit(243.04)+F.col("temperature_2m")))
                ).otherwise(F.lit(None))
            ).withColumn(
                "humidity_2m",
                F.when(F.col("humidity_2m_raw") < 0, 0.0).when(F.col("humidity_2m_raw") > 100, 100.0).otherwise(F.col("humidity_2m_raw"))
            ).drop("humidity_2m_raw")
            
            # >>> JOIN
            fact = fact.join(
                weather.select(
                    "facility_code",
                    "date_hour_ts_utc",
                    "shortwave_radiation",
                    "direct_radiation",
                    "diffuse_radiation",
                    "direct_normal_irradiance",
                    "temperature_2m",
                    "dew_point_2m",
                    "humidity_2m",
                    "cloud_cover",
                    "cloud_cover_low",
                    "cloud_cover_mid",
                    "cloud_cover_high",
                    "precipitation",
                    "sunshine_duration",
                    "wind_speed_10m",
                    "wind_direction_10m",
                    "wind_gusts_10m",
                    "pressure_msl",
                ),
                on=["facility_code", "date_hour_ts_utc"],
                how="left"
            )

        # 5. Join with AQI lookup (left join to preserve all energy records)
        if not is_empty(aqi_lookup):
            # 4) AQI lookup giữ nguyên join theo UTC
            aqi = aqi_lookup.select(
                "facility_code", F.col("date_hour").cast("timestamp").alias("date_hour_ts_utc"),
                "aqi_category_key","pm2_5","pm10","dust","nitrogen_dioxide","ozone",
                "sulphur_dioxide","carbon_monoxide","uv_index","uv_index_clear_sky","aqi_value"
            )
            fact = fact.join(aqi, on=["facility_code","date_hour_ts_utc"], how="left")

        # Calculate data quality metrics
        # Completeness based on presence of energy, weather, and air quality data
        # 5) Completeness / Validity giữ nguyên logic của bạn
        fact = fact.withColumn(
            "has_energy",
            F.col("energy_mwh").isNotNull()
        )
        fact = fact.withColumn(
            "has_weather",
            F.col("shortwave_radiation").isNotNull()
        )
        fact = fact.withColumn(
            "has_air_quality",
            F.col("pm2_5").isNotNull()
        )
        
        # Completeness percentage: 33.33% per data source
        fact = fact.withColumn(
            "completeness_pct",
            (
                F.when(F.col("has_energy"), F.lit(33.33)).otherwise(F.lit(0.0)) +
                F.when(F.col("has_weather"), F.lit(33.33)).otherwise(F.lit(0.0)) +
                F.when(F.col("has_air_quality"), F.lit(33.34)).otherwise(F.lit(0.0))
            ).cast(dec(5, 2))
        )

        # Validation: Record is valid if energy data is valid AND present
        fact = fact.withColumn(
            "is_valid",
            F.col("has_energy") & 
            (F.col("energy_mwh") >= F.lit(0.0)) &
            (F.col("power_avg_mw").isNull() | (F.col("power_avg_mw") >= F.lit(0.0)))
        )

        # Quality flag based on completeness and validity
        fact = fact.withColumn(
            "quality_flag",
            F.when(~F.col("is_valid"), F.lit("BAD"))
            .when(F.col("completeness_pct") >= F.lit(90.0), F.lit("GOOD"))
            .when(F.col("completeness_pct") >= F.lit(50.0), F.lit("WARNING"))
            .otherwise(F.lit("BAD"))
        )

        # 6) >>> SỬA QUAN TRỌNG: chuyển W/m² (mean 1h) → kWh/m²·h
        fact = fact.withColumn(
            "irr_kwh_m2_hour",
            F.when(F.col("shortwave_radiation").isNotNull(), F.col("shortwave_radiation") / F.lit(1000.0))
             .otherwise(F.lit(None))
        )

        # 7) Thêm trợ giúp BI: giờ nắng (h) và mẫu số PR đã cân công suất
        # cần cột total_capacity_mw từ dim_facility đã join trước đó
        fact = fact.withColumn("sunshine_hours",
            F.when(F.col("sunshine_duration").isNotNull(), F.col("sunshine_duration")/F.lit(3600.0))
             .otherwise(F.lit(None))
        )
        fact = fact.withColumn("yr_weighted_kwh",
            F.when(F.col("irr_kwh_m2_hour").isNotNull() & F.col("total_capacity_mw").isNotNull(),
                   F.col("irr_kwh_m2_hour") * F.col("total_capacity_mw") * F.lit(1000.0))
             .otherwise(F.lit(None))
        )

        # Add audit timestamps
        fact = fact.withColumn("created_at", F.current_timestamp())
        fact = fact.withColumn("updated_at", F.current_timestamp())

        # 8) Select cuối cùng: thêm sunshine_hours & yr_weighted_kwh
        result = fact.select(
            # Keys
            "facility_key",
            "date_key",
            "time_key",
            "aqi_category_key",
            
            # Energy metrics
            F.col("energy_mwh").cast(dec(12, 6)).alias("energy_mwh"),
            F.col("power_avg_mw").cast(dec(12, 6)).alias("power_avg_mw"),
            "intervals_count",
            
            # Weather metrics
            F.col("shortwave_radiation").cast(dec(10, 4)).alias("shortwave_radiation"),
            F.col("direct_radiation").cast(dec(10, 4)).alias("direct_radiation"),
            F.col("diffuse_radiation").cast(dec(10, 4)).alias("diffuse_radiation"),
            F.col("direct_normal_irradiance").cast(dec(10, 4)).alias("direct_normal_irradiance"),
            F.col("irr_kwh_m2_hour").cast(dec(10, 6)).alias("irr_kwh_m2_hour"),
            F.col("sunshine_hours").cast(dec(6, 3)).alias("sunshine_hours"),
            "temperature_2m",
            "dew_point_2m",
            "humidity_2m",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "precipitation",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "pressure_msl",
            
            # Air quality metrics
            "pm2_5",
            "pm10",
            "dust",
            "nitrogen_dioxide",
            "ozone",
            "sulphur_dioxide",
            "carbon_monoxide",
            "uv_index",
            "uv_index_clear_sky",
            "aqi_value",
            
            # Data quality
            "is_valid",
            "quality_flag",
            "completeness_pct",
            F.col("yr_weighted_kwh").cast(dec(16, 6)).alias("yr_weighted_kwh"),
            "created_at",
            "updated_at",
        )

        if is_empty(result):
            return None

        return {"fact_solar_environmental": result}


__all__ = ["GoldFactSolarEnvironmentalLoader"]
