"""Silver loader producing clean_hourly_air_quality."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions


class SilverHourlyAirQualityLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_air_quality"
    silver_table = "lh.silver.clean_hourly_air_quality"
    timestamp_column = "air_timestamp"
    partition_cols = ("date_hour",)

    def __init__(self, options: Optional[LoadOptions] = None) -> None:
        if options is None:
            options = LoadOptions()
        else:
            options.target_file_size_mb = min(options.target_file_size_mb, self.DEFAULT_TARGET_FILE_SIZE_MB)
            options.max_records_per_file = min(options.max_records_per_file, self.DEFAULT_MAX_RECORDS_PER_FILE)
        super().__init__(options)

    def run(self) -> int:
        """Process bronze air quality data in 7-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=7)

    # Physical bounds for air quality variables (μg/m³ or ppb equivalent)
    # Based on WHO standards and typical air pollution levels
    _numeric_columns = {
        # Particulate Matter (PM) - μg/m³
        "pm2_5": (0.0, 500.0),              # PM 2.5: WHO extreme = 250+, practical max = 500
        "pm10": (0.0, 500.0),               # PM 10: WHO extreme = 1,200, practical cap = 500
        "dust": (0.0, 500.0),               # Total dust/TSP: practical max = 500
        
        # Gaseous Pollutants - ppb (parts per billion) or μg/m³
        "nitrogen_dioxide": (0.0, 500.0),   # NO₂: WHO 1-hr = 200 μg/m³ (~100 ppb), cap = 500
        "ozone": (0.0, 500.0),              # O₃: WHO 8-hr = 100 μg/m³ (~50 ppb), cap = 500
        "sulphur_dioxide": (0.0, 500.0),    # SO₂: EPA 1-hr = 196 ppb (~500 μg/m³), cap = 500
        "carbon_monoxide": (0.0, 1000.0),   # CO: ppb units, typical clean air = 100-500, dust storms = 500-1000
        
        # UV Index - dimensionless (0-15 is typical)
        "uv_index": (0.0, 15.0),            # 0-11 typical (outdoor), extreme = 15+
        "uv_index_clear_sky": (0.0, 15.0),  # Clear-sky theoretical maximum
    }

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        if bronze_df is None or not bronze_df.columns:
            return None

        required_columns = {
            "facility_code",
            "facility_name",
            "air_timestamp",
        }
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze air-quality source: {sorted(missing)}")

        # Bronze timestamps are already in facility's local timezone
        prepared_base = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.col("air_timestamp").cast("timestamp").alias("timestamp_local"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("air_timestamp").isNotNull())
        )
        
        # No conversion needed - aggregate by local time
        prepared = (
            prepared_base
            .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_local")))
            .withColumn("date", F.to_date(F.col("timestamp_local")))
        )

        # Round numeric columns - build all columns in single SELECT (no intermediate withColumn!)
        select_exprs = [
            "facility_code", "facility_name", "timestamp_local", "date_hour", "date"
        ]
        for column in self._numeric_columns.keys():
            if column in prepared.columns:
                select_exprs.append(F.round(F.col(column), 4).alias(column))
            else:
                select_exprs.append(F.lit(None).alias(column))
        
        # Pre-compute AQI value
        aqi_value = self._aqi_from_pm25(F.col("pm2_5"))
        aqi_value_rounded = F.round(aqi_value).cast("int")
        
        # Pre-compute all validation checks
        is_valid_bounds = F.lit(True)
        bound_issues_list = []
        
        for column, (min_val, max_val) in self._numeric_columns.items():
            col_expr = F.col(column)
            col_valid = col_expr.isNull() | ((col_expr >= min_val) & (col_expr <= max_val))
            is_valid_bounds = is_valid_bounds & col_valid
            
            bound_issues_list.append(
                F.when((col_expr.isNotNull()) & ~col_valid, F.concat(F.lit(column), F.lit("_OUT_OF_BOUNDS")))
            )
        
        # AQI validity check
        aqi_valid = (aqi_value_rounded.isNull() | ((aqi_value_rounded >= 0) & (aqi_value_rounded <= 500)))
        is_valid_overall = is_valid_bounds & aqi_valid
        
        # Single SELECT with all columns - NO intermediate withColumn operations!
        result = prepared.select(
            select_exprs + [
                aqi_value_rounded.alias("aqi_value"),
                F.when(aqi_value_rounded.isNull(), F.lit(None))
                    .when(aqi_value_rounded <= 50, "Good")
                    .when(aqi_value_rounded <= 100, "Moderate")
                    .when(aqi_value_rounded <= 200, "Unhealthy")
                    .otherwise("Hazardous")
                    .alias("aqi_category"),
                is_valid_overall.alias("is_valid"),
                F.when(is_valid_overall, "GOOD").otherwise("CAUTION") \
                    .alias("quality_flag"),
                F.trim(F.concat_ws("|",
                    *bound_issues_list,
                    F.when((aqi_value_rounded.isNotNull()) & ~aqi_valid, "AQI_OUT_OF_RANGE"),
                )).alias("quality_issues"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            ]
        )

        return result.select(
            "facility_code", "facility_name", F.col("timestamp_local").alias("timestamp"),
            "date_hour", "date",
            "pm2_5", "pm10", "dust", "nitrogen_dioxide", "ozone", "sulphur_dioxide", "carbon_monoxide",
            "uv_index", "uv_index_clear_sky", "aqi_category", "aqi_value",
            "is_valid", "quality_flag", "quality_issues", "created_at", "updated_at"
        )

    def _aqi_from_pm25(self, column: F.Column) -> F.Column:
        """Calculate AQI (Air Quality Index) from PM2.5 concentration using EPA breakpoints."""
        def scale(col: F.Column, c_low: float, c_high: float, aqi_low: int, aqi_high: int) -> F.Column:
            return ((col - F.lit(c_low)) / F.lit(c_high - c_low)) * F.lit(aqi_high - aqi_low) + F.lit(aqi_low)

        return (
            F.when(column.isNull(), None)
            .when(column <= F.lit(12.0), scale(column, 0.0, 12.0, 0, 50))
            .when(column <= F.lit(35.4), scale(column, 12.1, 35.4, 51, 100))
            .when(column <= F.lit(55.4), scale(column, 35.5, 55.4, 101, 150))
            .when(column <= F.lit(150.4), scale(column, 55.5, 150.4, 151, 200))
            .when(column <= F.lit(250.4), scale(column, 150.5, 250.4, 201, 300))
            .otherwise(scale(F.least(column, F.lit(500.0)), 250.5, 500.0, 301, 500))
        )


__all__ = ["SilverHourlyAirQualityLoader"]
