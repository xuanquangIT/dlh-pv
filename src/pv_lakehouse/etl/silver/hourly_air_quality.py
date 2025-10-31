"""Silver loader producing clean_hourly_air_quality."""

from __future__ import annotations

from functools import reduce
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader


class SilverHourlyAirQualityLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_air_quality"
    silver_table = "lh.silver.clean_hourly_air_quality"
    s3_base_path = "s3a://lakehouse/silver/clean_hourly_air_quality"
    timestamp_column = "air_timestamp"
    partition_cols = ("date_hour",)

    def __init__(self, options=None):
        from .base import LoadOptions
        if options is None:
            options = LoadOptions(
                target_file_size_mb=self.DEFAULT_TARGET_FILE_SIZE_MB,
                max_records_per_file=self.DEFAULT_MAX_RECORDS_PER_FILE,
            )
        else:
            options.target_file_size_mb = min(options.target_file_size_mb, self.DEFAULT_TARGET_FILE_SIZE_MB)
            options.max_records_per_file = min(options.max_records_per_file, self.DEFAULT_MAX_RECORDS_PER_FILE)
        super().__init__(options)

    def run(self) -> int:
        """
        Process bronze air quality data in 7-day chunks.
        
        Weekly chunks balance memory usage with processing efficiency for moderately wide tables (9 columns).
        """
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0

        # Process in 7-day chunks (moderate table: 9 columns)
        return self._process_in_chunks(bronze_df, chunk_days=7)

    _numeric_columns = {
        "pm2_5": (0.0, 500.0),
        "pm10": (0.0, 500.0),
        "dust": (0.0, 500.0),
        "nitrogen_dioxide": (0.0, 500.0),
        "ozone": (0.0, 500.0),
        "sulphur_dioxide": (0.0, 500.0),
        "carbon_monoxide": (0.0, 500.0),
        "uv_index": (0.0, 15.0),
        "uv_index_clear_sky": (0.0, 15.0),
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

        # Keep hourly granularity - no aggregation
        prepared = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.col("air_timestamp").cast("timestamp").alias("timestamp"),
                F.date_trunc("hour", F.col("air_timestamp")).alias("date_hour"),
                F.to_date(F.col("air_timestamp")).alias("date"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("air_timestamp").isNotNull())
        )

        # Apply numeric column rounding and add missing columns
        result = prepared
        for column, (min_value, max_value) in self._numeric_columns.items():
            if column not in result.columns:
                result = result.withColumn(column, F.lit(None))
            else:
                result = result.withColumn(column, F.round(F.col(column), 4))

        # Calculate AQI from PM2.5 for each hourly record
        aqi_value = self._aqi_from_pm25(F.col("pm2_5"))
        result = result.withColumn("aqi_value", F.round(aqi_value).cast("int"))
        result = result.withColumn(
            "aqi_category",
            F.when(F.col("aqi_value").isNull(), F.lit(None))
            .when(F.col("aqi_value") <= 50, F.lit("Good"))
            .when(F.col("aqi_value") <= 100, F.lit("Moderate"))
            .when(F.col("aqi_value") <= 200, F.lit("Unhealthy"))
            .otherwise(F.lit("Hazardous")),
        )

        # Validation rules for each hourly record
        validity_conditions = [
            (F.col(column).isNull())
            | ((F.col(column) >= F.lit(min_value)) & (F.col(column) <= F.lit(max_value)))
            for column, (min_value, max_value) in self._numeric_columns.items()
        ]
        validity_conditions.append(
            (F.col("aqi_value").isNull())
            | ((F.col("aqi_value") >= F.lit(0)) & (F.col("aqi_value") <= F.lit(500)))
        )
        
        if validity_conditions:
            is_valid_expr = reduce(lambda acc, expr: acc & expr, validity_conditions)
        else:
            is_valid_expr = F.lit(True)

        result = result.withColumn("is_valid", is_valid_expr)
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("OUT_OF_RANGE")),
        )

        # Coalesce to 1 partition to minimize concurrent Iceberg partition writers
        # With hourly partitioning, FanoutDataWriter keeps ALL partition writers open
        result = result.coalesce(1)

        # Add metadata timestamps
        current_ts = F.current_timestamp()
        result = result.withColumn("created_at", current_ts).withColumn("updated_at", current_ts)

        # Select and order columns
        ordered_columns = [
            "facility_code",
            "facility_name",
            "timestamp",
            "date_hour",
            "date",
            "pm2_5",
            "pm10",
            "dust",
            "nitrogen_dioxide",
            "ozone",
            "sulphur_dioxide",
            "carbon_monoxide",
            "uv_index",
            "uv_index_clear_sky",
            "aqi_category",
            "aqi_value",
            "is_valid",
            "quality_flag",
            "created_at",
            "updated_at",
        ]
        return result.select(*ordered_columns)

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
