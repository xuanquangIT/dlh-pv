"""Silver loader producing clean_hourly_weather."""

from __future__ import annotations

from typing import Optional
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader


class SilverHourlyWeatherLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_weather"
    silver_table = "lh.silver.clean_hourly_weather"
    timestamp_column = "weather_timestamp"
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
        Process bronze weather data in 3-day chunks.
        
        3-day chunks limit concurrent partition writers to 72 (3 days × 24 hours),
        preventing OOM from Iceberg FanoutDataWriter which keeps all partition writers open.
        Reduces memory usage and improves performance compared to 7-day chunks.
        """
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0

        # Process in 3-day chunks (same as energy loader for consistency)
        return self._process_in_chunks(bronze_df, chunk_days=3)

    _numeric_columns = {
        "shortwave_radiation": (0.0, 1500.0),
        "direct_radiation": (0.0, 1500.0),
        "diffuse_radiation": (0.0, 1500.0),
        "direct_normal_irradiance": (0.0, 1500.0),
        "temperature_2m": (-50.0, 60.0),
        "dew_point_2m": (-50.0, 60.0),
        "wet_bulb_temperature_2m": (-50.0, 60.0),
        "cloud_cover": (0.0, 100.0),
        "cloud_cover_low": (0.0, 100.0),
        "cloud_cover_mid": (0.0, 100.0),
        "cloud_cover_high": (0.0, 100.0),
        "precipitation": (0.0, 1000.0),
        "sunshine_duration": (0.0, 3600.0),
        "total_column_integrated_water_vapour": (0.0, 100.0),
        "wind_speed_10m": (0.0, 60.0),
        "wind_direction_10m": (0.0, 360.0),
        "wind_gusts_10m": (0.0, 120.0),
        "pressure_msl": (800.0, 1100.0),
    }

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        if bronze_df is None or not bronze_df.columns:
            return None

        required_columns = {
            "facility_code",
            "facility_name",
            "weather_timestamp",
        }
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze weather source: {sorted(missing)}")

        # Timezone mapping for each facility (cached - no JOIN needed!)
        # Eliminates expensive shuffle operation
        timezone_map = {
            "NYNGAN": "Australia/Brisbane",      # NEM NSW1
            "COLEASF": "Australia/Brisbane",    # NEM NSW1
            "CLARESF": "Australia/Brisbane",    # NEM QLD1
            "GANNSF": "Australia/Brisbane",     # NEM VIC1
            "BNGSF1": "Australia/Adelaide",     # NEM SA1
        }
        
        prepared_base = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.col("weather_timestamp").cast("timestamp").alias("timestamp"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("weather_timestamp").isNotNull())
        )
        
        prepared = prepared_base.withColumn(
            "tz_string",
            F.create_map([F.lit(x) for pair in timezone_map.items() for x in pair])[F.col("facility_code")]
        )
        
        # Convert UTC → local time based on timezone mapping (no JOIN shuffle!)
        prepared = prepared.withColumn(
            "timestamp",
            F.from_utc_timestamp(F.col("timestamp"), F.col("tz_string"))
        )
        # Extract both date_hour and date in ONE operation (avoid multiple withColumn calls)
        prepared = prepared.withColumn("date_hour", F.date_trunc("hour", F.col("timestamp"))) \
                           .withColumn("date", F.to_date(F.col("timestamp"))) \
                           .drop("tz_string")

        # Apply numeric column rounding and add missing columns
        result = prepared
        for column, (min_value, max_value) in self._numeric_columns.items():
            if column not in result.columns:
                result = result.withColumn(column, F.lit(None))
            else:
                result = result.withColumn(column, F.round(F.col(column), 4))

        # Validation rules for each hourly record
        validity_conditions = [
            (F.col(column).isNull())
            | ((F.col(column) >= F.lit(min_value)) & (F.col(column) <= F.lit(max_value)))
            for column, (min_value, max_value) in self._numeric_columns.items()
        ]
        
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
            *self._numeric_columns.keys(),
            "is_valid",
            "quality_flag",
            "created_at",
            "updated_at",
        ]
        return result.select(*ordered_columns)


__all__ = ["SilverHourlyWeatherLoader"]
