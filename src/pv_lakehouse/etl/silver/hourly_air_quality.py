"""Silver loader producing clean_hourly_air_quality."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, Window
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
        """Process bronze air quality data in 3-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=3)

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

    # Outlier detection thresholds
    _outlier_zscore_threshold = 3.0  # Values > 3 sigma from mean
    _iqr_multiplier = 1.5  # Standard IQR multiplier for outliers

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
        prepared = prepared_base.withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_local"))) \
                                 .withColumn("date", F.to_date(F.col("timestamp_local")))

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

        # Validation: each column within range + AQI valid (0-500)
        is_valid_expr = F.lit(True)
        for column, (min_val, max_val) in self._numeric_columns.items():
            is_valid_expr = is_valid_expr & (
                F.col(column).isNull() | ((F.col(column) >= min_val) & (F.col(column) <= max_val))
            )
        is_valid_expr = is_valid_expr & (
            F.col("aqi_value").isNull() | ((F.col("aqi_value") >= 0) & (F.col("aqi_value") <= 500))
        )

        result = (
            result
            .withColumn("is_valid", is_valid_expr)
            .withColumn("quality_flag", F.when(F.col("is_valid"), "GOOD").otherwise("OUT_OF_RANGE"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        # Outlier detection: Z-score method per facility
        result = self._detect_outliers_zscore(result)
        
        # Outlier detection: IQR method per facility
        result = self._detect_outliers_iqr(result)

        return result.select(
            "facility_code", "facility_name", F.col("timestamp_local").alias("timestamp"),
            "date_hour", "date",
            "pm2_5", "pm10", "dust", "nitrogen_dioxide", "ozone", "sulphur_dioxide", "carbon_monoxide",
            "uv_index", "uv_index_clear_sky", "aqi_category", "aqi_value",
            "is_valid", "quality_flag", "created_at", "updated_at",
            "has_outliers_zscore", "outliers_zscore_cols",
            "has_outliers_iqr", "outliers_iqr_cols"
        )

    def _detect_outliers_zscore(self, df: DataFrame) -> DataFrame:
        """
        Detect outliers using Z-score method per facility.
        Marks values that deviate > threshold standard deviations from facility mean.
        """
        window_spec = Window.partitionBy("facility_code")
        
        outlier_flags = []
        for column in self._numeric_columns.keys():
            if column in df.columns:
                # Calculate mean and stddev per facility
                df_stats = (
                    df
                    .withColumn(f"{column}_mean", F.avg(F.col(column)).over(window_spec))
                    .withColumn(f"{column}_stddev", F.stddev(F.col(column)).over(window_spec))
                )
                
                # Calculate Z-score and flag outliers
                df_stats = (
                    df_stats
                    .withColumn(
                        f"{column}_zscore",
                        F.when(
                            (F.col(f"{column}_stddev").isNotNull()) & (F.col(f"{column}_stddev") > 0),
                            F.abs((F.col(column) - F.col(f"{column}_mean")) / F.col(f"{column}_stddev"))
                        ).otherwise(F.lit(0))
                    )
                    .withColumn(
                        f"{column}_is_outlier",
                        F.col(f"{column}_zscore") > self._outlier_zscore_threshold
                    )
                )
                
                df = df_stats
                outlier_flags.append(f"{column}_is_outlier")
        
        # Create composite outlier flag
        if outlier_flags:
            outlier_expr = F.lit(False)
            outlier_cols_expr = F.lit("")
            
            for flag_col in outlier_flags:
                column_name = flag_col.replace("_is_outlier", "")
                outlier_expr = outlier_expr | F.col(flag_col)
                outlier_cols_expr = F.concat(
                    outlier_cols_expr,
                    F.when(F.col(flag_col), F.concat(F.lit(column_name), F.lit(";"))).otherwise(F.lit(""))
                )
            
            df = df.withColumn("has_outliers_zscore", outlier_expr)
            df = df.withColumn("outliers_zscore_cols", F.rtrim(outlier_cols_expr, ";"))
            
            # Drop intermediate Z-score columns
            drop_cols = [col for col in df.columns if "_zscore" in col or col in outlier_flags]
            df = df.drop(*drop_cols)
        else:
            df = df.withColumn("has_outliers_zscore", F.lit(False))
            df = df.withColumn("outliers_zscore_cols", F.lit(""))
        
        return df

    def _detect_outliers_iqr(self, df: DataFrame) -> DataFrame:
        """
        Detect outliers using Interquartile Range (IQR) method per facility.
        Marks values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] range.
        """
        window_spec = Window.partitionBy("facility_code")
        
        outlier_flags = []
        for column in self._numeric_columns.keys():
            if column in df.columns:
                # Calculate Q1, Q3, IQR per facility
                df_stats = (
                    df
                    .withColumn(f"{column}_q1", F.percentile_approx(F.col(column), 0.25).over(window_spec))
                    .withColumn(f"{column}_q3", F.percentile_approx(F.col(column), 0.75).over(window_spec))
                )
                
                df_stats = (
                    df_stats
                    .withColumn(
                        f"{column}_iqr",
                        F.col(f"{column}_q3") - F.col(f"{column}_q1")
                    )
                    .withColumn(
                        f"{column}_lower_bound",
                        F.col(f"{column}_q1") - (self._iqr_multiplier * F.col(f"{column}_iqr"))
                    )
                    .withColumn(
                        f"{column}_upper_bound",
                        F.col(f"{column}_q3") + (self._iqr_multiplier * F.col(f"{column}_iqr"))
                    )
                    .withColumn(
                        f"{column}_is_outlier_iqr",
                        F.col(column).isNotNull() & (
                            (F.col(column) < F.col(f"{column}_lower_bound")) |
                            (F.col(column) > F.col(f"{column}_upper_bound"))
                        )
                    )
                )
                
                df = df_stats
                outlier_flags.append(f"{column}_is_outlier_iqr")
        
        # Create composite outlier flag
        if outlier_flags:
            outlier_expr = F.lit(False)
            outlier_cols_expr = F.lit("")
            
            for flag_col in outlier_flags:
                column_name = flag_col.replace("_is_outlier_iqr", "")
                outlier_expr = outlier_expr | F.col(flag_col)
                outlier_cols_expr = F.concat(
                    outlier_cols_expr,
                    F.when(F.col(flag_col), F.concat(F.lit(column_name), F.lit(";"))).otherwise(F.lit(""))
                )
            
            df = df.withColumn("has_outliers_iqr", outlier_expr)
            df = df.withColumn("outliers_iqr_cols", F.rtrim(outlier_cols_expr, ";"))
            
            # Drop intermediate IQR columns
            drop_cols = [col for col in df.columns if ("_q1" in col or "_q3" in col or "_iqr" in col or 
                                                        "_bound" in col or "_is_outlier_iqr" in col)]
            df = df.drop(*drop_cols)
        else:
            df = df.withColumn("has_outliers_iqr", F.lit(False))
            df = df.withColumn("outliers_iqr_cols", F.lit(""))
        
        return df

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
