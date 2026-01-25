"""Shared helpers for Gold-zone loaders."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOGGER = logging.getLogger(__name__)

# Default thresholds for dimension broadcasting
DEFAULT_BROADCAST_MAX_ROWS = 10_000
DEFAULT_BROADCAST_MAX_SIZE_MB = 10


def estimate_dataframe_size_mb(df: DataFrame) -> float:
    """Estimate DataFrame size in megabytes.

    Uses Spark's logical plan statistics when available, otherwise
    falls back to sampling-based estimation.

    Args:
        df: DataFrame to estimate size for.

    Returns:
        Estimated size in megabytes.
    """
    if df is None:
        return 0.0

    # Check row count first - early exit for empty DataFrames
    try:
        row_count = df.count()
        if row_count == 0:
            return 0.0
    except Exception:
        return 0.0

    # Try to get size from Spark statistics for non-empty DataFrames
    try:
        stats = df._jdf.queryExecution().analyzed().stats()
        size_bytes = stats.sizeInBytes()
        # Only trust stats if they're reasonable (less than Long.MaxValue placeholder)
        if size_bytes and 0 < size_bytes < 8_000_000_000_000:  # ~8TB max
            return size_bytes / (1024 * 1024)
    except Exception:
        pass

    # Fallback: estimate based on row count and schema
    try:
        # Estimate bytes per row based on schema
        bytes_per_row = 0
        for field in df.schema.fields:
            if isinstance(field.dataType, T.StringType):
                bytes_per_row += 50  # Average string length estimate
            elif isinstance(field.dataType, (T.IntegerType, T.FloatType)):
                bytes_per_row += 4
            elif isinstance(field.dataType, (T.LongType, T.DoubleType)):
                bytes_per_row += 8
            elif isinstance(field.dataType, T.DecimalType):
                bytes_per_row += 16
            elif isinstance(field.dataType, T.TimestampType):
                bytes_per_row += 8
            elif isinstance(field.dataType, T.DateType):
                bytes_per_row += 4
            else:
                bytes_per_row += 8  # Default estimate

        return (row_count * bytes_per_row) / (1024 * 1024)
    except Exception:
        return 0.0


def broadcast_small_dim(
    dataframe: Optional[DataFrame],
    max_rows: int = DEFAULT_BROADCAST_MAX_ROWS,
    max_size_mb: float = DEFAULT_BROADCAST_MAX_SIZE_MB,
) -> Optional[DataFrame]:
    """Broadcast dimension table for optimized join performance.

    Applies Spark broadcast hint to small dimension tables to avoid
    expensive shuffle operations during joins. Broadcasting sends the
    entire table to all executors, enabling hash joins without shuffle.

    Args:
        dataframe: Dimension DataFrame to potentially broadcast.
        max_rows: Maximum rows to broadcast (default 10,000).
        max_size_mb: Maximum size in MB to broadcast (default 10MB).

    Returns:
        Broadcasted DataFrame if within thresholds, original otherwise.
        Returns None if input is None or empty.
    """
    if dataframe is None or is_empty(dataframe):
        return dataframe

    # Estimate size for logging and threshold check
    estimated_size = estimate_dataframe_size_mb(dataframe)

    # Apply broadcast hint - Spark optimizer will verify actual feasibility
    result = F.broadcast(dataframe)

    LOGGER.debug(
        "Applied broadcast hint to dimension table (estimated %.2fMB)",
        estimated_size,
    )
    return result


def dec(precision: int, scale: int) -> T.DecimalType:
    """Create DecimalType with specified precision and scale.

    Args:
        precision: Total number of digits.
        scale: Number of digits after decimal point.

    Returns:
        Configured DecimalType instance.
    """
    return T.DecimalType(precision, scale)


def is_empty(dataframe: Optional[DataFrame]) -> bool:
    """Check if DataFrame is None or contains no rows.

    Args:
        dataframe: DataFrame to check.

    Returns:
        True if None or empty, False otherwise.
    """
    if dataframe is None:
        return True
    return dataframe.rdd.isEmpty()


def first_value(dataframe: Optional[DataFrame], column: str) -> Optional[Any]:
    """Extract first value from a DataFrame column.

    Args:
        dataframe: Source DataFrame.
        column: Column name to extract value from.

    Returns:
        First value in column, or None if unavailable.
    """
    if is_empty(dataframe) or column not in dataframe.columns:
        return None
    row = dataframe.select(column).limit(1).collect()
    if not row:
        return None
    return row[0][column]


def compute_date_key(date_col: Column) -> Column:
    return F.date_format(date_col, "yyyyMMdd").cast("int")


def season_expr(month_col: Column) -> Column:
    return (
        F.when(month_col.isin(12, 1, 2), F.lit("Summer"))
        .when(month_col.isin(3, 4, 5), F.lit("Autumn"))
        .when(month_col.isin(6, 7, 8), F.lit("Winter"))
        .otherwise(F.lit("Spring"))
    )


def time_of_day_expr(hour_col: Column) -> Column:
    return (
        F.when((hour_col >= 6) & (hour_col < 12), F.lit("Morning"))
        .when((hour_col >= 12) & (hour_col < 17), F.lit("Afternoon"))
        .when((hour_col >= 17) & (hour_col < 21), F.lit("Evening"))
        .otherwise(F.lit("Night"))
    )


def build_hourly_fact_base(
    dataframe: Optional[DataFrame],
    *,
    timestamp_column: str = "date_hour",
) -> Optional[DataFrame]:
    if dataframe is None or timestamp_column not in dataframe.columns:
        return None

    ts_col = F.col(timestamp_column)
    base = dataframe.withColumn("full_date", F.to_date(ts_col))
    base = base.withColumn("date_key", compute_date_key(F.col("full_date")))
    base = base.withColumn(
        "time_key",
        (F.hour(ts_col) * F.lit(100) + F.minute(ts_col)).cast("int"),
    )
    return base


def require_sources(
    sources: Dict[str, Optional[DataFrame]],
    required: Dict[str, str],
) -> Dict[str, DataFrame]:
    resolved: Dict[str, DataFrame] = {}
    for alias, context in required.items():
        dataframe = sources.get(alias)
        if is_empty(dataframe):
            raise ValueError(f"{alias} must be available before running {context}")
        resolved[alias] = dataframe  # type: ignore[assignment]
    return resolved


def classify_weather(dataframe: Optional[DataFrame]) -> Optional[DataFrame]:
    if is_empty(dataframe):
        return None
    enriched = dataframe.withColumn(
        "condition_name",
        F.when(F.col("shortwave_radiation") >= 800, F.lit("High Radiation"))
        .when(F.col("shortwave_radiation") >= 300, F.lit("Moderate Radiation"))
        .otherwise(F.lit("Low Radiation")),
    )
    enriched = enriched.withColumn(
        "radiation_level",
        F.when(F.col("shortwave_radiation") >= 800, F.lit("High"))
        .when(F.col("shortwave_radiation") >= 300, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )
    enriched = enriched.withColumn(
        "cloud_category",
        F.when(F.col("cloud_cover") <= 20, F.lit("Clear"))
        .when(F.col("cloud_cover") <= 70, F.lit("Partly Cloudy"))
        .otherwise(F.lit("Overcast")),
    )
    enriched = enriched.withColumn(
        "temperature_range",
        F.when(F.col("temperature_2m") <= 10, F.lit("Cold"))
        .when(F.col("temperature_2m") <= 25, F.lit("Mild"))
        .otherwise(F.lit("Hot")),
    )
    enriched = enriched.withColumn(
        "weather_severity",
        F.when(F.col("shortwave_radiation") < 100, F.lit("High"))
        .when(F.col("cloud_cover") > 80, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )
    return enriched


def classify_air_quality(dataframe: Optional[DataFrame]) -> Optional[DataFrame]:
    if is_empty(dataframe):
        return None
    return dataframe.withColumn(
        "aq_category",
        F.when(F.col("pm2_5") <= 12.0, F.lit("Good"))
        .when(F.col("pm2_5") <= 35.4, F.lit("Moderate"))
        .when(F.col("pm2_5") <= 55.4, F.lit("Unhealthy"))
        .otherwise(F.lit("Hazardous")),
    )


def build_weather_lookup(
    weather_records: Optional[DataFrame],
    dim_weather_condition: Optional[DataFrame],
) -> Optional[DataFrame]:
    if is_empty(weather_records) or is_empty(dim_weather_condition):
        return None
    
    # CRITICAL FIX: Keep hourly granularity by including 'date_hour' instead of just 'date'
    # Previous version grouped by (facility, date, condition) which caused cartesian product
    # when joining hourly facts - each fact row matched ALL conditions for that date
    
    # Join with dimension to get weather_condition_key
    enriched = weather_records.alias("weather").join(
        F.broadcast(dim_weather_condition.alias("dim")), 
        on="condition_name", 
        how="left"
    )

    # Return hourly weather data with condition keys
    # Join key MUST be (facility_code, date_hour) to maintain 1:1 relationship with facts
    result = enriched.select(
        F.col("weather.facility_code").alias("facility_code"),
        F.col("weather.date").alias("full_date"),
        F.col("weather.date_hour").alias("date_hour"),  # CRITICAL: Keep hour-level granularity
        F.col("dim.weather_condition_key").alias("weather_condition_key"),
        F.col("weather.shortwave_radiation").alias("shortwave_radiation"),
        F.col("weather.direct_radiation").alias("direct_radiation"),
        F.col("weather.diffuse_radiation").alias("diffuse_radiation"),
        F.col("weather.temperature_2m").alias("temperature_2m"),
        F.col("weather.cloud_cover").alias("cloud_cover"),
        F.col("weather.wind_speed_10m").alias("wind_speed_10m"),
        F.col("weather.precipitation").alias("precipitation"),
        F.col("weather.sunshine_duration").alias("sunshine_duration"),
        F.col("weather.weather_severity").alias("weather_severity"),
    )
    
    return result.dropDuplicates(["facility_code", "date_hour"])


def build_air_quality_lookup(
    air_quality_records: Optional[DataFrame],
    dim_air_quality_category: Optional[DataFrame],
) -> Optional[DataFrame]:
    if is_empty(air_quality_records) or is_empty(dim_air_quality_category):
        return None
    # Broadcast small dimension table (4 rows) to avoid shuffle join
    joined = air_quality_records.join(
        F.broadcast(dim_air_quality_category.select("air_quality_category_key", "category_name")),
        air_quality_records["aq_category"] == F.col("category_name"),
        how="left",
    )
    # CRITICAL FIX: Keep hourly granularity with date_hour
    # Previous version only had 'date' which caused cartesian product when joining hourly facts
    result = joined.select(
        "facility_code",
        F.col("date").alias("full_date"),
        F.col("date_hour").alias("date_hour"),  # CRITICAL: Keep hour-level granularity
        "air_quality_category_key",
        "pm2_5",
        "pm10",
        "dust",
        "nitrogen_dioxide",
        "ozone",
        "uv_index",
    )
    
    # CRITICAL: Deduplicate to ensure 1:1 join  
    return result.dropDuplicates(["facility_code", "date_hour"])


def build_aqi_lookup(
    air_quality_records: Optional[DataFrame],
    dim_aqi_category: Optional[DataFrame],
) -> Optional[DataFrame]:
    """Build AQI lookup table with category keys based on AQI value ranges.
    
    Args:
        air_quality_records: Silver air quality data with aqi_value column
        dim_aqi_category: Dimension table with AQI categories and ranges
        
    Returns:
        DataFrame with facility_code, date_hour, and aqi_category_key for joining with facts
    """
    if is_empty(air_quality_records) or is_empty(dim_aqi_category):
        return None
    
    # Ensure we have the required columns
    if "aqi_value" not in air_quality_records.columns:
        return None
    
    # Broadcast small dimension table (6 rows) to avoid shuffle join
    dim_broadcast = F.broadcast(dim_aqi_category.select(
        "aqi_category_key",
        "aqi_category", 
        "aqi_range_min",
        "aqi_range_max"
    ))
    
    # Join based on AQI value falling within range
    # Use cross join + filter for range-based matching
    result = air_quality_records.alias("aq").crossJoin(dim_broadcast.alias("dim"))
    
    # Filter to match AQI value with correct category range
    result = result.filter(
        (F.col("aq.aqi_value") >= F.col("dim.aqi_range_min")) &
        (F.col("aq.aqi_value") <= F.col("dim.aqi_range_max"))
    )
    
    # Select required columns for fact table join
    # CRITICAL: Keep hourly granularity with date_hour to maintain 1:1 relationship
    result = result.select(
        F.col("aq.facility_code").alias("facility_code"),
        F.col("aq.date").alias("full_date"),
        F.col("aq.date_hour").alias("date_hour"),  # CRITICAL: Hour-level granularity
        F.col("dim.aqi_category_key").alias("aqi_category_key"),
        F.col("aq.pm2_5").alias("pm2_5"),
        F.col("aq.pm10").alias("pm10"),
        F.col("aq.dust").alias("dust"),
        F.col("aq.nitrogen_dioxide").alias("nitrogen_dioxide"),
        F.col("aq.ozone").alias("ozone"),
        F.col("aq.sulphur_dioxide").alias("sulphur_dioxide"),
        F.col("aq.carbon_monoxide").alias("carbon_monoxide"),
        F.col("aq.uv_index").alias("uv_index"),
        F.col("aq.uv_index_clear_sky").alias("uv_index_clear_sky"),
        F.col("aq.aqi_value").alias("aqi_value"),
    )
    
    # CRITICAL: Deduplicate to ensure 1:1 join with fact table
    return result.dropDuplicates(["facility_code", "date_hour"])
