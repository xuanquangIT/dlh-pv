"""Dimension key lookup helpers for Gold layer transformations.

This module provides reusable functions for joining fact tables with
dimension tables to resolve surrogate keys. All functions apply broadcast
hints to dimension tables for optimal join performance.

Typical usage:
    fact = lookup_facility_key(fact, dim_facility)
    fact = lookup_date_key(fact, dim_date, F.to_date(F.col("date_hour")))
    fact = lookup_time_key(fact, dim_time, F.hour(F.col("date_hour")))
"""

from __future__ import annotations

import logging
from typing import Callable, List, Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .common import broadcast_small_dim, is_empty

LOGGER = logging.getLogger(__name__)


def lookup_dimension_key(
    fact_df: DataFrame,
    dim_df: DataFrame,
    join_condition: Column,
    key_column: str,
    additional_columns: Optional[List[str]] = None,
    broadcast: bool = True,
) -> DataFrame:
    """Generic dimension key lookup with configurable join condition.

    Performs a left join between fact and dimension tables to resolve
    surrogate keys. Optionally broadcasts the dimension table for
    improved join performance.

    Args:
        fact_df: Fact DataFrame requiring dimension key.
        dim_df: Dimension DataFrame containing surrogate keys.
        join_condition: Spark Column expression for join condition.
        key_column: Name of the surrogate key column in dimension.
        additional_columns: Extra columns to include from dimension.
        broadcast: Whether to broadcast dimension table (default True).

    Returns:
        Fact DataFrame with dimension key and additional columns joined.

    Raises:
        ValueError: If required DataFrames are None or empty.
    """
    if is_empty(fact_df):
        raise ValueError("fact_df cannot be None or empty")
    if is_empty(dim_df):
        raise ValueError("dim_df cannot be None or empty")

    # Apply broadcast hint if requested
    dim_prepared = broadcast_small_dim(dim_df) if broadcast else dim_df

    # Build column selection list
    dim_cols = [F.col(f"dim.{key_column}").alias(key_column)]
    if additional_columns:
        for col_name in additional_columns:
            if col_name in dim_df.columns:
                dim_cols.append(F.col(f"dim.{col_name}").alias(col_name))

    # Perform left join to preserve all fact records
    result = fact_df.alias("fact").join(
        dim_prepared.alias("dim"),
        join_condition,
        how="left",
    )

    # Select all fact columns plus dimension key and additional columns
    fact_cols = [F.col(f"fact.{c}") for c in fact_df.columns]
    result = result.select(*fact_cols, *dim_cols)

    LOGGER.debug("Joined dimension key '%s' to fact table", key_column)
    return result


def lookup_facility_key(
    fact_df: DataFrame,
    dim_facility: DataFrame,
    join_column: str = "facility_code",
    additional_columns: Optional[List[str]] = None,
) -> DataFrame:
    """Join fact table with facility dimension to get facility_key.

    Performs exact match join on facility_code (or specified column)
    to resolve facility surrogate key.

    Args:
        fact_df: Fact DataFrame with facility identifier.
        dim_facility: Facility dimension DataFrame.
        join_column: Column name for join (default 'facility_code').
        additional_columns: Extra columns to include (e.g., 'total_capacity_mw').

    Returns:
        Fact DataFrame with facility_key and requested additional columns.
    """
    if is_empty(fact_df) or is_empty(dim_facility):
        return fact_df

    # Default additional columns for facility dimension
    default_cols = ["total_capacity_mw"] if additional_columns is None else additional_columns

    join_condition = F.col(f"fact.{join_column}") == F.col(f"dim.{join_column}")

    return lookup_dimension_key(
        fact_df=fact_df,
        dim_df=dim_facility,
        join_condition=join_condition,
        key_column="facility_key",
        additional_columns=default_cols,
        broadcast=True,
    )


def lookup_date_key(
    fact_df: DataFrame,
    dim_date: DataFrame,
    date_expr: Column,
    additional_columns: Optional[List[str]] = None,
) -> DataFrame:
    """Join fact table with date dimension to get date_key.

    Performs date match between fact date expression and dimension
    full_date column to resolve date surrogate key.

    Args:
        fact_df: Fact DataFrame with date information.
        dim_date: Date dimension DataFrame with full_date column.
        date_expr: Spark Column expression that evaluates to date.
        additional_columns: Extra columns to include from dimension.

    Returns:
        Fact DataFrame with date_key and requested additional columns.

    Example:
        >>> fact = lookup_date_key(
        ...     fact,
        ...     dim_date,
        ...     F.to_date(F.col("date_hour")),
        ...     additional_columns=["year", "quarter", "month"]
        ... )
    """
    if is_empty(fact_df) or is_empty(dim_date):
        return fact_df

    # Default calendar columns from date dimension
    default_cols = ["year", "quarter", "month", "week", "day_of_week"]
    cols_to_include = additional_columns if additional_columns is not None else default_cols

    # Add temporary column for join
    fact_with_date = fact_df.withColumn("_join_date", date_expr)

    join_condition = F.col("fact._join_date") == F.col("dim.full_date")

    result = lookup_dimension_key(
        fact_df=fact_with_date,
        dim_df=dim_date,
        join_condition=join_condition,
        key_column="date_key",
        additional_columns=cols_to_include,
        broadcast=True,
    )

    # Remove temporary join column
    return result.drop("_join_date")


def lookup_time_key(
    fact_df: DataFrame,
    dim_time: DataFrame,
    hour_expr: Column,
    additional_columns: Optional[List[str]] = None,
) -> DataFrame:
    """Join fact table with time dimension to get time_key.

    Performs hour match between fact hour expression and dimension
    hour column to resolve time surrogate key.

    Args:
        fact_df: Fact DataFrame with timestamp information.
        dim_time: Time dimension DataFrame with hour column.
        hour_expr: Spark Column expression that evaluates to hour (0-23).
        additional_columns: Extra columns to include from dimension.

    Returns:
        Fact DataFrame with time_key and requested additional columns.

    Example:
        >>> fact = lookup_time_key(
        ...     fact,
        ...     dim_time,
        ...     F.hour(F.col("date_hour")),
        ...     additional_columns=["time_of_day"]
        ... )
    """
    if is_empty(fact_df) or is_empty(dim_time):
        return fact_df

    # Default time attributes
    default_cols = ["time_of_day"]
    cols_to_include = additional_columns if additional_columns is not None else default_cols

    # Add temporary column for join
    fact_with_hour = fact_df.withColumn("_join_hour", hour_expr)

    join_condition = F.col("fact._join_hour") == F.col("dim.hour")

    result = lookup_dimension_key(
        fact_df=fact_with_hour,
        dim_df=dim_time,
        join_condition=join_condition,
        key_column="time_key",
        additional_columns=cols_to_include,
        broadcast=True,
    )

    # Remove temporary join column
    return result.drop("_join_hour")


def lookup_aqi_category_key(
    fact_df: DataFrame,
    dim_aqi_category: DataFrame,
    aqi_value_column: str = "aqi_value",
) -> DataFrame:
    """Join fact table with AQI category dimension using range lookup.

    Performs range-based join where AQI value falls between category
    min and max thresholds to resolve aqi_category_key.

    Args:
        fact_df: Fact DataFrame with AQI value column.
        dim_aqi_category: AQI category dimension with range columns.
        aqi_value_column: Column name containing AQI values.

    Returns:
        Fact DataFrame with aqi_category_key.

    Note:
        Uses cross join with filter for range matching, which is
        efficient when dimension table is small (typically 6 rows).
    """
    if is_empty(fact_df) or is_empty(dim_aqi_category):
        return fact_df

    # Broadcast small dimension for cross join efficiency
    dim_broadcast = broadcast_small_dim(
        dim_aqi_category.select(
            "aqi_category_key",
            "aqi_range_min",
            "aqi_range_max",
        )
    )

    # Perform cross join and filter by range
    result = fact_df.alias("fact").crossJoin(dim_broadcast.alias("dim"))

    # Filter to match AQI value within category range
    result = result.filter(
        (F.col(f"fact.{aqi_value_column}") >= F.col("dim.aqi_range_min"))
        & (F.col(f"fact.{aqi_value_column}") <= F.col("dim.aqi_range_max"))
    )

    # Select fact columns plus the resolved key
    fact_cols = [F.col(f"fact.{c}") for c in fact_df.columns]
    result = result.select(
        *fact_cols,
        F.col("dim.aqi_category_key").alias("aqi_category_key"),
    )

    LOGGER.debug("Resolved AQI category keys using range lookup")
    return result


def lookup_weather_condition_key(
    fact_df: DataFrame,
    dim_weather_condition: DataFrame,
    condition_column: str = "condition_name",
) -> DataFrame:
    """Join fact table with weather condition dimension.

    Performs exact match join on weather condition name to resolve
    weather_condition_key surrogate key.

    Args:
        fact_df: Fact DataFrame with weather condition column.
        dim_weather_condition: Weather condition dimension DataFrame.
        condition_column: Column name for condition match.

    Returns:
        Fact DataFrame with weather_condition_key.
    """
    if is_empty(fact_df) or is_empty(dim_weather_condition):
        return fact_df

    join_condition = F.col(f"fact.{condition_column}") == F.col(f"dim.{condition_column}")

    return lookup_dimension_key(
        fact_df=fact_df,
        dim_df=dim_weather_condition,
        join_condition=join_condition,
        key_column="weather_condition_key",
        additional_columns=None,
        broadcast=True,
    )


__all__ = [
    "lookup_dimension_key",
    "lookup_facility_key",
    "lookup_date_key",
    "lookup_time_key",
    "lookup_aqi_category_key",
    "lookup_weather_condition_key",
]
