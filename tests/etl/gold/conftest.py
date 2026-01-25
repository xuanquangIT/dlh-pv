"""Shared pytest fixtures for Gold layer ETL tests.

Provides SparkSession, sample DataFrames, and dimension table fixtures
for testing Gold layer transformations without external dependencies.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Dict, Generator

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    """Create SparkSession for testing.

    Uses local mode with minimal configuration for fast test execution.
    Session is shared across all tests in the module for efficiency.

    Yields:
        Configured SparkSession instance.
    """
    session = (
        SparkSession.builder.appName("gold-layer-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


# -------------------------------------------------------------------------
# Dimension table fixtures
# -------------------------------------------------------------------------
@pytest.fixture
def dim_facility_df(spark: SparkSession) -> DataFrame:
    """Create sample facility dimension DataFrame.

    Returns:
        DataFrame with 3 sample facilities for testing.
    """
    schema = T.StructType(
        [
            T.StructField("facility_key", T.IntegerType(), False),
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("facility_name", T.StringType(), True),
            T.StructField("total_capacity_mw", T.DecimalType(10, 2), True),
        ]
    )
    data = [
        (1, "SOLAR_FARM_A", "Solar Farm Alpha", Decimal("50.00")),
        (2, "SOLAR_FARM_B", "Solar Farm Beta", Decimal("75.50")),
        (3, "SOLAR_FARM_C", "Solar Farm Gamma", Decimal("120.00")),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dim_date_df(spark: SparkSession) -> DataFrame:
    """Create sample date dimension DataFrame.

    Returns:
        DataFrame with date dimension for January 2024.
    """
    schema = T.StructType(
        [
            T.StructField("date_key", T.IntegerType(), False),
            T.StructField("full_date", T.DateType(), False),
            T.StructField("year", T.IntegerType(), True),
            T.StructField("quarter", T.IntegerType(), True),
            T.StructField("month", T.IntegerType(), True),
            T.StructField("month_name", T.StringType(), True),
            T.StructField("week", T.IntegerType(), True),
            T.StructField("day_of_month", T.IntegerType(), True),
            T.StructField("day_of_week", T.IntegerType(), True),
            T.StructField("day_name", T.StringType(), True),
        ]
    )
    data = [
        (
            20240101,
            dt.date(2024, 1, 1),
            2024,
            1,
            1,
            "January",
            1,
            1,
            1,
            "Monday",
        ),
        (
            20240102,
            dt.date(2024, 1, 2),
            2024,
            1,
            1,
            "January",
            1,
            2,
            2,
            "Tuesday",
        ),
        (
            20240103,
            dt.date(2024, 1, 3),
            2024,
            1,
            1,
            "January",
            1,
            3,
            3,
            "Wednesday",
        ),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dim_time_df(spark: SparkSession) -> DataFrame:
    """Create sample time dimension DataFrame.

    Returns:
        DataFrame with 24 hours for testing.
    """
    schema = T.StructType(
        [
            T.StructField("time_key", T.IntegerType(), False),
            T.StructField("hour", T.IntegerType(), False),
            T.StructField("time_of_day", T.StringType(), True),
        ]
    )
    data = []
    for hour in range(24):
        time_key = hour * 100
        if 6 <= hour < 12:
            time_of_day = "Morning"
        elif 12 <= hour < 17:
            time_of_day = "Afternoon"
        elif 17 <= hour < 21:
            time_of_day = "Evening"
        else:
            time_of_day = "Night"
        data.append((time_key, hour, time_of_day))
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dim_aqi_category_df(spark: SparkSession) -> DataFrame:
    """Create sample AQI category dimension DataFrame.

    Returns:
        DataFrame with EPA AQI categories.
    """
    schema = T.StructType(
        [
            T.StructField("aqi_category_key", T.IntegerType(), False),
            T.StructField("aqi_category", T.StringType(), False),
            T.StructField("aqi_range_min", T.IntegerType(), False),
            T.StructField("aqi_range_max", T.IntegerType(), False),
        ]
    )
    data = [
        (1, "Good", 0, 50),
        (2, "Moderate", 51, 100),
        (3, "Unhealthy for Sensitive", 101, 150),
        (4, "Unhealthy", 151, 200),
        (5, "Very Unhealthy", 201, 300),
        (6, "Hazardous", 301, 500),
    ]
    return spark.createDataFrame(data, schema)


# -------------------------------------------------------------------------
# Silver layer source fixtures
# -------------------------------------------------------------------------
@pytest.fixture
def hourly_energy_df(spark: SparkSession) -> DataFrame:
    """Create sample hourly energy DataFrame from Silver layer.

    Returns:
        DataFrame with energy production data for 3 facilities over 2 days.
    """
    schema = T.StructType(
        [
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("facility_name", T.StringType(), True),
            T.StructField("network_code", T.StringType(), True),
            T.StructField("network_region", T.StringType(), True),
            T.StructField("date_hour", T.TimestampType(), False),
            T.StructField("energy_mwh", T.DecimalType(12, 6), True),
            T.StructField("intervals_count", T.IntegerType(), True),
            T.StructField("quality_flag", T.StringType(), True),
            T.StructField("quality_issues", T.StringType(), True),
            T.StructField("completeness_pct", T.DecimalType(5, 2), True),
        ]
    )
    data = [
        # Day 1 - Hour 10
        (
            "SOLAR_FARM_A",
            "Solar Farm Alpha",
            "NEM",
            "NSW1",
            dt.datetime(2024, 1, 1, 10, 0),
            Decimal("25.500000"),
            12,
            "GOOD",
            None,
            Decimal("100.00"),
        ),
        (
            "SOLAR_FARM_B",
            "Solar Farm Beta",
            "NEM",
            "VIC1",
            dt.datetime(2024, 1, 1, 10, 0),
            Decimal("38.250000"),
            12,
            "GOOD",
            None,
            Decimal("100.00"),
        ),
        # Day 1 - Hour 11
        (
            "SOLAR_FARM_A",
            "Solar Farm Alpha",
            "NEM",
            "NSW1",
            dt.datetime(2024, 1, 1, 11, 0),
            Decimal("28.750000"),
            12,
            "GOOD",
            None,
            Decimal("100.00"),
        ),
        # Day 2 - Hour 12
        (
            "SOLAR_FARM_A",
            "Solar Farm Alpha",
            "NEM",
            "NSW1",
            dt.datetime(2024, 1, 2, 12, 0),
            Decimal("32.100000"),
            12,
            "WARNING",
            "Missing intervals",
            Decimal("83.33"),
        ),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def hourly_weather_df(spark: SparkSession) -> DataFrame:
    """Create sample hourly weather DataFrame from Silver layer.

    Returns:
        DataFrame with weather data matching energy records.
    """
    schema = T.StructType(
        [
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
            T.StructField("shortwave_radiation", T.DecimalType(10, 4), True),
            T.StructField("direct_radiation", T.DecimalType(10, 4), True),
            T.StructField("diffuse_radiation", T.DecimalType(10, 4), True),
            T.StructField("direct_normal_irradiance", T.DecimalType(10, 4), True),
            T.StructField("temperature_2m", T.DecimalType(6, 2), True),
            T.StructField("dew_point_2m", T.DecimalType(6, 2), True),
            T.StructField("cloud_cover", T.DecimalType(5, 2), True),
            T.StructField("cloud_cover_low", T.DecimalType(5, 2), True),
            T.StructField("cloud_cover_mid", T.DecimalType(5, 2), True),
            T.StructField("cloud_cover_high", T.DecimalType(5, 2), True),
            T.StructField("precipitation", T.DecimalType(8, 4), True),
            T.StructField("sunshine_duration", T.DecimalType(8, 2), True),
            T.StructField("wind_speed_10m", T.DecimalType(6, 2), True),
            T.StructField("wind_direction_10m", T.IntegerType(), True),
            T.StructField("wind_gusts_10m", T.DecimalType(6, 2), True),
            T.StructField("pressure_msl", T.DecimalType(8, 2), True),
        ]
    )
    data = [
        # SOLAR_FARM_A - Day 1, Hour 10
        (
            "SOLAR_FARM_A",
            dt.datetime(2024, 1, 1, 10, 0),
            Decimal("850.0000"),
            Decimal("650.0000"),
            Decimal("200.0000"),
            Decimal("750.0000"),
            Decimal("25.50"),
            Decimal("15.20"),
            Decimal("10.00"),
            Decimal("5.00"),
            Decimal("3.00"),
            Decimal("2.00"),
            Decimal("0.0000"),
            Decimal("3600.00"),
            Decimal("8.50"),
            180,
            Decimal("12.30"),
            Decimal("1015.20"),
        ),
        # SOLAR_FARM_B - Day 1, Hour 10
        (
            "SOLAR_FARM_B",
            dt.datetime(2024, 1, 1, 10, 0),
            Decimal("780.0000"),
            Decimal("580.0000"),
            Decimal("200.0000"),
            Decimal("680.0000"),
            Decimal("23.80"),
            Decimal("14.50"),
            Decimal("15.00"),
            Decimal("8.00"),
            Decimal("4.00"),
            Decimal("3.00"),
            Decimal("0.0000"),
            Decimal("3500.00"),
            Decimal("6.20"),
            195,
            Decimal("9.80"),
            Decimal("1013.80"),
        ),
        # SOLAR_FARM_A - Day 1, Hour 11
        (
            "SOLAR_FARM_A",
            dt.datetime(2024, 1, 1, 11, 0),
            Decimal("920.0000"),
            Decimal("720.0000"),
            Decimal("200.0000"),
            Decimal("820.0000"),
            Decimal("27.30"),
            Decimal("15.80"),
            Decimal("8.00"),
            Decimal("4.00"),
            Decimal("2.00"),
            Decimal("2.00"),
            Decimal("0.0000"),
            Decimal("3600.00"),
            Decimal("9.20"),
            175,
            Decimal("14.10"),
            Decimal("1014.50"),
        ),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def hourly_air_quality_df(spark: SparkSession) -> DataFrame:
    """Create sample hourly air quality DataFrame from Silver layer.

    Returns:
        DataFrame with air quality data matching energy records.
    """
    schema = T.StructType(
        [
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
            T.StructField("date", T.DateType(), True),
            T.StructField("pm2_5", T.DecimalType(8, 4), True),
            T.StructField("pm10", T.DecimalType(8, 4), True),
            T.StructField("dust", T.DecimalType(8, 4), True),
            T.StructField("nitrogen_dioxide", T.DecimalType(8, 4), True),
            T.StructField("ozone", T.DecimalType(8, 4), True),
            T.StructField("sulphur_dioxide", T.DecimalType(8, 4), True),
            T.StructField("carbon_monoxide", T.DecimalType(8, 4), True),
            T.StructField("uv_index", T.DecimalType(5, 2), True),
            T.StructField("uv_index_clear_sky", T.DecimalType(5, 2), True),
            T.StructField("aqi_value", T.IntegerType(), True),
        ]
    )
    data = [
        # SOLAR_FARM_A - Day 1, Hour 10 - Good AQI
        (
            "SOLAR_FARM_A",
            dt.datetime(2024, 1, 1, 10, 0),
            dt.date(2024, 1, 1),
            Decimal("8.5000"),
            Decimal("12.3000"),
            Decimal("5.2000"),
            Decimal("15.8000"),
            Decimal("45.2000"),
            Decimal("2.1000"),
            Decimal("0.3500"),
            Decimal("7.50"),
            Decimal("8.20"),
            35,
        ),
        # SOLAR_FARM_B - Day 1, Hour 10 - Moderate AQI
        (
            "SOLAR_FARM_B",
            dt.datetime(2024, 1, 1, 10, 0),
            dt.date(2024, 1, 1),
            Decimal("18.2000"),
            Decimal("25.5000"),
            Decimal("8.8000"),
            Decimal("22.3000"),
            Decimal("52.1000"),
            Decimal("3.5000"),
            Decimal("0.4200"),
            Decimal("6.80"),
            Decimal("7.50"),
            68,
        ),
        # SOLAR_FARM_A - Day 1, Hour 11 - Good AQI
        (
            "SOLAR_FARM_A",
            dt.datetime(2024, 1, 1, 11, 0),
            dt.date(2024, 1, 1),
            Decimal("9.2000"),
            Decimal("13.8000"),
            Decimal("5.5000"),
            Decimal("16.2000"),
            Decimal("48.5000"),
            Decimal("2.3000"),
            Decimal("0.3800"),
            Decimal("8.20"),
            Decimal("9.10"),
            42,
        ),
    ]
    return spark.createDataFrame(data, schema)


# -------------------------------------------------------------------------
# Combined fixtures
# -------------------------------------------------------------------------
@pytest.fixture
def all_dimensions(
    dim_facility_df: DataFrame,
    dim_date_df: DataFrame,
    dim_time_df: DataFrame,
    dim_aqi_category_df: DataFrame,
) -> Dict[str, DataFrame]:
    """Bundle all dimension DataFrames.

    Returns:
        Dictionary of dimension DataFrames keyed by name.
    """
    return {
        "dim_facility": dim_facility_df,
        "dim_date": dim_date_df,
        "dim_time": dim_time_df,
        "dim_aqi_category": dim_aqi_category_df,
    }


@pytest.fixture
def all_sources(
    hourly_energy_df: DataFrame,
    hourly_weather_df: DataFrame,
    hourly_air_quality_df: DataFrame,
    all_dimensions: Dict[str, DataFrame],
) -> Dict[str, DataFrame]:
    """Bundle all source DataFrames for transform testing.

    Returns:
        Dictionary of all source DataFrames including dimensions.
    """
    return {
        "hourly_energy": hourly_energy_df,
        "hourly_weather": hourly_weather_df,
        "hourly_air_quality": hourly_air_quality_df,
        **all_dimensions,
    }
