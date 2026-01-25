"""Gold loader for fact_solar_environmental.

Produces a fact table combining solar energy production with environmental
conditions (weather and air quality) at hourly granularity per facility.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import (
    broadcast_small_dim,
    build_aqi_lookup,
    build_hourly_fact_base,
    dec,
    is_empty,
    require_sources,
)
from .dimension_lookup import lookup_date_key, lookup_facility_key, lookup_time_key

LOGGER = logging.getLogger(__name__)


class GoldFactSolarEnvironmentalLoader(BaseGoldLoader):
    """Produce Gold fact rows combining solar energy with environmental conditions.

    This fact table integrates:
    - Energy production metrics (from Silver hourly_energy)
    - Weather conditions (from Silver hourly_weather)
    - Air quality metrics (from Silver hourly_air_quality)

    Grain: 1 row = 1 hour at 1 facility
    """

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="date_hour",
            required_columns=[
                "facility_code",
                "facility_name",
                "network_code",
                "network_region",
                "date_hour",
                "energy_mwh",
                "intervals_count",
                "quality_flag",
                "quality_issues",
                "completeness_pct",
            ],
        ),
        "hourly_weather": SourceTableConfig(
            table_name="lh.silver.clean_hourly_weather",
            timestamp_column="date_hour",
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
            timestamp_column="date_hour",
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
            required_columns=["facility_key", "facility_code", "total_capacity_mw"],
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

    # -------------------------------------------------------------------------
    # Main transform orchestration
    # -------------------------------------------------------------------------
    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        """Transform Silver hourly data into Gold fact table.

        Args:
            sources: Dictionary of source DataFrames keyed by table name.

        Returns:
            Dictionary with single 'fact_solar_environmental' DataFrame,
            or None if no valid data to process.
        """
        # Validate and prepare dimension tables
        dimensions = self._prepare_dimensions(sources)
        if dimensions is None:
            return None

        # Build base fact from energy data
        fact = self._build_base_fact(sources.get("hourly_energy"))
        if fact is None:
            return None

        # Join dimension keys
        fact = self._join_dimension_keys(fact, dimensions)

        # Enrich with weather and air quality data
        fact = self._enrich_weather_data(fact, sources.get("hourly_weather"))
        fact = self._enrich_air_quality_data(fact, sources.get("hourly_air_quality"), dimensions)

        # Calculate metrics and quality indicators
        fact = self._calculate_quality_metrics(fact)
        fact = self._calculate_derived_metrics(fact)

        # Add audit columns and project final schema
        fact = self._add_audit_columns(fact)
        result = self._project_final_columns(fact)

        if is_empty(result):
            return None

        return {"fact_solar_environmental": result}

    # -------------------------------------------------------------------------
    # Dimension preparation
    # -------------------------------------------------------------------------
    def _prepare_dimensions(
        self, sources: Dict[str, DataFrame]
    ) -> Optional[Dict[str, DataFrame]]:
        """Validate and broadcast dimension tables.

        Args:
            sources: Source DataFrames including dimension tables.

        Returns:
            Dictionary of prepared dimension DataFrames, or None if validation fails.
        """
        try:
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
        except ValueError as e:
            LOGGER.error("Missing required dimension: %s", e)
            return None

        return {
            "dim_facility": broadcast_small_dim(required["dim_facility"]),
            "dim_date": broadcast_small_dim(required["dim_date"]),
            "dim_time": broadcast_small_dim(required["dim_time"]),
            "dim_aqi_category": required["dim_aqi_category"],
        }

    # -------------------------------------------------------------------------
    # Base fact construction
    # -------------------------------------------------------------------------
    def _build_base_fact(self, hourly_energy: Optional[DataFrame]) -> Optional[DataFrame]:
        """Build base fact structure from energy data.

        Args:
            hourly_energy: Silver layer energy DataFrame.

        Returns:
            Base fact DataFrame with date_key and time_key, or None.
        """
        if is_empty(hourly_energy):
            LOGGER.warning("No energy data available for fact table")
            return None

        base = build_hourly_fact_base(hourly_energy, timestamp_column="date_hour")
        if is_empty(base):
            return None

        LOGGER.info("Built base fact from energy data")
        return base

    # -------------------------------------------------------------------------
    # Dimension key joins
    # -------------------------------------------------------------------------
    def _join_dimension_keys(
        self, fact: DataFrame, dimensions: Dict[str, DataFrame]
    ) -> DataFrame:
        """Join all dimension keys to fact table.

        Args:
            fact: Base fact DataFrame.
            dimensions: Prepared dimension DataFrames.

        Returns:
            Fact DataFrame with all dimension keys joined.
        """
        dim_facility = dimensions["dim_facility"]
        dim_date = dimensions["dim_date"]
        dim_time = dimensions["dim_time"]

        # Join facility dimension
        fact = fact.join(dim_facility, on="facility_code", how="left")

        # Join date dimension with proper column aliasing
        dim_date_selected = dim_date.select(
            F.col("full_date").alias("dim_full_date"),
            F.col("date_key").alias("dim_date_key"),
            F.col("year"),
            F.col("quarter"),
            F.col("month"),
            F.col("month_name"),
            F.col("week"),
            F.col("day_of_month"),
            F.col("day_of_week"),
            F.col("day_name"),
        )
        fact = fact.join(
            dim_date_selected,
            F.to_date(F.col("date_hour")) == F.col("dim_full_date"),
            how="left",
        )
        fact = fact.drop("date_key").withColumn("date_key", F.col("dim_date_key"))

        # Join time dimension
        dim_time_selected = dim_time.select(
            F.col("time_key").alias("dim_time_key"),
            F.col("hour"),
            F.col("time_of_day"),
        )
        fact = fact.join(
            dim_time_selected,
            F.hour(F.col("date_hour")) == F.col("hour"),
            how="left",
        )
        fact = fact.drop("time_key").withColumn("time_key", F.col("dim_time_key"))

        return fact

    # -------------------------------------------------------------------------
    # Weather data enrichment
    # -------------------------------------------------------------------------
    def _enrich_weather_data(
        self, fact: DataFrame, hourly_weather: Optional[DataFrame]
    ) -> DataFrame:
        """Join weather data and calculate humidity.

        Args:
            fact: Fact DataFrame to enrich.
            hourly_weather: Silver layer weather DataFrame.

        Returns:
            Fact DataFrame with weather columns joined.
        """
        if is_empty(hourly_weather):
            return fact

        # Calculate relative humidity using Magnus formula
        weather = self._calculate_humidity(hourly_weather)

        # Select columns for join
        weather_cols = weather.select(
            "facility_code",
            "date_hour",
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
        )

        return fact.join(weather_cols, on=["facility_code", "date_hour"], how="left")

    def _calculate_humidity(self, weather: DataFrame) -> DataFrame:
        """Calculate relative humidity from temperature and dew point.

        Uses Magnus formula: RH = 100 * exp((17.625*Td)/(243.04+Td)) /
                                       exp((17.625*T)/(243.04+T))

        Args:
            weather: Weather DataFrame with temperature_2m and dew_point_2m.

        Returns:
            Weather DataFrame with humidity_2m column added.
        """
        # Magnus formula constants
        a = F.lit(17.625)
        b = F.lit(243.04)

        # Calculate saturation vapor pressure ratio
        humidity_raw = F.when(
            F.col("dew_point_2m").isNotNull() & F.col("temperature_2m").isNotNull(),
            100.0
            * F.exp((a * F.col("dew_point_2m")) / (b + F.col("dew_point_2m")))
            / F.exp((a * F.col("temperature_2m")) / (b + F.col("temperature_2m"))),
        ).otherwise(F.lit(None))

        # Clamp to valid range [0, 100]
        humidity_clamped = (
            F.when(humidity_raw < 0, 0.0)
            .when(humidity_raw > 100, 100.0)
            .otherwise(humidity_raw)
        )

        return weather.withColumn("humidity_2m", humidity_clamped)

    # -------------------------------------------------------------------------
    # Air quality data enrichment
    # -------------------------------------------------------------------------
    def _enrich_air_quality_data(
        self,
        fact: DataFrame,
        hourly_air_quality: Optional[DataFrame],
        dimensions: Dict[str, DataFrame],
    ) -> DataFrame:
        """Join air quality data with AQI category lookup.

        Args:
            fact: Fact DataFrame to enrich.
            hourly_air_quality: Silver layer air quality DataFrame.
            dimensions: Dimension tables including dim_aqi_category.

        Returns:
            Fact DataFrame with air quality columns joined.
        """
        aqi_lookup = build_aqi_lookup(
            hourly_air_quality, dimensions["dim_aqi_category"]
        )

        if is_empty(aqi_lookup):
            return fact

        aqi_cols = aqi_lookup.select(
            "facility_code",
            "date_hour",
            "aqi_category_key",
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
        )

        return fact.join(aqi_cols, on=["facility_code", "date_hour"], how="left")

    # -------------------------------------------------------------------------
    # Quality metrics calculation
    # -------------------------------------------------------------------------
    def _calculate_quality_metrics(self, fact: DataFrame) -> DataFrame:
        """Calculate data completeness and quality indicators.

        Args:
            fact: Fact DataFrame with all data sources joined.

        Returns:
            Fact DataFrame with quality metrics added.
        """
        # Get available columns for conditional logic
        fact_columns = set(fact.columns)

        # Data source presence flags - handle missing columns gracefully
        fact = fact.withColumn("has_energy", F.col("energy_mwh").isNotNull())

        if "shortwave_radiation" in fact_columns:
            fact = fact.withColumn("has_weather", F.col("shortwave_radiation").isNotNull())
        else:
            fact = fact.withColumn("has_weather", F.lit(False))

        if "pm2_5" in fact_columns:
            fact = fact.withColumn("has_air_quality", F.col("pm2_5").isNotNull())
        else:
            fact = fact.withColumn("has_air_quality", F.lit(False))

        # Completeness percentage (33.33% per source)
        completeness = (
            F.when(F.col("has_energy"), F.lit(33.33)).otherwise(F.lit(0.0))
            + F.when(F.col("has_weather"), F.lit(33.33)).otherwise(F.lit(0.0))
            + F.when(F.col("has_air_quality"), F.lit(33.34)).otherwise(F.lit(0.0))
        )
        fact = fact.withColumn("completeness_pct", completeness.cast(dec(5, 2)))

        # Validity check: energy present and non-negative
        fact = fact.withColumn(
            "is_valid",
            F.col("has_energy") & (F.col("energy_mwh") >= F.lit(0.0)),
        )

        # Quality flag based on validity and completeness
        quality_flag = (
            F.when(~F.col("is_valid"), F.lit("BAD"))
            .when(F.col("completeness_pct") >= F.lit(90.0), F.lit("GOOD"))
            .when(F.col("completeness_pct") >= F.lit(50.0), F.lit("WARNING"))
            .otherwise(F.lit("BAD"))
        )
        fact = fact.withColumn("quality_flag", quality_flag)

        return fact

    # -------------------------------------------------------------------------
    # Derived metrics calculation
    # -------------------------------------------------------------------------
    def _calculate_derived_metrics(self, fact: DataFrame) -> DataFrame:
        """Calculate derived analytics metrics.

        Args:
            fact: Fact DataFrame with raw measurements.

        Returns:
            Fact DataFrame with derived metrics added.
        """
        fact_columns = set(fact.columns)

        # Convert irradiance: W/m² to kWh/m²·h
        if "shortwave_radiation" in fact_columns:
            fact = fact.withColumn(
                "irr_kwh_m2_hour",
                F.when(
                    F.col("shortwave_radiation").isNotNull(),
                    F.col("shortwave_radiation") / F.lit(1000.0),
                ).otherwise(F.lit(None)),
            )
        else:
            fact = fact.withColumn("irr_kwh_m2_hour", F.lit(None))

        # Convert sunshine duration: seconds to hours
        if "sunshine_duration" in fact_columns:
            fact = fact.withColumn(
                "sunshine_hours",
                F.when(
                    F.col("sunshine_duration").isNotNull(),
                    F.col("sunshine_duration") / F.lit(3600.0),
                ).otherwise(F.lit(None)),
            )
        else:
            fact = fact.withColumn("sunshine_hours", F.lit(None))

        # Capacity-weighted irradiance for Performance Ratio calculations
        if "total_capacity_mw" in fact_columns:
            fact = fact.withColumn(
                "yr_weighted_kwh",
                F.when(
                    F.col("irr_kwh_m2_hour").isNotNull()
                    & F.col("total_capacity_mw").isNotNull(),
                    F.col("irr_kwh_m2_hour") * F.col("total_capacity_mw") * F.lit(1000.0),
                ).otherwise(F.lit(None)),
            )
        else:
            fact = fact.withColumn("yr_weighted_kwh", F.lit(None))

        return fact

    # -------------------------------------------------------------------------
    # Audit and final projection
    # -------------------------------------------------------------------------
    def _add_audit_columns(self, fact: DataFrame) -> DataFrame:
        """Add audit timestamp columns.

        Args:
            fact: Fact DataFrame.

        Returns:
            Fact DataFrame with created_at and updated_at columns.
        """
        return fact.withColumn("created_at", F.current_timestamp()).withColumn(
            "updated_at", F.current_timestamp()
        )

    def _ensure_column(
        self,
        fact: DataFrame,
        col_name: str,
        cast_type: Optional[T.DataType] = None,
    ) -> Column:
        """Get column if exists, else return NULL literal.

        Args:
            fact: DataFrame to check for column.
            col_name: Column name to retrieve.
            cast_type: Optional type to cast to.

        Returns:
            Column expression (actual column or NULL).
        """
        if col_name in fact.columns:
            col_expr = F.col(col_name)
            if cast_type:
                return col_expr.cast(cast_type).alias(col_name)
            return col_expr
        else:
            if cast_type:
                return F.lit(None).cast(cast_type).alias(col_name)
            return F.lit(None).alias(col_name)

    def _project_final_columns(self, fact: DataFrame) -> DataFrame:
        """Project final schema with proper data types.

        Args:
            fact: Complete fact DataFrame.

        Returns:
            DataFrame with final Gold layer schema.
        """
        return fact.select(
            # Dimension keys
            self._ensure_column(fact, "facility_key"),
            self._ensure_column(fact, "date_key"),
            self._ensure_column(fact, "time_key"),
            self._ensure_column(fact, "aqi_category_key"),
            # Energy metrics
            self._ensure_column(fact, "energy_mwh", dec(12, 6)),
            self._ensure_column(fact, "intervals_count"),
            # Weather metrics
            self._ensure_column(fact, "shortwave_radiation", dec(10, 4)),
            self._ensure_column(fact, "direct_radiation", dec(10, 4)),
            self._ensure_column(fact, "diffuse_radiation", dec(10, 4)),
            self._ensure_column(fact, "direct_normal_irradiance", dec(10, 4)),
            self._ensure_column(fact, "irr_kwh_m2_hour", dec(10, 6)),
            self._ensure_column(fact, "sunshine_hours", dec(6, 3)),
            self._ensure_column(fact, "temperature_2m"),
            self._ensure_column(fact, "dew_point_2m"),
            self._ensure_column(fact, "humidity_2m"),
            self._ensure_column(fact, "cloud_cover"),
            self._ensure_column(fact, "cloud_cover_low"),
            self._ensure_column(fact, "cloud_cover_mid"),
            self._ensure_column(fact, "cloud_cover_high"),
            self._ensure_column(fact, "precipitation"),
            self._ensure_column(fact, "wind_speed_10m"),
            self._ensure_column(fact, "wind_direction_10m"),
            self._ensure_column(fact, "wind_gusts_10m"),
            self._ensure_column(fact, "pressure_msl"),
            # Air quality metrics
            self._ensure_column(fact, "pm2_5"),
            self._ensure_column(fact, "pm10"),
            self._ensure_column(fact, "dust"),
            self._ensure_column(fact, "nitrogen_dioxide"),
            self._ensure_column(fact, "ozone"),
            self._ensure_column(fact, "sulphur_dioxide"),
            self._ensure_column(fact, "carbon_monoxide"),
            self._ensure_column(fact, "uv_index"),
            self._ensure_column(fact, "uv_index_clear_sky"),
            self._ensure_column(fact, "aqi_value"),
            # Data quality
            self._ensure_column(fact, "is_valid"),
            self._ensure_column(fact, "quality_flag"),
            self._ensure_column(fact, "completeness_pct"),
            self._ensure_column(fact, "yr_weighted_kwh", dec(16, 6)),
            self._ensure_column(fact, "created_at"),
            self._ensure_column(fact, "updated_at"),
        )


__all__ = ["GoldFactSolarEnvironmentalLoader"]
