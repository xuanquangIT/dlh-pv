"""Silver loader producing clean_hourly_weather."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions


class SilverHourlyWeatherLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_weather"
    silver_table = "lh.silver.clean_hourly_weather"
    timestamp_column = "weather_timestamp"
    partition_cols = ("date_hour",)

    def __init__(self, options: Optional[LoadOptions] = None) -> None:
        if options is None:
            options = LoadOptions()
        else:
            options.target_file_size_mb = min(options.target_file_size_mb, self.DEFAULT_TARGET_FILE_SIZE_MB)
            options.max_records_per_file = min(options.max_records_per_file, self.DEFAULT_MAX_RECORDS_PER_FILE)
        super().__init__(options)

    def run(self) -> int:
        """Process bronze weather data in 7-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=7)

    _numeric_columns = {
        # Radiation bounds (W/m²)
        "shortwave_radiation": (0.0, 1000.0),  # Max ~1000 W/m² at Earth surface (1361 extraterrestrial)
        "direct_radiation": (0.0, 1000.0),
        "diffuse_radiation": (0.0, 800.0),    # Diffuse is typically lower
        "direct_normal_irradiance": (0.0, 950.0),  # Updated: 900→950 W/m² 
        
        # Temperature bounds (°C) - Earth's atmospheric limits
        "temperature_2m": (-50.0, 60.0),       # Min: -50°C (Siberia record), Max: 60°C (Death Valley)
        "dew_point_2m": (-50.0, 60.0),         # Physical limit: <= ambient temperature
        "wet_bulb_temperature_2m": (-50.0, 60.0),  # Always <= ambient temperature
        
        # Cloud cover (%)
        "cloud_cover": (0.0, 100.0),           # Percentage
        "cloud_cover_low": (0.0, 100.0),       # Low level clouds (< 2km)
        "cloud_cover_mid": (0.0, 100.0),       # Mid level clouds (2-6km)
        "cloud_cover_high": (0.0, 100.0),      # High level clouds (> 6km)
        
        # Precipitation (mm/hour)
        "precipitation": (0.0, 1000.0),        # Heavy rain: ~400mm/hr extreme
        "sunshine_duration": (0.0, 3600.0),    # Seconds in hour (0-3600)
        "total_column_integrated_water_vapour": (0.0, 80.0),  # Total column water vapor (mm)
        
        # Wind bounds (m/s) - Based on Beaufort scale
        "wind_speed_10m": (0.0, 60.0),         # Max: ~60 m/s (hurricane force, category 5)
        "wind_direction_10m": (0.0, 360.0),    # Compass direction (0-360°)
        "wind_gusts_10m": (0.0, 120.0),        # Peak gust (up to ~120 m/s in extreme tornados)
        
        # Pressure (hPa) - Atmospheric pressure
        "pressure_msl": (800.0, 1100.0),       # Sea level: 800-1100 hPa (typhoon to high pressure)
    }
    
    # Phase 2-3: Enhanced radiation anomaly detection constants ✅
    # Clear sky detection and radiation component ratio validation
    CLEAR_SKY_DNI_THRESHOLD = 500.0           # W/m² - threshold for "clear sky" conditions
    NIGHT_RADIATION_THRESHOLD = 50.0          # W/m² - max allowed shortwave at night
    MIN_SHORTWAVE_RATIO = 0.25                # Min ratio of shortwave/DNI (conservative)
    MAX_SHORTWAVE_RATIO = 0.55                # Max ratio of shortwave/DNI (with diffuse)
    REALISTIC_SHORTWAVE_MIN = 200.0           # W/m² - unrealistic if < this on clear days
    UNREALISTIC_RADIATION_MAX = 1000.0        # W/m² - never exceeds this on Earth surface

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

        # Bronze timestamps are already in facility's local timezone
        prepared_base = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.col("weather_timestamp").cast("timestamp").alias("timestamp_local"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("weather_timestamp").isNotNull())
        )
        
        # No conversion needed - aggregate by local time
        prepared = (
            prepared_base
            .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_local")))
            .withColumn("date", F.to_date(F.col("timestamp_local")))
        )

        # Quality Validation: Check numeric columns within bounds
        # Pre-compute all bounds checks in single pass (NO intermediate withColumn)
        is_valid_bounds = F.lit(True)
        bound_issues_list = []
        
        for column, (min_val, max_val) in self._numeric_columns.items():
            col_expr = F.col(column)
            col_valid = col_expr.isNull() | ((col_expr >= min_val) & (col_expr <= max_val))
            is_valid_bounds = is_valid_bounds & col_valid
            
            # Collect issues only for columns that are OUT_OF_BOUNDS
            bound_issues_list.append(
                F.when((col_expr.isNotNull()) & ~col_valid, F.concat(F.lit(column), F.lit("_OUT_OF_BOUNDS")))
            )
        
        # Pre-compute hour_of_day for radiation checks
        hour_of_day = F.hour(F.col("timestamp_local"))
        
        # Efficient radiation checks - all computed in one pass
        is_night = (hour_of_day < 6) | (hour_of_day >= 22)
        
        # Radiation component consistency
        is_unrealistic_rad = F.col("shortwave_radiation") > self.UNREALISTIC_RADIATION_MAX
        is_sunrise_spike = (hour_of_day == 6) & (F.col("shortwave_radiation") > 500)
        
        is_inconsistent_radiation = (
            (F.col("direct_normal_irradiance").isNotNull()) & 
            (F.col("shortwave_radiation").isNotNull()) &
            (F.col("direct_normal_irradiance") > 900) &
            (F.col("shortwave_radiation") < 200)
        )
        
        # Night radiation check
        is_night_rad_high = is_night & (F.col("shortwave_radiation") > self.NIGHT_RADIATION_THRESHOLD)
        
        # Bidirectional radiation checks (both directions in one pass)
        is_high_dni_low_shortwave = (
            (F.col("direct_normal_irradiance") > self.CLEAR_SKY_DNI_THRESHOLD) &
            (F.col("shortwave_radiation").isNotNull()) &
            (
                (F.col("shortwave_radiation") < F.col("direct_normal_irradiance") * self.MIN_SHORTWAVE_RATIO) |
                (F.col("shortwave_radiation") < self.REALISTIC_SHORTWAVE_MIN)
            )
        )
        
        is_high_shortwave_low_dni = (
            (F.col("shortwave_radiation") > 700) &
            (F.col("direct_normal_irradiance").isNotNull()) &
            (F.col("direct_normal_irradiance") < 300)
        )
        
        # Build quality expressions
        quality_issues_expr = F.trim(F.concat_ws("|",
            *bound_issues_list,
            F.when(is_night_rad_high, "NIGHT_RADIATION_SPIKE"),
            F.when(is_unrealistic_rad, "UNREALISTIC_RADIATION"),
            F.when(is_high_dni_low_shortwave, "RADIATION_COMPONENT_MISMATCH_LOW_SW"),
            F.when(is_high_shortwave_low_dni, "RADIATION_COMPONENT_MISMATCH_LOW_DNI"),
            F.when(is_sunrise_spike, "SUNRISE_SPIKE_ANOMALY"),
            F.when(is_inconsistent_radiation, "INCONSISTENT_RADIATION_COMPONENTS"),
        ))
        
        quality_flag_expr = F.when(
            is_unrealistic_rad | is_sunrise_spike | is_inconsistent_radiation | ~is_valid_bounds,
            "REJECT"
        ).when(
            is_night_rad_high | is_high_shortwave_low_dni,
            "CAUTION"
        ).otherwise("GOOD")
        
        is_valid_expr = is_valid_bounds & ~(is_unrealistic_rad | is_sunrise_spike | is_inconsistent_radiation)
        
        # Build select expressions - must include ALL numeric columns to match silver schema!
        select_exprs = [
            "facility_code",
            "facility_name",
            F.col("timestamp_local").alias("timestamp"),
            "date_hour",
            "date"
        ]
        
        # Add ALL numeric columns (required by silver table schema)
        for column in self._numeric_columns.keys():
            if column in prepared.columns:
                select_exprs.append(F.round(F.col(column), 4).alias(column))
            else:
                # If column doesn't exist, add NULL with proper type
                select_exprs.append(F.lit(None).cast("double").alias(column))
        
        # Add quality columns
        select_exprs.extend([
            is_valid_expr.alias("is_valid"),
            quality_flag_expr.alias("quality_flag"),
            quality_issues_expr.alias("quality_issues"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ])
        
        return prepared.select(*select_exprs)


__all__ = ["SilverHourlyWeatherLoader"]
