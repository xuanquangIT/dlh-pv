"""
Outlier detection and data quality validation for Silver layer transformation.

Provides utilities for identifying and flagging anomalous records based on:
- Physical bounds validation
- Temporal anomalies
- Statistical outliers (IQR, Z-score)
- Diurnal pattern validation (solar-specific)
"""

from __future__ import annotations

from typing import Dict, Optional, Set, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Physical bounds for each variable (min, max)
PHYSICAL_BOUNDS: Dict[str, Tuple[float, Optional[float]]] = {
    # Energy/Power
    "value": (0.0, None),
    "energy_mwh": (0.0, None),
    "power_mw": (0.0, None),
    
    # Weather - Radiation (W/m²)
    "shortwave_radiation": (0.0, 1000.0),  # Max ~1000 W/m² at Earth surface
    "direct_radiation": (0.0, 1000.0),    # Max ~1000 W/m² at Earth surface
    "diffuse_radiation": (0.0, 800.0),    # Diffuse typically lower than direct
    "direct_normal_irradiance": (0.0, 950.0),  # Max ~950 W/m² in clear-sky conditions
    "terrestrial_radiation": (0.0, 100.0),
    
    # Weather - Temperature (°C)
    "temperature_2m": (-50.0, 60.0),
    "dew_point_2m": (-50.0, 60.0),
    "wet_bulb_temperature_2m": (-50.0, 60.0),
    
    # Weather - Cloud & Precipitation
    "cloud_cover": (0.0, 100.0),  # %
    "cloud_cover_low": (0.0, 100.0),
    "cloud_cover_mid": (0.0, 100.0),
    "cloud_cover_high": (0.0, 100.0),
    "precipitation": (0.0, 1000.0),  # mm
    "total_column_integrated_water_vapour": (0.0, 100.0),
    "sunshine_duration": (0.0, 3600.0),  # seconds
    "pressure_msl": (800.0, 1100.0),  # hPa
    "boundary_layer_height": (0.0, 10000.0),  # meters
    
    # Weather - Wind
    "wind_speed_10m": (0.0, 60.0),  # m/s
    "wind_direction_10m": (0.0, 360.0),  # degrees
    "wind_gusts_10m": (0.0, 120.0),  # m/s
    
    # Air Quality (μg/m³ or ppb equivalent)
    "pm2_5": (0.0, 500.0),
    "pm10": (0.0, 500.0),
    "dust": (0.0, 500.0),
    "nitrogen_dioxide": (0.0, 500.0),
    "ozone": (0.0, 500.0),
    "sulphur_dioxide": (0.0, 500.0),
    "carbon_monoxide": (0.0, 10000.0),
    "uv_index": (0.0, 15.0),
    "uvi_clear_sky": (0.0, 15.0),
}

# Diurnal pattern rules for solar PV
DIURNAL_RULES = {
    "night_hours_start": 22,  # 22:00
    "night_hours_end": 6,     # 06:00
    "peak_hours_start": 10,   # 10:00
    "peak_hours_end": 15,     # 15:00
    "max_night_energy_mwh": 0.1,  # Max allowed energy during night
    "min_peak_energy_mwh": 1.0,   # Min expected energy during peak hours
}


def add_physical_bounds_flag(
    df: DataFrame,
    bounds: Optional[Dict[str, Tuple[float, Optional[float]]]] = None,
) -> DataFrame:
    """
    Add is_within_bounds flag for each numeric column.
    
    Args:
        df: Input DataFrame
        bounds: Dict of {column_name: (min_value, max_value)}
    
    Returns:
        DataFrame with is_within_bounds flag added
    """
    bounds = bounds or PHYSICAL_BOUNDS
    
    conditions = [F.lit(True)]  # Start with True
    
    for col, (min_val, max_val) in bounds.items():
        if col not in df.columns:
            continue
        
        # Build condition: (col IS NULL) OR (min <= col <= max)
        col_expr = F.col(col)
        if min_val is not None and max_val is not None:
            condition = col_expr.isNull() | ((col_expr >= F.lit(min_val)) & (col_expr <= F.lit(max_val)))
        elif min_val is not None:
            condition = col_expr.isNull() | (col_expr >= F.lit(min_val))
        else:
            condition = col_expr.isNull() | (col_expr <= F.lit(max_val))
        
        conditions.append(condition)
    
    # Combine all conditions with AND
    final_condition = conditions[0]
    for cond in conditions[1:]:
        final_condition = final_condition & cond
    
    return df.withColumn("is_within_bounds", final_condition)


def add_temporal_flags(df: DataFrame, timestamp_col: str) -> DataFrame:
    """
    Add temporal quality flags: null_timestamp, future_timestamp.
    
    Args:
        df: Input DataFrame
        timestamp_col: Name of timestamp column
    
    Returns:
        DataFrame with temporal flags added
    """
    timestamp_expr = F.col(timestamp_col)
    current_ts = F.current_timestamp()
    
    df = df.withColumn(
        "null_timestamp",
        timestamp_expr.isNull()
    ).withColumn(
        "future_timestamp",
        (timestamp_expr > current_ts) & timestamp_expr.isNotNull()
    ).withColumn(
        "valid_timestamp",
        timestamp_expr.isNotNull() & (timestamp_expr <= current_ts)
    )
    
    return df


def add_diurnal_pattern_flags(
    df: DataFrame,
    timestamp_col: str,
    energy_col: str,
    rules: Optional[Dict] = None,
) -> DataFrame:
    """
    Add flags for solar diurnal pattern violations.
    
    Flags:
    - night_high_energy: High generation during night hours (should be ~0)
    - peak_low_energy: Low generation during peak hours (should be high)
    
    Args:
        df: Input DataFrame
        timestamp_col: Name of timestamp column
        energy_col: Name of energy column
        rules: Diurnal pattern rules
    
    Returns:
        DataFrame with diurnal pattern flags
    """
    rules = rules or DIURNAL_RULES
    
    # Extract hour
    df = df.withColumn("hour_of_day", F.hour(F.col(timestamp_col)))
    
    # Night hours mask: 22:00-06:00
    night_start = rules["night_hours_start"]
    night_end = rules["night_hours_end"]
    night_mask = (F.col("hour_of_day") >= F.lit(night_start)) | (F.col("hour_of_day") < F.lit(night_end))
    
    # Peak hours mask: 10:00-15:00
    peak_start = rules["peak_hours_start"]
    peak_end = rules["peak_hours_end"]
    peak_mask = (F.col("hour_of_day") >= F.lit(peak_start)) & (F.col("hour_of_day") < F.lit(peak_end))
    
    # Check violations
    df = df.withColumn(
        "night_high_energy",
        night_mask & (F.col(energy_col) > F.lit(rules["max_night_energy_mwh"]))
    ).withColumn(
        "peak_low_energy",
        peak_mask & (F.col(energy_col) < F.lit(rules["min_peak_energy_mwh"]))
    )
    
    return df


def add_statistical_outlier_flags(
    df: DataFrame,
    value_col: str,
    method: str = "tukey",
    z_threshold: float = 3.0,
) -> DataFrame:
    """
    Add statistical outlier flags using Tukey's IQR or Z-score method.
    
    Args:
        df: Input DataFrame
        value_col: Name of numeric column to check
        method: "tukey" (IQR-based) or "zscore" (3-sigma)
        z_threshold: Z-score threshold for outliers
    
    Returns:
        DataFrame with statistical_outlier flag
    """
    if value_col not in df.columns:
        return df.withColumn("statistical_outlier", F.lit(False))
    
    if method == "tukey":
        # Tukey's IQR method
        q1 = F.percentile_approx(F.col(value_col), F.lit(0.25))
        q3 = F.percentile_approx(F.col(value_col), F.lit(0.75))
        iqr = q3 - q1
        lower_fence = q1 - (F.lit(1.5) * iqr)
        upper_fence = q3 + (F.lit(1.5) * iqr)
        
        # Use window function for per-partition calculation
        from pyspark.sql import Window
        window = Window.partitionBy()  # Global window
        
        q1_val = F.percentile_approx(F.col(value_col), 0.25).over(window)
        q3_val = F.percentile_approx(F.col(value_col), 0.75).over(window)
        iqr_val = q3_val - q1_val
        
        lower_fence_val = q1_val - (1.5 * iqr_val)
        upper_fence_val = q3_val + (1.5 * iqr_val)
        
        outlier_flag = (
            (F.col(value_col) < lower_fence_val) |
            (F.col(value_col) > upper_fence_val)
        ) & F.col(value_col).isNotNull()
        
        return df.withColumn("statistical_outlier", outlier_flag)
    
    # Z-score method (simplified for Spark)
    elif method == "zscore":
        from pyspark.sql import Window
        window = Window.partitionBy()
        
        mean_val = F.avg(F.col(value_col)).over(window)
        stddev_val = F.stddev(F.col(value_col)).over(window)
        
        z_score = F.abs((F.col(value_col) - mean_val) / (stddev_val + F.lit(1e-10)))
        outlier_flag = (z_score > F.lit(z_threshold)) & F.col(value_col).isNotNull()
        
        return df.withColumn("statistical_outlier", outlier_flag)
    
    else:
        raise ValueError(f"Unknown method: {method}. Use 'tukey' or 'zscore'")


def add_composite_quality_flag(
    df: DataFrame,
    timestamp_col: str,
    value_col: str,
    include_diurnal: bool = False,
) -> DataFrame:
    """
    Add composite quality_flag combining all validations.
    
    Quality flags:
    - "GOOD": Passes all checks
    - "CAUTION": Passes bounds but has statistical issues or diurnal anomalies
    - "REJECT": Fails physical bounds or temporal checks
    
    Args:
        df: Input DataFrame (should have is_within_bounds, valid_timestamp, etc.)
        timestamp_col: Name of timestamp column
        value_col: Name of value column
        include_diurnal: Include diurnal pattern validation
    
    Returns:
        DataFrame with quality_flag and quality_issues columns
    """
    # Start with GOOD
    df = df.withColumn("quality_flag", F.lit("GOOD"))
    df = df.withColumn("quality_issues", F.lit(""))
    
    # Check physical bounds - REJECT if violated
    if "is_within_bounds" in df.columns:
        df = df.withColumn(
            "quality_flag",
            F.when(
                ~F.col("is_within_bounds"),
                F.lit("REJECT")
            ).otherwise(F.col("quality_flag"))
        ).withColumn(
            "quality_issues",
            F.when(
                ~F.col("is_within_bounds"),
                F.concat(F.col("quality_issues"), F.lit("OUT_OF_BOUNDS|"))
            ).otherwise(F.col("quality_issues"))
        )
    
    # Check timestamps - REJECT if invalid
    if "valid_timestamp" in df.columns:
        df = df.withColumn(
            "quality_flag",
            F.when(
                ~F.col("valid_timestamp"),
                F.lit("REJECT")
            ).otherwise(F.col("quality_flag"))
        ).withColumn(
            "quality_issues",
            F.when(
                ~F.col("valid_timestamp"),
                F.concat(F.col("quality_issues"), F.lit("INVALID_TIMESTAMP|"))
            ).otherwise(F.col("quality_issues"))
        )
    
    # Statistical outliers - CAUTION if flag exists and quality_flag is still GOOD
    if "statistical_outlier" in df.columns:
        df = df.withColumn(
            "quality_flag",
            F.when(
                F.col("statistical_outlier") & (F.col("quality_flag") == F.lit("GOOD")),
                F.lit("CAUTION")
            ).otherwise(F.col("quality_flag"))
        ).withColumn(
            "quality_issues",
            F.when(
                F.col("statistical_outlier"),
                F.concat(F.col("quality_issues"), F.lit("STATISTICAL_OUTLIER|"))
            ).otherwise(F.col("quality_issues"))
        )
    
    # Diurnal pattern issues - CAUTION if detected and quality_flag is still GOOD
    if include_diurnal:
        if "night_high_energy" in df.columns:
            df = df.withColumn(
                "quality_flag",
                F.when(
                    F.col("night_high_energy") & (F.col("quality_flag") == F.lit("GOOD")),
                    F.lit("CAUTION")
                ).otherwise(F.col("quality_flag"))
            ).withColumn(
                "quality_issues",
                F.when(
                    F.col("night_high_energy"),
                    F.concat(F.col("quality_issues"), F.lit("NIGHT_HIGH_ENERGY|"))
                ).otherwise(F.col("quality_issues"))
            )
    
    # Clean up quality_issues (remove trailing |)
    df = df.withColumn(
        "quality_issues",
        F.trim(F.regexp_replace(F.col("quality_issues"), r"\|+$", ""))
    )
    
    return df


def filter_by_quality(
    df: DataFrame,
    quality_flag_col: str = "quality_flag",
    keep_good: bool = True,
    keep_caution: bool = False,
    keep_reject: bool = False,
) -> DataFrame:
    """
    Filter DataFrame by quality flags.
    
    Args:
        df: Input DataFrame with quality_flag column
        quality_flag_col: Name of quality flag column
        keep_good: Keep GOOD records
        keep_caution: Keep CAUTION records
        keep_reject: Keep REJECT records
    
    Returns:
        Filtered DataFrame
    """
    conditions = []
    
    if keep_good:
        conditions.append(F.col(quality_flag_col) == F.lit("GOOD"))
    if keep_caution:
        conditions.append(F.col(quality_flag_col) == F.lit("CAUTION"))
    if keep_reject:
        conditions.append(F.col(quality_flag_col) == F.lit("REJECT"))
    
    if not conditions:
        return df.limit(0)  # Return empty DataFrame
    
    # Combine conditions with OR
    final_condition = conditions[0]
    for cond in conditions[1:]:
        final_condition = final_condition | cond
    
    return df.filter(final_condition)


def get_quality_summary(
    df: DataFrame,
    quality_flag_col: str = "quality_flag",
) -> Dict:
    """
    Get summary statistics of quality flags.
    
    Args:
        df: Input DataFrame with quality_flag column
        quality_flag_col: Name of quality flag column
    
    Returns:
        Dict with counts and percentages by flag
    """
    total = df.count()
    
    if total == 0:
        return {
            "total": 0,
            "GOOD": 0,
            "CAUTION": 0,
            "REJECT": 0,
            "GOOD_pct": 0.0,
            "CAUTION_pct": 0.0,
            "REJECT_pct": 0.0,
        }
    
    counts = df.groupBy(quality_flag_col).count().collect()
    flag_counts = {row[quality_flag_col]: row["count"] for row in counts}
    
    good = flag_counts.get("GOOD", 0)
    caution = flag_counts.get("CAUTION", 0)
    reject = flag_counts.get("REJECT", 0)
    
    return {
        "total": total,
        "GOOD": good,
        "CAUTION": caution,
        "REJECT": reject,
        "GOOD_pct": (good / total * 100) if total > 0 else 0.0,
        "CAUTION_pct": (caution / total * 100) if total > 0 else 0.0,
        "REJECT_pct": (reject / total * 100) if total > 0 else 0.0,
    }


__all__ = [
    "PHYSICAL_BOUNDS",
    "DIURNAL_RULES",
    "add_physical_bounds_flag",
    "add_temporal_flags",
    "add_diurnal_pattern_flags",
    "add_statistical_outlier_flags",
    "add_composite_quality_flag",
    "filter_by_quality",
    "get_quality_summary",
]
