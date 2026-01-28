#!/usr/bin/env python3
"""Validation utilities for Silver layer data quality checks.

This module provides reusable validators for numeric bounds checking and
logical consistency validation across weather, energy, and air quality data.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from pyspark.sql import Column
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)


class NumericBoundsValidator:
    """Validates numeric columns against min/max bounds from configuration.
    
    Example:
        >>> bounds_config = {
        ...     "temperature_2m": {"min": -10.0, "max": 50.0},
        ...     "wind_speed_10m": {"min": 0.0, "max": 50.0}
        ... }
        >>> validator = NumericBoundsValidator(bounds_config)
        >>> is_valid, issues = validator.validate_all(df.columns)
    """

    def __init__(self, bounds_config: Dict[str, Dict[str, float]]) -> None:
        """Initialize validator with bounds configuration.
        
        Args:
            bounds_config: Dictionary mapping column names to {"min": float, "max": float}.
        """
        self.bounds = bounds_config
        LOGGER.debug("Initialized NumericBoundsValidator with %d columns", len(bounds_config))

    def validate_all(
        self, available_columns: List[str]
    ) -> Tuple[Column, Column]:
        """Validate all configured columns that exist in the DataFrame.
        
        Args:
            available_columns: List of column names available in DataFrame.
            
        Returns:
            Tuple of (is_valid_column, issues_string_column):
                - is_valid_column: Boolean column indicating if all bounds are satisfied
                - issues_string_column: String column with pipe-separated issue labels
                
        Example:
            >>> is_valid, issues = validator.validate_all(df.columns)
            >>> result = df.select("*", is_valid.alias("is_valid_bounds"), 
            ...                    issues.alias("bound_issues"))
        """
        is_valid = F.lit(True)
        issue_parts = []
        
        validated_count = 0
        for column_name, bounds in self.bounds.items():
            if column_name not in available_columns:
                continue
                
            min_val = bounds.get("min")
            max_val = bounds.get("max")
            
            if min_val is None and max_val is None:
                continue
            
            col_valid, col_issue = self.validate_single(
                F.col(column_name), column_name, min_val, max_val
            )
            is_valid = is_valid & col_valid
            issue_parts.append(col_issue)
            validated_count += 1
        
        # Use concat_ws to join, which automatically filters empty strings
        issues = F.concat_ws("|", *issue_parts) if issue_parts else F.lit("")
        
        LOGGER.debug("Validated %d numeric columns", validated_count)
        return is_valid, issues

    def validate_single(
        self,
        col: Column,
        col_name: str,
        min_val: Optional[float],
        max_val: Optional[float],
    ) -> Tuple[Column, Column]:
        """Validate a single column against bounds.
        
        Args:
            col: Column to validate.
            col_name: Name of column (for issue labeling).
            min_val: Minimum valid value (None = no lower bound).
            max_val: Maximum valid value (None = no upper bound).
            
        Returns:
            Tuple of (is_valid_column, issue_label_column):
                - is_valid_column: Boolean indicating if value is within bounds or null
                - issue_label_column: String with issue label or empty string
        """
        # Null values are considered valid
        conditions = []
        
        if min_val is not None:
            conditions.append(col >= F.lit(min_val))
        if max_val is not None:
            conditions.append(col <= F.lit(max_val))
        
        if not conditions:
            return F.lit(True), F.lit("")
        
        # Combine conditions with AND
        bounds_check = conditions[0]
        for cond in conditions[1:]:
            bounds_check = bounds_check & cond
        
        is_valid = col.isNull() | bounds_check
        issue_label = F.when(
            col.isNotNull() & ~bounds_check,
            F.lit(f"{col_name}_OUT_OF_BOUNDS")
        ).otherwise(F.lit(""))
        
        return is_valid, issue_label


class LogicValidator:
    """Validates logical consistency for weather and energy data patterns.
    
    This class provides static methods for domain-specific validation rules
    that go beyond simple numeric bounds checking.
    """

    @staticmethod
    def check_night_radiation(
        timestamp_col: Column,
        radiation_col: Column,
        threshold: float = 100.0,
        night_hours: Optional[List[int]] = None,
    ) -> Tuple[Column, Column]:
        """Check for nighttime radiation spikes (likely sensor errors).
        
        Args:
            timestamp_col: Timestamp column (local time).
            radiation_col: Radiation column to check (e.g., shortwave_radiation).
            threshold: Radiation threshold in W/m² (default: 100.0).
            night_hours: List of hours considered "night" (default: [22,23,0,1,2,3,4,5]).
            
        Returns:
            Tuple of (has_issue_column, issue_label_column).
        """
        if night_hours is None:
            night_hours = [22, 23, 0, 1, 2, 3, 4, 5]
        
        hour_col = F.hour(timestamp_col)
        is_night = hour_col.isin(night_hours)
        has_spike = is_night & (radiation_col > F.lit(threshold))
        
        issue_label = F.when(has_spike, F.lit("NIGHT_RADIATION")).otherwise(F.lit(""))
        return has_spike, issue_label

    @staticmethod
    def check_radiation_consistency(
        direct_col: Column,
        diffuse_col: Column,
        shortwave_col: Column,
        tolerance_factor: float = 1.05,
    ) -> Tuple[Column, Column]:
        """Check if direct + diffuse radiation exceeds shortwave radiation.
        
        Physical principle: Direct + Diffuse ≈ Shortwave (Global Horizontal Irradiance).
        Small tolerance allowed for measurement errors.
        
        Args:
            direct_col: Direct radiation column.
            diffuse_col: Diffuse radiation column.
            shortwave_col: Shortwave (global horizontal) radiation column.
            tolerance_factor: Allowed overshoot factor (default: 1.05 = 5% tolerance).
            
        Returns:
            Tuple of (has_issue_column, issue_label_column).
        """
        sum_radiation = direct_col + diffuse_col
        has_inconsistency = sum_radiation > (shortwave_col * F.lit(tolerance_factor))
        
        issue_label = F.when(
            has_inconsistency, F.lit("RADIATION_INCONSISTENCY")
        ).otherwise(F.lit(""))
        
        return has_inconsistency, issue_label

    @staticmethod
    def check_cloud_radiation_mismatch(
        timestamp_col: Column,
        cloud_col: Column,
        radiation_col: Column,
        peak_hours: Optional[List[int]] = None,
        cloud_threshold: float = 98.0,
        radiation_threshold: float = 600.0,
    ) -> Tuple[Column, Column]:
        """Check for high cloud cover with unexpected radiation during peak sun hours.
        
        Args:
            timestamp_col: Timestamp column (local time).
            cloud_col: Cloud cover percentage column.
            radiation_col: Radiation column (e.g., shortwave_radiation).
            peak_hours: Hours considered "peak sun" (default: [10,11,12,13,14]).
            cloud_threshold: Cloud cover % threshold (default: 98.0).
            radiation_threshold: Radiation threshold in W/m² (default: 600.0).
            
        Returns:
            Tuple of (has_issue_column, issue_label_column).
        """
        if peak_hours is None:
            peak_hours = [10, 11, 12, 13, 14]
        
        hour_col = F.hour(timestamp_col)
        is_peak_sun = hour_col.isin(peak_hours)
        
        has_mismatch = (
            is_peak_sun
            & (cloud_col > F.lit(cloud_threshold))
            & (radiation_col < F.lit(radiation_threshold))
        )
        
        issue_label = F.when(
            has_mismatch, F.lit("CLOUD_MEASUREMENT_INCONSISTENCY")
        ).otherwise(F.lit(""))
        
        return has_mismatch, issue_label

    @staticmethod
    def check_extreme_temperature(
        temp_col: Column,
        min_threshold: float = -10.0,
        max_threshold: float = 45.0,
    ) -> Tuple[Column, Column]:
        """Check for extreme temperature values outside expected range.
        
        Args:
            temp_col: Temperature column.
            min_threshold: Minimum temperature threshold in °C (default: -10.0).
            max_threshold: Maximum temperature threshold in °C (default: 45.0).
            
        Returns:
            Tuple of (has_issue_column, issue_label_column).
        """
        is_extreme = (temp_col < F.lit(min_threshold)) | (temp_col > F.lit(max_threshold))
        
        issue_label = F.when(is_extreme, F.lit("EXTREME_TEMPERATURE")).otherwise(F.lit(""))
        return is_extreme, issue_label

    @staticmethod
    def check_night_energy(
        timestamp_col: Column,
        energy_col: Column,
        threshold: float = 1.0,
        night_hours: Optional[List[int]] = None,
    ) -> Tuple[Column, Column]:
        """Check for energy production during nighttime hours (solar panels should not produce).
        
        Args:
            timestamp_col: Timestamp column (local time).
            energy_col: Energy production column (MWh).
            threshold: Energy threshold in MWh (default: 1.0).
            night_hours: List of hours considered "night" (default: [22,23,0,1,2,3,4,5]).
            
        Returns:
            Tuple of (has_issue_column, issue_label_column).
        """
        if night_hours is None:
            night_hours = [22, 23, 0, 1, 2, 3, 4, 5]
        
        hour_col = F.hour(timestamp_col)
        is_night = hour_col.isin(night_hours)
        has_anomaly = is_night & (energy_col > F.lit(threshold))
        
        issue_label = F.when(has_anomaly, F.lit("NIGHT_ENERGY")).otherwise(F.lit(""))
        return has_anomaly, issue_label

    @staticmethod
    def check_daytime_zero_energy(
        timestamp_col: Column,
        energy_col: Column,
        daytime_hours: Optional[List[int]] = None,
    ) -> Tuple[Column, Column]:
        """Check for zero energy production during daytime hours.
        
        Args:
            timestamp_col: Timestamp column (local time).
            energy_col: Energy production column (MWh).
            daytime_hours: Hours considered "daytime" (default: [8-17]).
            
        Returns:
            Tuple of (has_issue_column, issue_label_column).
        """
        if daytime_hours is None:
            daytime_hours = list(range(8, 18))  # 8-17
        
        hour_col = F.hour(timestamp_col)
        is_daytime = hour_col.isin(daytime_hours)
        has_zero = is_daytime & (energy_col == F.lit(0.0))
        
        issue_label = F.when(has_zero, F.lit("DAYTIME_ZERO_ENERGY")).otherwise(F.lit(""))
        return has_zero, issue_label

    @staticmethod
    def check_equipment_downtime(
        timestamp_col: Column,
        energy_col: Column,
        peak_hours: Optional[List[int]] = None,
    ) -> Tuple[Column, Column]:
        """Check for zero energy during peak production hours (equipment failure).
        
        Args:
            timestamp_col: Timestamp column (local time).
            energy_col: Energy production column (MWh).
            peak_hours: Hours considered "peak" (default: [10,11,12,13,14]).
            
        Returns:
            Tuple of (has_issue_column, issue_label_column).
        """
        if peak_hours is None:
            peak_hours = [10, 11, 12, 13, 14]
        
        hour_col = F.hour(timestamp_col)
        is_peak = hour_col.isin(peak_hours)
        has_downtime = is_peak & (energy_col == F.lit(0.0))
        
        issue_label = F.when(has_downtime, F.lit("EQUIPMENT_DOWNTIME")).otherwise(F.lit(""))
        return has_downtime, issue_label


__all__ = ["NumericBoundsValidator", "LogicValidator"]
