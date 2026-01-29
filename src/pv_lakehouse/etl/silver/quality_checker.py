#!/usr/bin/env python3
"""Quality flag assignment utilities for Silver layer data quality management.

This module provides centralized logic for assigning quality flags (BAD/WARNING/GOOD)
based on validation results across all Silver layer loaders.
"""
from __future__ import annotations

import logging
from typing import List

from pyspark.sql import Column
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)


class QualityFlagAssigner:
    """Assigns quality flags to data based on validation results.
    
    Quality flag tiers:
        - BAD: Invalid/impossible values (out of bounds, negative energy, etc.)
        - WARNING: Anomalous but potentially valid (logical inconsistencies, low production)
        - GOOD: All validations pass
    
    Example:
        >>> assigner = QualityFlagAssigner()
        >>> quality_flag = assigner.assign_three_tier(
        ...     is_valid_bounds=F.col("is_valid_bounds"),
        ...     has_severe_issue=is_night_rad | out_of_bounds,
        ...     has_warning_issue=is_cloud_mismatch | is_extreme_temp
        ... )
        >>> df = df.withColumn("quality_flag", quality_flag)
    """

    @staticmethod
    def assign_three_tier(
        is_valid_bounds: Column,
        has_severe_issue: Column,
        has_warning_issue: Column,
    ) -> Column:
        """Assign three-tier quality flag (BAD/WARNING/GOOD).
        
        Args:
            is_valid_bounds: Boolean column indicating if all numeric bounds are valid.
            has_severe_issue: Boolean column indicating severe quality issues (BAD flag).
            has_warning_issue: Boolean column indicating warning-level issues.
            
        Returns:
            Column with quality flag string: "BAD", "WARNING", or "GOOD".
            
        Logic:
            - BAD: bounds invalid OR severe issue present
            - WARNING: bounds valid AND no severe issues BUT warning issue present
            - GOOD: all validations pass
        """
        LOGGER.debug("Assigning three-tier quality flags (BAD/WARNING/GOOD)")
        
        return (
            F.when(~is_valid_bounds | has_severe_issue, F.lit("BAD"))
            .when(has_warning_issue, F.lit("WARNING"))
            .otherwise(F.lit("GOOD"))
        )

    @staticmethod
    def assign_binary(
        is_valid: Column,
        good_label: str = "GOOD",
        bad_label: str = "WARNING",
    ) -> Column:
        """Assign binary quality flag (customizable labels).
        
        Args:
            is_valid: Boolean column indicating if data is valid.
            good_label: Label for valid data (default: "GOOD").
            bad_label: Label for invalid data (default: "WARNING").
            
        Returns:
            Column with quality flag string.
            
        Note:
            Air quality loader uses GOOD/WARNING (no BAD tier).
        """
        LOGGER.debug("Assigning binary quality flags (%s/%s)", good_label, bad_label)
        
        return F.when(is_valid, F.lit(good_label)).otherwise(F.lit(bad_label))

    @staticmethod
    def build_issues_string(issue_columns: List[Column]) -> Column:
        """Build pipe-separated quality issues string from multiple issue columns.
        
        Args:
            issue_columns: List of Column objects containing issue labels (or empty strings).
            
        Returns:
            Column with pipe-separated concatenated issues (empty strings filtered out).
            
        Example:
            >>> issues = assigner.build_issues_string([
            ...     bound_issues,
            ...     F.when(is_night_rad, F.lit("NIGHT_RADIATION_SPIKE")).otherwise(F.lit("")),
            ...     F.when(is_cloud_mismatch, F.lit("CLOUD_INCONSISTENCY")).otherwise(F.lit(""))
            ... ])
            >>> df = df.withColumn("quality_issues", issues)
        """
        if not issue_columns:
            return F.lit("")
        
        # Use concat_ws which automatically filters out empty strings and nulls
        return F.concat_ws("|", *issue_columns)


class QualityMetrics:
    """Helper class for computing quality metrics and statistics.
    
    This class provides utility methods for analyzing quality flags across datasets.
    """

    @staticmethod
    def count_by_flag(df_with_quality_flag, quality_flag_col: str = "quality_flag") -> dict:
        """Count records by quality flag.
        
        Args:
            df_with_quality_flag: DataFrame with quality_flag column.
            quality_flag_col: Name of quality flag column (default: "quality_flag").
            
        Returns:
            Dictionary mapping quality flags to counts.
            
        Example:
            >>> metrics = QualityMetrics.count_by_flag(result_df)
            >>> print(f"GOOD: {metrics.get('GOOD', 0)}, WARNING: {metrics.get('WARNING', 0)}")
        """
        try:
            counts = (
                df_with_quality_flag.groupBy(quality_flag_col)
                .count()
                .collect()
            )
            return {row[quality_flag_col]: row["count"] for row in counts}
        except Exception as e:
            LOGGER.error("Failed to compute quality metrics: %s", e)
            return {}

    @staticmethod
    def log_quality_summary(
        counts: dict,
        logger: logging.Logger,
        table_name: str,
    ) -> None:
        """Log quality flag distribution summary.
        
        Args:
            counts: Dictionary from count_by_flag().
            logger: Logger instance to use.
            table_name: Table name for log message.
        """
        total = sum(counts.values())
        if total == 0:
            logger.warning("No records to analyze for quality summary")
            return
        
        good = counts.get("GOOD", 0)
        warning = counts.get("WARNING", 0)
        bad = counts.get("BAD", 0)
        
        good_pct = (good / total) * 100
        warning_pct = (warning / total) * 100
        bad_pct = (bad / total) * 100
        
        logger.info(
            "Quality summary for %s: GOOD=%d (%.1f%%), WARNING=%d (%.1f%%), BAD=%d (%.1f%%)",
            table_name,
            good,
            good_pct,
            warning,
            warning_pct,
            bad,
            bad_pct,
        )


__all__ = ["QualityFlagAssigner", "QualityMetrics"]
