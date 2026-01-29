"""Silver loader producing clean_facility_master."""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseSilverLoader

LOGGER = logging.getLogger(__name__)


class SilverFacilityMasterLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facilities"
    silver_table = "lh.silver.clean_facility_master"
    timestamp_column = "ingest_timestamp"
    partition_cols = ("facility_code",)

    _output_columns = [
        "facility_code",
        "facility_name",
        "network_id",
        "network_region",
        "location_lat",
        "location_lng",
        "unit_count",
        "total_capacity_mw",
        "total_capacity_registered_mw",
        "total_capacity_maximum_mw",
        "unit_fueltech_summary",
        "unit_status_summary",
        "facility_description",
        "effective_from",
        "effective_to",
        "is_current",
        "is_valid",
        "quality_flag",
        "created_at",
        "updated_at",
    ]

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        """Transform bronze facility master data to Silver layer with SCD Type 2.
        
        Args:
            bronze_df: Bronze layer facilities DataFrame.
            
        Returns:
            Transformed Silver DataFrame with SCD Type 2 tracking, or None if input is invalid.
        """
        # Validation: Check input DataFrame
        if bronze_df is None or not bronze_df.columns:
            LOGGER.warning("Empty bronze DataFrame provided to facility_master transform()")
            return None

        required_columns = {
            "facility_code",
            "ingest_timestamp",
            "facility_name",
            "network_id",
            "network_region",
            "location_lat",
            "location_lng",
            "unit_count",
            "total_capacity_mw",
            "total_capacity_registered_mw",
            "total_capacity_maximum_mw",
            "unit_fueltech_summary",
            "unit_status_summary",
            "facility_description",
        }
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze facilities source: {sorted(missing)}")
        
        quality_assigner = self._get_quality_assigner()
        LOGGER.debug("Starting facility master transform with SCD Type 2 logic")

        # Step 1: Filter and prepare bronze data with type casting in single select()
        bronze_filtered = bronze_df.select(
            "facility_code",
            "facility_name",
            "network_id",
            "network_region",
            "location_lat",
            "location_lng",
            "unit_count",
            "total_capacity_mw",
            "total_capacity_registered_mw",
            "total_capacity_maximum_mw",
            "unit_fueltech_summary",
            "unit_status_summary",
            "facility_description",
            F.col("ingest_timestamp").alias("effective_from"),
            "ingest_timestamp",
        ).where(F.col("facility_code").isNotNull())

        if bronze_filtered.rdd.isEmpty():
            LOGGER.warning("No facilities found in bronze data after filtering")
            return None

        # Step 2: Cast numeric columns in single select() operation
        bronze_clean = bronze_filtered.select(
            "facility_code",
            "facility_name",
            "network_id",
            "network_region",
            F.col("location_lat").cast(T.DecimalType(10, 6)).alias("location_lat"),
            F.col("location_lng").cast(T.DecimalType(10, 6)).alias("location_lng"),
            "unit_count",
            F.col("total_capacity_mw").cast(T.DecimalType(10, 4)).alias("total_capacity_mw"),
            F.col("total_capacity_registered_mw").cast(T.DecimalType(10, 4)).alias("total_capacity_registered_mw"),
            F.col("total_capacity_maximum_mw").cast(T.DecimalType(10, 4)).alias("total_capacity_maximum_mw"),
            "unit_fueltech_summary",
            "unit_status_summary",
            "facility_description",
            "effective_from",
            "ingest_timestamp",
            F.lit("bronze").alias("source"),
        )

        # Step 3: Load existing Silver data as baseline for SCD Type 2
        existing = self._safe_read_silver()
        baseline_df: Optional[DataFrame] = None
        if existing is not None:
            baseline_df = existing.filter(F.col("is_current")).select(
                "facility_code",
                "facility_name",
                "network_id",
                "network_region",
                "location_lat",
                "location_lng",
                "unit_count",
                "total_capacity_mw",
                "total_capacity_registered_mw",
                "total_capacity_maximum_mw",
                "unit_fueltech_summary",
                "unit_status_summary",
                "facility_description",
                F.col("effective_from"),
                F.col("effective_from").alias("ingest_timestamp"),
                F.lit("baseline").alias("source"),
            )
            LOGGER.debug("Loaded %d baseline facilities from existing Silver", baseline_df.count())

        # Step 4: Combine bronze and baseline data
        combined = bronze_clean.unionByName(baseline_df, allowMissingColumns=True) if baseline_df is not None else bronze_clean

        # Step 5: Detect changes using hash-based comparison (SCD Type 2)
        comparison_cols = [
            "facility_name", "network_id", "network_region", "location_lat", "location_lng",
            "unit_count", "total_capacity_mw", "total_capacity_registered_mw",
            "total_capacity_maximum_mw", "unit_fueltech_summary", "unit_status_summary",
            "facility_description",
        ]
        
        hash_inputs = [F.coalesce(F.col(col).cast("string"), F.lit("")) for col in comparison_cols]
        record_hash = F.sha2(F.concat_ws("||", *hash_inputs), 256)
        
        window = Window.partitionBy("facility_code").orderBy("effective_from", "source")
        prev_hash = F.lag(record_hash).over(window)
        next_effective_from = F.lead(F.col("effective_from")).over(window)
        
        is_changed = F.when(
            F.col("source") == F.lit("baseline"),
            F.lit(False)
        ).otherwise(
            prev_hash.isNull() | (record_hash != prev_hash)
        )
        
        combined = combined.select(
            "*",
            record_hash.alias("record_hash"),
            prev_hash.alias("prev_hash"),
            next_effective_from.alias("next_effective_from"),
            is_changed.alias("is_changed"),
        )

        # Step 6: Filter changed records and compute SCD Type 2 attributes
        changed_records = combined.filter(F.col("is_changed")).drop(
            "prev_hash", "record_hash", "is_changed", "source", "ingest_timestamp"
        )
        
        # Step 7: Build quality columns and final result in single select()
        is_valid = F.col("total_capacity_mw") > F.lit(0)
        quality_flag = quality_assigner.assign_binary(is_valid, "GOOD", "CAPACITY_INVALID")
        
        result = changed_records.select(
            "facility_code",
            "facility_name",
            "network_id",
            "network_region",
            F.col("location_lat"),
            F.col("location_lng"),
            "unit_count",
            "total_capacity_mw",
            "total_capacity_registered_mw",
            "total_capacity_maximum_mw",
            "unit_fueltech_summary",
            "unit_status_summary",
            "facility_description",
            F.col("effective_from"),
            F.col("next_effective_from").alias("effective_to"),
            F.col("next_effective_from").isNull().alias("is_current"),
            is_valid.alias("is_valid"),
            quality_flag.alias("quality_flag"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )

        LOGGER.info("Facility master transform completed successfully")
        return result


__all__ = ["SilverFacilityMasterLoader"]
