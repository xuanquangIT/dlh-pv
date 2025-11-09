"""Silver loader producing clean_facility_master."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseSilverLoader


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
        if bronze_df is None or not bronze_df.columns:
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
            return None

        bronze_clean = (
            bronze_filtered.withColumn("source", F.lit("bronze"))
            .withColumn(
                "total_capacity_mw",
                F.col("total_capacity_mw").cast(T.DecimalType(10, 4)),
            )
            .withColumn(
                "total_capacity_registered_mw",
                F.col("total_capacity_registered_mw").cast(T.DecimalType(10, 4)),
            )
            .withColumn(
                "total_capacity_maximum_mw",
                F.col("total_capacity_maximum_mw").cast(T.DecimalType(10, 4)),
            )
            .withColumn("location_lat", F.col("location_lat").cast(T.DecimalType(10, 6)))
            .withColumn("location_lng", F.col("location_lng").cast(T.DecimalType(10, 6)))
        )

        existing = self._safe_read_silver()
        baseline_df: Optional[DataFrame] = None
        if existing is not None:
            baseline_df = (
                existing.filter(F.col("is_current"))
                .select(
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
                )
                .withColumn("ingest_timestamp", F.col("effective_from"))
                .withColumn("source", F.lit("baseline"))
            )

        if baseline_df is not None:
            combined = bronze_clean.unionByName(baseline_df, allowMissingColumns=True)
        else:
            combined = bronze_clean

        relevant_cols = [
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
            "ingest_timestamp",
            "source",
        ]
        combined = combined.select(*relevant_cols)

        comparison_cols = [
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
        ]

        hash_inputs = [F.coalesce(F.col(col).cast("string"), F.lit("")) for col in comparison_cols]
        combined = combined.withColumn("record_hash", F.sha2(F.concat_ws("||", *hash_inputs), 256))

        window = Window.partitionBy("facility_code").orderBy("effective_from", "source")
        combined = combined.withColumn("prev_hash", F.lag("record_hash").over(window))
        combined = combined.withColumn("next_effective_from", F.lead("effective_from").over(window))
        combined = combined.withColumn(
            "is_changed",
            F.when(F.col("source") == F.lit("baseline"), F.lit(False)).otherwise(
                (F.col("prev_hash").isNull()) | (F.col("record_hash") != F.col("prev_hash"))
            ),
        )

        result = (
            combined.filter(F.col("is_changed"))
            .drop("prev_hash", "record_hash", "is_changed")
            .withColumn("effective_to", F.col("next_effective_from"))
            .withColumn("is_current", F.col("next_effective_from").isNull())
            .drop("next_effective_from")
        )

        result = result.withColumn("is_valid", F.col("total_capacity_mw") > F.lit(0))
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("CAPACITY_INVALID")),
        )

        current_ts = F.current_timestamp()
        result = result.withColumn("created_at", current_ts).withColumn("updated_at", current_ts)

        return result.select(*self._output_columns)


__all__ = ["SilverFacilityMasterLoader"]
