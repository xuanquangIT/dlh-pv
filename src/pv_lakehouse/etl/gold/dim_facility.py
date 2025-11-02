"""Gold loader for dim_facility."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import dec, is_empty


class GoldDimFacilityLoader(BaseGoldLoader):
    """Materialise facility dimension records from the Silver master table."""

    source_tables: Dict[str, SourceTableConfig] = {
        "facility": SourceTableConfig(
            table_name="lh.silver.clean_facility_master",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "facility_name",
                "network_id",
                "network_region",
                "location_lat",
                "location_lng",
                "total_capacity_mw",
                "total_capacity_registered_mw",
                "total_capacity_maximum_mw",
                "unit_fueltech_summary",
                "unit_status_summary",
                "effective_from",
                "effective_to",
                "is_current",
            ],
        )
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_facility": GoldTableConfig(
            iceberg_table="lh.gold.dim_facility",
            s3_base_path="s3a://lakehouse/gold/dim_facility",
            partition_cols=("network_region",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        facilities = sources.get("facility")
        if is_empty(facilities):
            return None

        current = facilities.filter(F.col("is_current") | F.col("effective_to").isNull())
        if is_empty(current):
            current = facilities

        deduped = current.dropDuplicates(["facility_code"])
        if is_empty(deduped):
            return None

        window_key = Window.orderBy("facility_code")
        result = (
            deduped.withColumn("facility_key", F.row_number().over(window_key))
            .select(
                "facility_key",
                "facility_code",
                "facility_name",
                "network_id",
                "network_region",
                F.col("location_lat").cast(dec(10, 6)).alias("location_lat"),
                F.col("location_lng").cast(dec(10, 6)).alias("location_lng"),
                F.col("total_capacity_mw").cast(dec(10, 4)).alias("total_capacity_mw"),
                F.col("total_capacity_registered_mw").cast(dec(10, 4)).alias("total_capacity_registered_mw"),
                F.col("total_capacity_maximum_mw").cast(dec(10, 4)).alias("total_capacity_maximum_mw"),
                "unit_fueltech_summary",
                "unit_status_summary",
            )
        )

        return {"dim_facility": result}
