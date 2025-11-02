"""Gold loader for dim_equipment_status."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig


class GoldDimEquipmentStatusLoader(BaseGoldLoader):
    """Materialise equipment status dimension from static definitions."""

    source_tables: Dict[str, SourceTableConfig] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_equipment_status": GoldTableConfig(
            iceberg_table="lh.gold.dim_equipment_status",
            s3_base_path="s3a://lakehouse/gold/dim_equipment_status",
        )
    }

    _ROWS = [
        {
            "status_name": "Available",
            "availability_status": "Available",
            "maintenance_required": False,
            "fault_indicator": False,
            "status_description": "All systems operational.",
        },
        {
            "status_name": "Degraded",
            "availability_status": "Degraded",
            "maintenance_required": True,
            "fault_indicator": False,
            "status_description": "Performance degraded; investigate causes.",
        },
        {
            "status_name": "Unavailable",
            "availability_status": "Unavailable",
            "maintenance_required": True,
            "fault_indicator": True,
            "status_description": "Facility offline awaiting maintenance.",
        },
    ]

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        schema = T.StructType(
            [
                T.StructField("status_name", T.StringType(), False),
                T.StructField("availability_status", T.StringType(), False),
                T.StructField("maintenance_required", T.BooleanType(), False),
                T.StructField("fault_indicator", T.BooleanType(), False),
                T.StructField("status_description", T.StringType(), False),
            ]
        )
        base = self.spark.createDataFrame(self._ROWS, schema=schema)
        window_key = Window.orderBy("status_name")
        result = base.withColumn("equipment_status_key", F.row_number().over(window_key)).select(
            "equipment_status_key",
            "status_name",
            "availability_status",
            "maintenance_required",
            "fault_indicator",
            "status_description",
        )

        return {"dim_equipment_status": result}
