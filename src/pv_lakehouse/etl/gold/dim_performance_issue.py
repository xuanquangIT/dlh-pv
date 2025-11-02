"""Gold loader for dim_performance_issue."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import dec


class GoldDimPerformanceIssueLoader(BaseGoldLoader):
    """Materialise performance issue dimension entries from static definitions."""

    source_tables: Dict[str, SourceTableConfig] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_performance_issue": GoldTableConfig(
            iceberg_table="lh.gold.dim_performance_issue",
            s3_base_path="s3a://lakehouse/gold/dim_performance_issue",
        )
    }

    _ROWS = [
        {
            "issue_category": "Weather",
            "issue_type": "High Cloud Cover",
            "root_cause": "Cloud-induced irradiance reduction",
            "severity_level": "High",
            "recommended_action": "Adjust forecasting baselines and monitor conditions.",
            "estimated_impact_pct": 25.0,
        },
        {
            "issue_category": "Weather",
            "issue_type": "Low Radiation",
            "root_cause": "Insufficient solar radiation",
            "severity_level": "Medium",
            "recommended_action": "Review irradiance assumptions and schedule cleaning.",
            "estimated_impact_pct": 15.0,
        },
        {
            "issue_category": "Soiling",
            "issue_type": "High Particulate Matter",
            "root_cause": "Elevated PM2.5 concentration",
            "severity_level": "High",
            "recommended_action": "Dispatch cleaning crew to restore efficiency.",
            "estimated_impact_pct": 30.0,
        },
        {
            "issue_category": "Grid",
            "issue_type": "Grid Constraint",
            "root_cause": "Grid export limitation",
            "severity_level": "Medium",
            "recommended_action": "Coordinate with grid operator for capacity updates.",
            "estimated_impact_pct": 10.0,
        },
        {
            "issue_category": "Equipment",
            "issue_type": "Low Availability",
            "root_cause": "Reduced system availability",
            "severity_level": "Medium",
            "recommended_action": "Inspect critical components and resolve outages.",
            "estimated_impact_pct": 20.0,
        },
    ]

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        schema = T.StructType(
            [
                T.StructField("issue_category", T.StringType(), False),
                T.StructField("issue_type", T.StringType(), False),
                T.StructField("root_cause", T.StringType(), False),
                T.StructField("severity_level", T.StringType(), False),
                T.StructField("recommended_action", T.StringType(), False),
                T.StructField("estimated_impact_pct", T.DoubleType(), False),
            ]
        )
        base = self.spark.createDataFrame(self._ROWS, schema=schema)
        window_key = Window.orderBy("issue_category", "issue_type")
        result = base.withColumn("performance_issue_key", F.row_number().over(window_key)).select(
            "performance_issue_key",
            "issue_category",
            "issue_type",
            "root_cause",
            "severity_level",
            "recommended_action",
            F.col("estimated_impact_pct").cast(dec(5, 2)).alias("estimated_impact_pct"),
        )

        return {"dim_performance_issue": result}
