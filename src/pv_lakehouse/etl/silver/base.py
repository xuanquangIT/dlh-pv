"""Shared base classes and options for Silver-zone loaders."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from ..utils.spark_utils import create_spark_session, write_iceberg_table


@dataclass
class LoadOptions:
    """Parameters controlling a silver load execution."""

    mode: str = "incremental"  # "full" or "incremental"
    start: Optional[dt.datetime] = None
    end: Optional[dt.datetime] = None
    load_strategy: str = "merge"  # "overwrite" or "merge"
    app_name: str = "silver-loader"
    target_file_size_mb: int = 128
    max_records_per_file: int = 250_000


class BaseSilverLoader:
    """Base helper offering shared orchestration behaviour."""

    bronze_table: str
    silver_table: str
    s3_base_path: str
    timestamp_column: str
    partition_cols: Sequence[str] = ()

    def __init__(self, options: Optional[LoadOptions] = None) -> None:
        self.options = options or LoadOptions()
        self._spark: Optional[SparkSession] = None
        self._validate_options()

    # ------------------------------------------------------------------
    # Orchestration helpers
    # ------------------------------------------------------------------
    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = create_spark_session(self.options.app_name)
        return self._spark

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def run(self) -> int:
        try:
            bronze_df = self._read_bronze()
            if bronze_df is None:
                return 0

            transformed_df = self.transform(bronze_df)
            if transformed_df is None:
                return 0

            row_count = transformed_df.count()
            if row_count == 0:
                return 0

            self._write_outputs(transformed_df, row_count)
            return row_count
        finally:
            self.close()

    # ------------------------------------------------------------------
    # Abstract hooks
    # ------------------------------------------------------------------
    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:  # pragma: no cover - abstract
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Shared utilities
    # ------------------------------------------------------------------
    def _read_bronze(self) -> Optional[DataFrame]:
        try:
            df = self.spark.table(self.bronze_table)
        except AnalysisException:
            return None

        if self.timestamp_column not in df.columns:
            raise ValueError(
                f"Timestamp column '{self.timestamp_column}' missing from bronze table {self.bronze_table}"
            )

        start_literal = self._normalise_datetime(self.options.start)
        end_literal = self._normalise_datetime(self.options.end)
        timestamp_col = F.col(self.timestamp_column)

        if self.options.mode not in {"full", "incremental"}:
            raise ValueError("mode must be either 'full' or 'incremental'")

        if start_literal:
            df = df.filter(timestamp_col >= F.to_timestamp(F.lit(start_literal)))
        if end_literal:
            df = df.filter(timestamp_col <= F.to_timestamp(F.lit(end_literal)))

        return df

    def _normalise_datetime(self, value: Optional[dt.datetime | dt.date | str]) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, dt.datetime):
            return value.replace(microsecond=0).isoformat()
        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time.min).isoformat()
        if isinstance(value, str):
            return value
        raise TypeError(f"Unsupported datetime value: {value!r}")

    def _write_outputs(self, dataframe: DataFrame, row_count: int) -> None:
        load_strategy = self.options.load_strategy
        if load_strategy not in {"overwrite", "merge"}:
            raise ValueError("load_strategy must be 'overwrite' or 'merge'")

        # 'merge' uses overwritePartitions to replace impacted slices
        iceberg_mode = "overwrite" if load_strategy in {"overwrite", "merge"} else "append"
        s3_mode = "overwrite" if load_strategy in {"overwrite", "merge"} else "append"

        self._maybe_set_conf(
            "spark.sql.iceberg.target-file-size-bytes",
            str(self.options.target_file_size_mb * 1024 * 1024),
        )
        self._maybe_set_conf(
            "spark.sql.iceberg.max-records-per-file",
            str(self.options.max_records_per_file),
        )
        self._maybe_set_conf(
            "spark.sql.files.maxRecordsPerFile",
            str(self.options.max_records_per_file),
        )

        load_date = self._resolve_load_date()
        s3_target = f"{self.s3_base_path}/load_date={load_date}"

        (
            dataframe.write.mode(s3_mode)
            .format("parquet")
            .option("compression", "snappy")
            .save(s3_target)
        )

        write_iceberg_table(
            dataframe,
            self.silver_table,
            mode=iceberg_mode,
            partition_cols=self.partition_cols,
        )

    def _resolve_load_date(self) -> str:
        if isinstance(self.options.end, dt.datetime):
            return self.options.end.date().isoformat()
        if isinstance(self.options.start, dt.datetime):
            return self.options.start.date().isoformat()
        if isinstance(self.options.start, dt.date):
            return self.options.start.isoformat()
        if isinstance(self.options.end, dt.date):
            return self.options.end.isoformat()
        return dt.date.today().isoformat()

    def _safe_read_silver(self) -> Optional[DataFrame]:
        try:
            return self.spark.table(self.silver_table)
        except AnalysisException:
            return None

    def _maybe_add_columns(self, dataframe: DataFrame, columns: Iterable[str]) -> DataFrame:
        result = dataframe
        for column in columns:
            if column not in result.columns:
                result = result.withColumn(column, F.lit(None))
        return result

    def _validate_options(self) -> None:
        if self.options.mode not in {"full", "incremental"}:
            raise ValueError("LoadOptions.mode must be 'full' or 'incremental'")
        if self.options.load_strategy not in {"overwrite", "merge"}:
            raise ValueError("LoadOptions.load_strategy must be 'overwrite' or 'merge'")
        start = self.options.start
        end = self.options.end
        if start and end and self._normalise_datetime(start) and self._normalise_datetime(end):
            start_iso = self._normalise_datetime(start)
            end_iso = self._normalise_datetime(end)
            if start_iso and end_iso and end_iso < start_iso:
                raise ValueError("LoadOptions.end must not be before LoadOptions.start")

    def _maybe_set_conf(self, key: str, value: str) -> None:
        current = self.spark.conf.get(key, None)
        if current is None:
            self.spark.conf.set(key, value)


__all__ = ["LoadOptions", "BaseSilverLoader"]
