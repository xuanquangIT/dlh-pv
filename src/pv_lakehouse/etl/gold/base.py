"""Shared base classes for Gold-zone loaders."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from ..utils.spark_utils import cleanup_spark_staging, create_spark_session, write_iceberg_table


@dataclass
class SourceTableConfig:
    """Configuration describing a source table and optional filters."""

    table_name: str
    timestamp_column: Optional[str] = None
    required_columns: Optional[Iterable[str]] = None


@dataclass
class GoldTableConfig:
    """Configuration for Gold table outputs."""

    iceberg_table: str
    s3_base_path: str
    partition_cols: Iterable[str] = field(default_factory=tuple)


@dataclass
class GoldLoadOptions:
    """Parameters controlling a gold load execution."""

    mode: str = "incremental"  # "full" or "incremental"
    start: Optional[dt.datetime] = None
    end: Optional[dt.datetime] = None
    load_strategy: str = "merge"  # "overwrite" or "merge"
    app_name: str = "gold-loader"
    target_file_size_mb: int = 128
    max_records_per_file: int = 250_000


class BaseGoldLoader:
    """Base helper offering shared orchestration behaviour for Gold layer."""

    source_tables: Dict[str, SourceTableConfig]
    gold_tables: Dict[str, GoldTableConfig]

    def __init__(self, options: Optional[GoldLoadOptions] = None) -> None:
        self.options = options or GoldLoadOptions()
        self._spark: Optional[SparkSession] = None
        self._validate_options()

    # ------------------------------------------------------------------
    # Orchestration helpers
    # ------------------------------------------------------------------
    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = create_spark_session(self.options.app_name)
            # Optimize for Gold layer: smaller datasets, many small dimension joins
            # Reduce shuffle partitions from default 200 to 8 for better performance
            self._maybe_set_conf("spark.sql.shuffle.partitions", "8")
            # Enable broadcast join auto-detection for tables < 10MB
            self._maybe_set_conf("spark.sql.autoBroadcastJoinThreshold", "10485760")
        return self._spark

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            cleanup_spark_staging()

    def run(self) -> int:
        try:
            source_frames = self._read_sources()
            if not source_frames and getattr(self, "source_tables", None):
                return 0

            outputs = self.transform(source_frames)
            if not outputs:
                return 0

            total_rows = 0
            materialised: Dict[str, DataFrame] = {}
            for name, dataframe in outputs.items():
                if dataframe is None:
                    continue
                row_count = dataframe.count()
                if row_count == 0:
                    continue
                materialised[name] = dataframe
                total_rows += row_count

            if not materialised:
                return 0

            self._write_outputs(materialised)
            return total_rows
        finally:
            self.close()

    # ------------------------------------------------------------------
    # Abstract hooks
    # ------------------------------------------------------------------
    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:  # pragma: no cover - abstract
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Shared utilities
    # ------------------------------------------------------------------
    def _read_sources(self) -> Dict[str, DataFrame]:
        frames: Dict[str, DataFrame] = {}
        for name, config in self.source_tables.items():
            dataframe = self._read_table(config)
            if dataframe is None:
                continue
            frames[name] = dataframe
        return frames

    def _read_table(self, config: SourceTableConfig) -> Optional[DataFrame]:
        try:
            dataframe = self.spark.table(config.table_name)
        except AnalysisException:
            return None

        timestamp_column = config.timestamp_column
        if timestamp_column:
            if timestamp_column not in dataframe.columns:
                raise ValueError(
                    f"Timestamp column '{timestamp_column}' missing from table {config.table_name}"
                )
            start_literal = self._normalise_datetime(self.options.start)
            end_literal = self._normalise_datetime(self.options.end)
            timestamp_col = F.col(timestamp_column)
            if start_literal:
                dataframe = dataframe.filter(timestamp_col >= F.to_timestamp(F.lit(start_literal)))
            if end_literal:
                dataframe = dataframe.filter(timestamp_col <= F.to_timestamp(F.lit(end_literal)))

        required_columns = set(config.required_columns or [])
        missing = required_columns - set(dataframe.columns)
        if missing:
            raise ValueError(
                f"Missing expected columns {sorted(missing)} in table {config.table_name}"
            )
        return dataframe

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

    def _write_outputs(self, dataframes: Dict[str, DataFrame]) -> None:
        if not hasattr(self, "gold_tables") or not self.gold_tables:
            raise ValueError("gold_tables configuration is required for Gold loaders")

        for name, dataframe in dataframes.items():
            if name not in self.gold_tables:
                raise KeyError(f"Missing GoldTableConfig for output '{name}'")

        for name, dataframe in dataframes.items():
            config = self.gold_tables[name]
            self._write_single_output(dataframe, config)

    def _write_single_output(self, dataframe: DataFrame, config: GoldTableConfig) -> None:
        load_strategy = self.options.load_strategy
        if load_strategy not in {"overwrite", "merge"}:
            raise ValueError("load_strategy must be 'overwrite' or 'merge'")

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
        s3_target = f"{config.s3_base_path}/load_date={load_date}"

        (
            dataframe.write.mode(s3_mode)
            .format("parquet")
            .option("compression", "snappy")
            .save(s3_target)
        )

        write_iceberg_table(
            dataframe,
            config.iceberg_table,
            mode=iceberg_mode,
            partition_cols=config.partition_cols,
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

    def _maybe_set_conf(self, key: str, value: str) -> None:
        current = self.spark.conf.get(key, None)
        if current is None:
            self.spark.conf.set(key, value)

    def _validate_options(self) -> None:
        if self.options.mode not in {"full", "incremental"}:
            raise ValueError("GoldLoadOptions.mode must be 'full' or 'incremental'")
        if self.options.load_strategy not in {"overwrite", "merge"}:
            raise ValueError("GoldLoadOptions.load_strategy must be 'overwrite' or 'merge'")
        start = self.options.start
        end = self.options.end
        start_iso = self._normalise_datetime(start) if start else None
        end_iso = self._normalise_datetime(end) if end else None
        if start_iso and end_iso and end_iso < start_iso:
            raise ValueError("GoldLoadOptions.end must not be before GoldLoadOptions.start")


__all__ = [
    "GoldLoadOptions",
    "GoldTableConfig",
    "SourceTableConfig",
    "BaseGoldLoader",
]
