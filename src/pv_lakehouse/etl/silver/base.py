"""Shared base classes and options for Silver-zone loaders."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from ..utils.spark_utils import cleanup_spark_staging, create_spark_session, write_iceberg_table


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
    timestamp_column: str  # Bronze timestamp column for filtering
    partition_cols: Sequence[str] = ()
    silver_timestamp_column: Optional[str] = None  # Silver timestamp column (defaults to partition_cols[0])
    
    # Default Iceberg file size settings
    DEFAULT_TARGET_FILE_SIZE_MB = 8
    DEFAULT_MAX_RECORDS_PER_FILE = 10_000

    def __init__(self, options: Optional[LoadOptions] = None) -> None:
        self.options = options or LoadOptions()
        self._spark: Optional[SparkSession] = None
        # Default silver_timestamp_column to first partition column if not set
        if self.silver_timestamp_column is None and self.partition_cols:
            self.silver_timestamp_column = self.partition_cols[0]
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
            cleanup_spark_staging()

    def _process_in_chunks(self, bronze_df: DataFrame, chunk_days: int) -> int:
        """
        Process bronze data in time-based chunks to limit concurrent partition writers.
        
        Args:
            bronze_df: Source bronze DataFrame
            chunk_days: Chunk size in days (e.g., 3 for energy, 7 for weather/air_quality)
            
        Returns:
            Total number of rows written across all chunks
        """
        timestamp_col = self.timestamp_column
        if timestamp_col not in bronze_df.columns:
            raise ValueError(
                f"Timestamp column '{timestamp_col}' missing from bronze table {self.bronze_table}"
            )

        # Get data time range
        min_ts = bronze_df.select(F.min(F.col(timestamp_col))).collect()[0][0]
        max_ts = bronze_df.select(F.max(F.col(timestamp_col))).collect()[0][0]
        if min_ts is None or max_ts is None:
            return 0

        # Apply user-specified time range filters
        if isinstance(self.options.start, dt.datetime):
            min_ts = max(min_ts, self.options.start)
        elif isinstance(self.options.start, dt.date):
            min_ts = max(min_ts, dt.datetime.combine(self.options.start, dt.time.min))

        if isinstance(self.options.end, dt.datetime):
            max_ts = min(max_ts, self.options.end)
        elif isinstance(self.options.end, dt.date):
            max_ts = min(max_ts, dt.datetime.combine(self.options.end, dt.time.max))

        if min_ts > max_ts:
            return 0

        # Process in chunks
        chunk_start = min_ts.replace(hour=0, minute=0, second=0, microsecond=0)
        total_rows = 0

        # Backup original options to restore after chunking
        original_start = self.options.start
        original_end = self.options.end

        try:
            while chunk_start <= max_ts:
                chunk_end = min(max_ts, chunk_start + dt.timedelta(days=chunk_days) - dt.timedelta(microseconds=1))

                print(
                    f"Processing {chunk_days}-day chunk ({chunk_start.strftime('%Y-%m-%d')}): "
                    f"{chunk_start.isoformat()} -> {chunk_end.isoformat()}"
                )

                # Filter to chunk time range
                chunk_df = bronze_df.filter(
                    (F.col(timestamp_col) >= F.lit(chunk_start)) & (F.col(timestamp_col) <= F.lit(chunk_end))
                )
                
                try:
                    transformed_df = self.transform(chunk_df)
                    if transformed_df is None:
                        chunk_start = chunk_end + dt.timedelta(microseconds=1)
                        continue

                    # Materialize count before write to ensure transform is complete
                    row_count = transformed_df.count()
                    if row_count == 0:
                        chunk_start = chunk_end + dt.timedelta(microseconds=1)
                        continue

                    # Update options for partition-aware write
                    self.options.start = chunk_start
                    self.options.end = chunk_end
                    self._write_outputs(transformed_df)
                    total_rows += row_count

                    print(f"Finished {chunk_days}-day chunk: wrote {row_count} rows to {self.silver_table}")
                finally:
                    pass

                chunk_start = chunk_end + dt.timedelta(microseconds=1)
        finally:
            # Restore original options
            self.options.start = original_start
            self.options.end = original_end

        return total_rows

    def run(self) -> int:
        """
        Default run method: read bronze, transform, and write to silver.
        Subclasses can override this method to implement chunking (e.g., hourly loaders).
        """
        try:
            bronze_df = self._read_bronze()
            if bronze_df is None or not bronze_df.columns:
                return 0

            transformed_df = self.transform(bronze_df)
            if transformed_df is None:
                return 0

            # Materialize count before write
            row_count = transformed_df.count()
            if row_count == 0:
                return 0

            self._write_outputs(transformed_df)
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

        # Auto-detection: Query last loaded timestamp from Silver if incremental mode without start date
        if self.options.mode == "incremental" and self.options.start is None and self.silver_timestamp_column:
            try:
                max_ts_row = self.spark.sql(f"""
                    SELECT MAX({self.silver_timestamp_column}) as max_ts
                    FROM {self.silver_table}
                """).collect()
                
                if max_ts_row and max_ts_row[0]["max_ts"] is not None:
                    max_ts = max_ts_row[0]["max_ts"]
                    # Get current time for comparison
                    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
                    
                    if isinstance(max_ts, dt.datetime):
                        last_hour = max_ts.replace(minute=0, second=0, microsecond=0)
                        current_hour = now.replace(minute=0, second=0, microsecond=0)
                        
                        # If last loaded is current hour or future, reload from current hour
                        # Otherwise start from next hour after last loaded
                        if last_hour >= current_hour:
                            self.options.start = current_hour
                            print(f"[SILVER INCREMENTAL] Reloading current hour {self.options.start} (last loaded: {max_ts})", flush=True)
                        else:
                            self.options.start = last_hour + dt.timedelta(hours=1)
                            print(f"[SILVER INCREMENTAL] Loading from {self.options.start} (last loaded: {max_ts})", flush=True)
                    else:
                        # If timestamp is date type, start from next day
                        today = now.date()
                        last_date = max_ts if isinstance(max_ts, dt.date) else max_ts.date()
                        
                        if last_date >= today:
                            self.options.start = today
                            print(f"[SILVER INCREMENTAL] Reloading today {self.options.start} (last loaded: {last_date})", flush=True)
                        else:
                            self.options.start = last_date + dt.timedelta(days=1)
                            print(f"[SILVER INCREMENTAL] Loading from {self.options.start} (last loaded: {last_date})", flush=True)
                else:
                    print("[SILVER INCREMENTAL] No existing Silver data found, will process all Bronze data", flush=True)
            except Exception as e:
                # Silver table doesn't exist yet or query failed - process all Bronze data
                print(f"[SILVER INCREMENTAL] Could not query Silver table (likely first run): {e}", flush=True)
                print("[SILVER INCREMENTAL] Will process all Bronze data", flush=True)

        # Mode validation already done in _validate_options()
        start_literal = self._normalise_datetime(self.options.start)
        end_literal = self._normalise_datetime(self.options.end)
        timestamp_col = F.col(self.timestamp_column)

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

    def _write_outputs(self, dataframe: DataFrame) -> None:
        # Strategy validation already done in _validate_options()
        # Both 'merge' and 'overwrite' use overwritePartitions mode
        load_strategy = self.options.load_strategy
        iceberg_mode = "overwrite" if load_strategy in {"overwrite", "merge"} else "append"

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

        # Write only to Iceberg table (no S3/MinIO write like bronze layer)
        write_iceberg_table(
            dataframe,
            self.silver_table,
            mode=iceberg_mode,
            partition_cols=self.partition_cols,
        )

    def _safe_read_silver(self) -> Optional[DataFrame]:
        try:
            return self.spark.table(self.silver_table)
        except AnalysisException:
            return None

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
