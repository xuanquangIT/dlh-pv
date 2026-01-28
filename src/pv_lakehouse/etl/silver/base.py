"""Shared base classes and options for Silver-zone loaders."""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Sequence

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pv_lakehouse.etl.bronze.facility_timezones import FACILITY_TIMEZONES, DEFAULT_TIMEZONE
from pv_lakehouse.etl.utils.etl_metrics import ETLTimer, log_etl_start, log_etl_summary

from .quality_checker import QualityFlagAssigner
from .validators import LogicValidator, NumericBoundsValidator

# Maximum timezone offset in hours from UTC (Australia is UTC+10/11)
MAX_TIMEZONE_OFFSET_HOURS = 12

LOGGER = logging.getLogger(__name__)

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
        self._validation_config: Optional[Dict] = None
        # Default silver_timestamp_column to first partition column if not set
        if self.silver_timestamp_column is None and self.partition_cols:
            self.silver_timestamp_column = self.partition_cols[0]
        self._validate_options()

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def _load_validation_config(self) -> Dict:
        """Load validation rules from config/silver_validation_rules.yaml.
        
        Returns:
            Dictionary containing validation rules for all domains.
        """
        if self._validation_config is not None:
            return self._validation_config
        
        # Path: base.py -> silver -> etl -> pv_lakehouse -> src -> workdir (5 parents)
        config_path = Path(__file__).parent.parent.parent.parent.parent / "config" / "silver_validation_rules.yaml"
        
        if not config_path.exists():
            LOGGER.warning("Validation config not found at %s, using empty config", config_path)
            self._validation_config = {}
            return self._validation_config
        
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self._validation_config = yaml.safe_load(f) or {}
            LOGGER.debug("Loaded validation config from %s", config_path)
        except Exception as e:
            LOGGER.error("Failed to load validation config from %s: %s", config_path, e)
            self._validation_config = {}
        
        return self._validation_config

    def _get_bounds_config(self, domain: str) -> Dict[str, Dict[str, float]]:
        """Get numeric bounds configuration for a specific domain.
        
        Args:
            domain: Domain name ("weather", "air_quality", "energy").
            
        Returns:
            Dictionary mapping column names to {"min": float, "max": float, ...}.
        """
        config = self._load_validation_config()
        return config.get(domain, {})

    def _get_quality_thresholds(self, domain: str) -> Dict:
        """Get quality check thresholds for a specific domain.
        
        Args:
            domain: Domain name ("weather", "air_quality", "energy").
            
        Returns:
            Dictionary containing threshold values for quality checks.
        """
        config = self._load_validation_config()
        thresholds = config.get("quality_thresholds", {})
        return thresholds.get(domain, {})

    def _get_numeric_validator(self, domain: str) -> NumericBoundsValidator:
        """Create NumericBoundsValidator for a specific domain.
        
        Args:
            domain: Domain name ("weather", "air_quality", "energy").
            
        Returns:
            Configured NumericBoundsValidator instance.
        """
        bounds_config = self._get_bounds_config(domain)
        return NumericBoundsValidator(bounds_config)

    @staticmethod
    def _get_logic_validator() -> LogicValidator:
        """Get LogicValidator instance (static methods, no state).
        
        Returns:
            LogicValidator instance.
        """
        return LogicValidator()

    @staticmethod
    def _get_quality_assigner() -> QualityFlagAssigner:
        """Get QualityFlagAssigner instance (static methods, no state).
        
        Returns:
            QualityFlagAssigner instance.
        """
        return QualityFlagAssigner()

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

    def _get_hour_offset(self) -> int:
        """Return hour offset to apply to incremental start for loaders that shift timestamps.
        
        Override in subclasses that shift timestamps (e.g., energy loader shifts by +1 hour).
        Returns:
            Hour offset (0 = no shift, 1 = shift by 1 hour)
        """
        return 0

    def _get_timezone_lookback_hours(self) -> int:
        # Max UTC offset for Australia timezones (AEST=UTC+10, AEDT=UTC+11)
        # Use 12h as safe buffer to handle DST transitions and edge cases
        return MAX_TIMEZONE_OFFSET_HOURS

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

                # Persist to avoid recomputation
                chunk_df.persist()
                
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
                    # Release memory immediately after processing each chunk
                    chunk_df.unpersist()

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
        timer = ETLTimer()
        log_etl_start(LOGGER, self.silver_table, self.options.mode)
        
        try:
            bronze_df = self._read_bronze()
            if bronze_df is None or not bronze_df.columns:
                LOGGER.warning("No Bronze data found for %s", self.bronze_table)
                return 0

            transformed_df = self.transform(bronze_df)
            if transformed_df is None:
                LOGGER.warning("Transform returned None for %s", self.bronze_table)
                return 0

            # Materialize count before write
            row_count = transformed_df.count()
            if row_count == 0:
                LOGGER.warning("Transform produced 0 rows for %s", self.bronze_table)
                return 0

            self._write_outputs(transformed_df)
            log_etl_summary(LOGGER, self.silver_table, row_count, timer.elapsed())
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

        # Helper functions to query timestamps (consolidated to avoid duplication)
        def _get_silver_max_ts():
            if not self.silver_timestamp_column:
                return None
            try:
                max_ts_row = self.spark.sql(f"""
                    SELECT MAX({self.silver_timestamp_column}) as max_ts
                    FROM {self.silver_table}
                """).collect()
                if max_ts_row and max_ts_row[0]["max_ts"] is not None:
                    return max_ts_row[0]["max_ts"]
            except Exception:
                pass
            return None

        def _get_bronze_min_ts():
            try:
                min_bronze_ts_row = self.spark.sql(f"""
                    SELECT MIN(CAST({self.timestamp_column} AS TIMESTAMP)) as min_ts
                    FROM {self.bronze_table}
                """).collect()
                if min_bronze_ts_row and min_bronze_ts_row[0]["min_ts"] is not None:
                    return min_bronze_ts_row[0]["min_ts"]
            except Exception:
                pass
            return None

        # If Silver is empty and user provided explicit start, check if Bronze has earlier data
        silver_max_ts = _get_silver_max_ts()
        if silver_max_ts is None and self.options.start is not None:
            bronze_min_ts = _get_bronze_min_ts()
            if bronze_min_ts:
                user_start = self._normalise_datetime(self.options.start)
                if user_start:
                    user_start_ts = dt.datetime.fromisoformat(user_start)
                    if bronze_min_ts < user_start_ts:
                        self.options.start = bronze_min_ts
                        print(f"[SILVER FIRST-RUN] Detected earlier Bronze data at {bronze_min_ts}, overriding user start {user_start_ts}", flush=True)

        # Auto-detection: Query last loaded timestamp from Silver if incremental mode without start date
        if self.options.mode == "incremental" and self.options.start is None and self.silver_timestamp_column:
            silver_max_ts = _get_silver_max_ts()
            
            if silver_max_ts is not None:
                # Get current time for comparison
                now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
                
                if isinstance(silver_max_ts, dt.datetime):
                    last_hour = silver_max_ts.replace(minute=0, second=0, microsecond=0)
                    current_hour = now.replace(minute=0, second=0, microsecond=0)
                    
                    # Calculate total lookback: hour_offset + timezone_lookback
                    # This ensures we capture all Bronze (UTC) data that maps to Silver (local time)
                    hour_offset = self._get_hour_offset()
                    tz_lookback = self._get_timezone_lookback_hours()
                    total_lookback = hour_offset + tz_lookback
                    
                    # If last loaded is current hour or future, reload from current hour minus lookback
                    # Otherwise start from last_hour minus total_lookback to capture boundary hours
                    if last_hour >= current_hour:
                        self.options.start = current_hour - dt.timedelta(hours=total_lookback)
                        print(f"[SILVER INCREMENTAL] Reloading from {self.options.start} (current hour: {current_hour}, lookback: {total_lookback}h)", flush=True)
                    else:
                        self.options.start = last_hour - dt.timedelta(hours=total_lookback)
                        print(f"[SILVER INCREMENTAL] Loading from {self.options.start} (last loaded: {silver_max_ts}, lookback: {total_lookback}h = offset:{hour_offset}h + tz:{tz_lookback}h)", flush=True)
                else:
                    # If timestamp is date type, start from next day
                    today = now.date()
                    # Handle both date and string types
                    if isinstance(silver_max_ts, dt.date):
                        last_date = silver_max_ts
                    elif isinstance(silver_max_ts, str):
                        last_date = dt.datetime.fromisoformat(silver_max_ts.replace('Z', '+00:00')).date()
                    else:
                        last_date = silver_max_ts.date()
                    
                    if last_date >= today:
                        self.options.start = today
                        print(f"[SILVER INCREMENTAL] Reloading today {self.options.start} (last loaded: {last_date})", flush=True)
                    else:
                        self.options.start = last_date + dt.timedelta(days=1)
                        print(f"[SILVER INCREMENTAL] Loading from {self.options.start} (last loaded: {last_date})", flush=True)
                        
            else:
                print("[SILVER INCREMENTAL] No existing Silver data found, will process all Bronze data", flush=True)
                # For first-run incremental, detect min Bronze timestamp to start from there
                bronze_min_ts = _get_bronze_min_ts()
                if bronze_min_ts:
                    self.options.start = bronze_min_ts
                    print(f"[SILVER INCREMENTAL FIRST-RUN] Setting start from Bronze min timestamp: {self.options.start}", flush=True)

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

    def _safe_read_silver(self) -> Optional[DataFrame]:
        """Try to read the target silver table, return None if it doesn't exist."""
        try:
            return self.spark.table(self.silver_table)
        except AnalysisException:
            return None

    def _maybe_set_conf(self, key: str, value: str) -> None:
        current = self.spark.conf.get(key, None)
        if current is None:
            self.spark.conf.set(key, value)


__all__ = ["LoadOptions", "BaseSilverLoader"]
