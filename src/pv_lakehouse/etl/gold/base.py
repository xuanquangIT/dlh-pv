"""Shared base classes for Gold-zone loaders."""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

LOGGER = logging.getLogger(__name__)

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
    """Parameters controlling a gold load execution.

    Attributes:
        mode: Load mode - 'full' for complete reload, 'incremental' for delta.
        start: Start datetime for incremental filtering.
        end: End datetime for incremental filtering.
        load_strategy: Write strategy - 'overwrite' or 'merge'.
        app_name: Spark application name for job identification.
        target_file_size_mb: Target Iceberg file size in megabytes.
        max_records_per_file: Maximum records per output file.
        broadcast_threshold_mb: Threshold for auto-broadcast joins (MB).
        enable_aqe: Enable Spark Adaptive Query Execution.
        partition_pruning: Enable partition filter pushdown.
        merge_keys: Keys for MERGE INTO operation.
        explain_plan: Enable query plan logging for debugging.
        explain_mode: Plan verbosity - 'simple', 'extended', 'codegen', 'cost'.
    """

    mode: str = "incremental"
    start: Optional[dt.datetime] = None
    end: Optional[dt.datetime] = None
    load_strategy: str = "merge"
    app_name: str = "gold-loader"
    target_file_size_mb: int = 128
    max_records_per_file: int = 250_000
    broadcast_threshold_mb: int = 10
    enable_aqe: bool = True
    partition_pruning: bool = True
    merge_keys: Optional[List[str]] = None
    explain_plan: bool = False
    explain_mode: str = "simple"


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
        """Get or create SparkSession with optimized Gold layer configuration."""
        if self._spark is None:
            self._spark = create_spark_session(self.options.app_name)
            self._configure_spark_optimizations()
        return self._spark

    def _configure_spark_optimizations(self) -> None:
        """Apply Spark configurations for Gold layer workloads.

        Configures shuffle partitions, broadcast thresholds, and Adaptive
        Query Execution (AQE) settings based on GoldLoadOptions.
        """
        # Reduce shuffle partitions for smaller Gold layer datasets
        self._maybe_set_conf("spark.sql.shuffle.partitions", "8")

        # Configure broadcast join threshold from options
        threshold_bytes = str(self.options.broadcast_threshold_mb * 1024 * 1024)
        self._maybe_set_conf("spark.sql.autoBroadcastJoinThreshold", threshold_bytes)

        # Enable Adaptive Query Execution for runtime optimization
        if self.options.enable_aqe:
            self._maybe_set_conf("spark.sql.adaptive.enabled", "true")
            self._maybe_set_conf("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self._maybe_set_conf("spark.sql.adaptive.localShuffleReader.enabled", "true")
            self._maybe_set_conf("spark.sql.adaptive.skewJoin.enabled", "true")

        LOGGER.info(
            "Gold layer Spark config: broadcast_threshold=%dMB, aqe=%s, partitions=8",
            self.options.broadcast_threshold_mb,
            self.options.enable_aqe,
        )

    def close(self) -> None:
        """Stop SparkSession and cleanup staging directories."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            cleanup_spark_staging()

    def run(self) -> int:
        """Execute the full ETL pipeline: read, transform, write.

        Returns:
            Total number of rows written across all output tables.
        """
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

                # Log explain plan before materialization if enabled
                self._log_explain_plan(dataframe, f"pre_write_{name}")

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
    # Explain plan and debugging utilities
    # ------------------------------------------------------------------
    def _log_explain_plan(self, df: DataFrame, stage: str) -> None:
        """Log DataFrame query execution plan for debugging.

        Args:
            df: DataFrame to explain.
            stage: Descriptive name for the transformation stage.
        """
        if not self.options.explain_plan:
            return

        try:
            explain_mode = self.options.explain_mode.lower()
            if explain_mode == "extended":
                explain_str = df._jdf.queryExecution().toString()
            elif explain_mode == "codegen":
                explain_str = df._jdf.queryExecution().debug().codegen()
            elif explain_mode == "cost":
                explain_str = df._jdf.queryExecution().optimizedPlan().stats().toString()
            else:
                explain_str = df._jdf.queryExecution().simpleString()

            LOGGER.info("[EXPLAIN] Stage: %s\n%s", stage, explain_str)
        except Exception as e:
            LOGGER.warning("Failed to generate explain plan for stage %s: %s", stage, e)

    def _log_join_strategy(self, df: DataFrame, join_name: str) -> None:
        """Log the join strategy used by Spark optimizer.

        Args:
            df: DataFrame result of a join operation.
            join_name: Descriptive name for the join being analyzed.
        """
        if not self.options.explain_plan:
            return

        try:
            plan_str = df._jdf.queryExecution().simpleString()
            join_types = ["BroadcastHashJoin", "SortMergeJoin", "BroadcastNestedLoopJoin"]
            detected = [jt for jt in join_types if jt in plan_str]
            if detected:
                LOGGER.info("[JOIN] %s uses: %s", join_name, ", ".join(detected))
        except Exception as e:
            LOGGER.debug("Failed to detect join strategy for %s: %s", join_name, e)

    def _log_transformation_metrics(self, df: DataFrame, stage: str) -> None:
        """Log DataFrame metrics for performance monitoring.

        Args:
            df: DataFrame to analyze.
            stage: Descriptive name for the transformation stage.
        """
        if not self.options.explain_plan:
            return

        try:
            partition_count = df.rdd.getNumPartitions()
            LOGGER.info("[METRICS] %s: partitions=%d", stage, partition_count)
        except Exception as e:
            LOGGER.debug("Failed to collect metrics for %s: %s", stage, e)

    # ------------------------------------------------------------------
    # Abstract hooks
    # ------------------------------------------------------------------
    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:  # pragma: no cover - abstract
        """Transform source DataFrames into Gold layer outputs.

        Args:
            sources: Dictionary mapping source names to DataFrames.

        Returns:
            Dictionary mapping output names to transformed DataFrames.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Shared utilities
    # ------------------------------------------------------------------
    def _auto_detect_start_time(self) -> None:
        """
        Auto-detect start time from Gold table's last date_key (if exists),
        or from Silver source tables' updated_at timestamps.
        
        Priority:
        1. If Gold table exists: Use MAX(date_key) from Gold + 1 day (ensures we don't reload)
        2. Else: Use MIN of MAX(timestamp_column) from Silver sources
        """
        if self.options.mode != "incremental" or self.options.start is not None:
            return  # Only auto-detect for incremental mode without explicit start
        
        # First, try to get last date from Gold table (if it exists)
        if hasattr(self, "gold_tables") and self.gold_tables:
            for gold_table_name, gold_config in self.gold_tables.items():
                try:
                    max_date_row = self.spark.sql(f"""
                        SELECT MAX(date_key) as max_date_key
                        FROM {gold_config.iceberg_table}
                    """).collect()
                    
                    if max_date_row and max_date_row[0]["max_date_key"] is not None:
                        max_date_key = max_date_row[0]["max_date_key"]
                        # Convert YYYYMMDD to datetime
                        try:
                            max_date = dt.datetime.strptime(str(max_date_key), "%Y%m%d")
                            self.options.start = max_date + dt.timedelta(days=1)
                            print(f"[GOLD INCREMENTAL] Gold table {gold_config.iceberg_table} last date: {max_date_key}", flush=True)
                            print(f"[GOLD INCREMENTAL]   Will load from: {self.options.start}", flush=True)
                            return  # Found Gold data, use it
                        except ValueError:
                            pass
                except Exception:
                    # Table doesn't exist or query failed - continue to Silver check
                    pass
        
        # Fallback: If Gold table is empty or doesn't exist, check Silver sources
        if not hasattr(self, "source_tables") or not self.source_tables:
            return  # No source tables defined
        
        max_timestamps = []
        
        # Query MAX(timestamp_column) from each Silver source table
        for source_name, source_config in self.source_tables.items():
            if not source_config.timestamp_column:
                continue  # Skip sources without timestamp column (e.g., dimension tables)
            
            try:
                max_ts_row = self.spark.sql(f"""
                    SELECT MAX({source_config.timestamp_column}) as max_ts
                    FROM {source_config.table_name}
                """).collect()
                
                if max_ts_row and max_ts_row[0]["max_ts"] is not None:
                    max_ts = max_ts_row[0]["max_ts"]
                    max_timestamps.append((source_config.table_name, max_ts))
            except Exception:
                # Table doesn't exist yet or query failed - skip
                continue
        
        if max_timestamps:
            # Use the MINIMUM of all max timestamps to ensure we don't miss any data
            # (in case some Silver tables are behind others)
            min_of_max = min(ts for _, ts in max_timestamps)
            
            # Start from the next hour after the earliest max timestamp
            if isinstance(min_of_max, dt.datetime):
                self.options.start = min_of_max + dt.timedelta(hours=1)
            else:
                # If timestamp is date type, start from next day
                self.options.start = min_of_max + dt.timedelta(days=1)
            
            print("[GOLD INCREMENTAL] Gold table empty, using Silver sources:", flush=True)
            print("[GOLD INCREMENTAL] Auto-detected last loaded timestamps from Silver sources:", flush=True)
            for table_name, max_ts in max_timestamps:
                print(f"[GOLD INCREMENTAL]   {table_name}: {max_ts}", flush=True)
            print(f"[GOLD INCREMENTAL]   Using earliest: {min_of_max}", flush=True)
            print(f"[GOLD INCREMENTAL]   Will load from: {self.options.start}", flush=True)
        else:
            print("[GOLD INCREMENTAL] No existing Silver data found, will process all Silver data", flush=True)

    def _read_sources(self) -> Dict[str, DataFrame]:
        """Read source tables with optional partition pruning.

        Applies timestamp filters and partition pruning based on
        GoldLoadOptions configuration for efficient data retrieval.

        Returns:
            Dictionary mapping source names to filtered DataFrames.
        """
        # Auto-detect start time before reading sources
        self._auto_detect_start_time()
        
        frames: Dict[str, DataFrame] = {}
        for name, config in self.source_tables.items():
            dataframe = self._read_table_with_pruning(config)
            if dataframe is None:
                continue
            frames[name] = dataframe
        return frames

    def _read_table_with_pruning(self, config: SourceTableConfig) -> Optional[DataFrame]:
        """Read table with partition pruning and timestamp filtering.

        Applies partition filter pushdown when date_key column exists,
        otherwise falls back to timestamp column filtering.

        Args:
            config: Source table configuration.

        Returns:
            Filtered DataFrame or None if table doesn't exist.
        """
        try:
            dataframe = self.spark.table(config.table_name)
        except AnalysisException:
            return None

        # Apply partition pruning if enabled and date_key column exists
        if self.options.partition_pruning and "date_key" in dataframe.columns:
            dataframe = self._apply_partition_pruning(dataframe, config)
        elif config.timestamp_column:
            dataframe = self._apply_timestamp_filter(dataframe, config)

        required_columns = set(config.required_columns or [])
        missing = required_columns - set(dataframe.columns)
        if missing:
            raise ValueError(
                f"Missing expected columns {sorted(missing)} in table {config.table_name}"
            )
        return dataframe

    def _apply_partition_pruning(
        self, dataframe: DataFrame, config: SourceTableConfig
    ) -> DataFrame:
        """Apply date_key partition filter for efficient data scanning.

        Converts datetime options to date_key format (YYYYMMDD) and applies
        filter predicates that Spark can push down to partition pruning.

        Args:
            dataframe: Source DataFrame with date_key column.
            config: Source table configuration.

        Returns:
            DataFrame with partition filters applied.
        """
        if self.options.start:
            start_date = (
                self.options.start.date()
                if isinstance(self.options.start, dt.datetime)
                else self.options.start
            )
            start_key = int(start_date.strftime("%Y%m%d"))
            dataframe = dataframe.filter(F.col("date_key") >= start_key)
            LOGGER.debug(
                "Applied partition pruning: date_key >= %d for %s",
                start_key,
                config.table_name,
            )

        if self.options.end:
            end_date = (
                self.options.end.date()
                if isinstance(self.options.end, dt.datetime)
                else self.options.end
            )
            end_key = int(end_date.strftime("%Y%m%d"))
            dataframe = dataframe.filter(F.col("date_key") <= end_key)
            LOGGER.debug(
                "Applied partition pruning: date_key <= %d for %s",
                end_key,
                config.table_name,
            )

        return dataframe

    def _apply_timestamp_filter(
        self, dataframe: DataFrame, config: SourceTableConfig
    ) -> DataFrame:
        """Apply timestamp column filter for incremental loading.

        Args:
            dataframe: Source DataFrame.
            config: Source table configuration with timestamp_column.

        Returns:
            DataFrame with timestamp filters applied.
        """
        timestamp_column = config.timestamp_column
        if not timestamp_column:
            return dataframe

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

        return dataframe

    def _read_table(self, config: SourceTableConfig) -> Optional[DataFrame]:
        """Read table with timestamp filtering (legacy method).

        Deprecated: Use _read_table_with_pruning instead.

        Args:
            config: Source table configuration.

        Returns:
            Filtered DataFrame or None if table doesn't exist.
        """
        return self._read_table_with_pruning(config)

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
        """Set Spark configuration if not already defined.

        Args:
            key: Configuration key name.
            value: Configuration value to set.
        """
        current = self.spark.conf.get(key, None)
        if current is None:
            self.spark.conf.set(key, value)

    def _validate_options(self) -> None:
        """Validate GoldLoadOptions configuration.

        Raises:
            ValueError: If any option value is invalid.
        """
        if self.options.mode not in {"full", "incremental"}:
            raise ValueError("GoldLoadOptions.mode must be 'full' or 'incremental'")
        if self.options.load_strategy not in {"overwrite", "merge"}:
            raise ValueError("GoldLoadOptions.load_strategy must be 'overwrite' or 'merge'")
        if self.options.explain_mode not in {"simple", "extended", "codegen", "cost"}:
            raise ValueError("GoldLoadOptions.explain_mode must be 'simple', 'extended', 'codegen', or 'cost'")
        if self.options.broadcast_threshold_mb < 0:
            raise ValueError("GoldLoadOptions.broadcast_threshold_mb must be non-negative")

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
