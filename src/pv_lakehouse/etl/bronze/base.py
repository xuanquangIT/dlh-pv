#!/usr/bin/env python3
"""Shared base classes and options for Bronze-zone loaders."""

from __future__ import annotations

import datetime as dt
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException, ParseException

from pv_lakehouse.etl.bronze.sql_templates import (
    ALLOWED_BRONZE_TABLES,
    build_merge_query,
    build_max_timestamp_query,
)
from pv_lakehouse.etl.utils import resolve_facility_codes
from pv_lakehouse.etl.utils.spark_utils import (
    create_spark_session,
    write_iceberg_table,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class BronzeLoadOptions:
    """Parameters controlling a bronze load execution.

    Attributes:
        mode: Load mode - 'backfill' for complete reload, 'incremental' for delta.
        start: Start datetime for filtering (format varies by loader).
        end: End datetime for filtering.
        facility_codes: Comma-separated facility codes or None for all.
        api_key: Optional API key override.
        max_workers: Concurrent threads for API calls.
        app_name: Spark application name.
    """

    mode: str = "incremental"
    start: Optional[dt.datetime] = None
    end: Optional[dt.datetime] = None
    facility_codes: Optional[str] = None
    api_key: Optional[str] = None
    max_workers: int = 4
    app_name: str = "bronze-loader"


class BaseBronzeLoader(ABC):
    """Base helper offering shared orchestration behaviour for Bronze layer.

    Subclasses must define class attributes and implement abstract methods:
    - iceberg_table: Target Iceberg table name
    - timestamp_column: Column name for incremental detection
    - merge_keys: Columns used as keys for MERGE INTO deduplication

    Abstract methods to implement:
    - fetch_data(): Fetch raw data from external API
    - transform(): Apply Bronze-layer transformations
    """

    # Class attributes to be defined by subclasses
    iceberg_table: str
    timestamp_column: str
    merge_keys: Sequence[str] = ()

    def __init__(self, options: Optional[BronzeLoadOptions] = None) -> None:
        """Initialize loader with options."""
        self.options = options or BronzeLoadOptions()
        self._spark: Optional[SparkSession] = None
        self._validate_table()
        self._initialize_date_range()

    def _validate_table(self) -> None:
        """Validate iceberg_table is in allowed list."""
        if self.iceberg_table not in ALLOWED_BRONZE_TABLES:
            raise ValueError(
                f"Table '{self.iceberg_table}' not in allowed list: {ALLOWED_BRONZE_TABLES}"
            )

    def _initialize_date_range(self) -> None:
        """Initialize date range for incremental loads. Override if needed."""
        pass

    @property
    def spark(self) -> SparkSession:
        """Get or create SparkSession."""
        if self._spark is None:
            self._spark = create_spark_session(self.options.app_name)
        return self._spark

    def close(self) -> None:
        """Stop SparkSession."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def resolve_facilities(self) -> List[str]:
        """Resolve facility codes from options or defaults."""
        return resolve_facility_codes(self.options.facility_codes)

    def get_max_timestamp(self) -> Optional[dt.datetime]:
        """Get max timestamp from target table for incremental detection.

        Returns:
            Max timestamp or None if table is empty or doesn't exist.
        """
        try:
            query = build_max_timestamp_query(self.iceberg_table, self.timestamp_column)
            result = self.spark.sql(query).collect()
            if result and len(result) > 0 and result[0][0] is not None:
                return result[0][0]
            return None
        except (AnalysisException, ParseException) as e:
            LOGGER.warning("Spark SQL error querying %s: %s", self.iceberg_table, e)
            return None
        except (ValueError, RuntimeError) as e:
            LOGGER.warning("Could not query max timestamp from %s: %s", self.iceberg_table, e)
            return None

    def add_ingest_columns(self, df: DataFrame) -> DataFrame:
        """Add standard Bronze ingest metadata columns.

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with ingest_mode, ingest_timestamp columns added.
        """
        return (
            df.withColumn("ingest_mode", F.lit(self.options.mode))
            .withColumn("ingest_timestamp", F.current_timestamp())
        )

    def write_merge(self, df: DataFrame, source_view: str = "bronze_source") -> None:
        """Write DataFrame using MERGE INTO for upsert with deduplication.

        Args:
            df: DataFrame to write.
            source_view: Temp view name for MERGE source.

        Raises:
            RuntimeError: If both MERGE and append fallback fail.
        """
        df.createOrReplaceTempView(source_view)
        merge_sql = build_merge_query(
            table=self.iceberg_table,
            source_view=source_view,
            keys=self.merge_keys,
            timestamp_column=self.timestamp_column,
        )
        try:
            self.spark.sql(merge_sql)
            LOGGER.info("MERGE completed for %s", self.iceberg_table)
        except (AnalysisException, ParseException) as e:
            LOGGER.warning(
                "MERGE failed for %s: %s. Falling back to append...",
                self.iceberg_table,
                e,
            )
            try:
                write_iceberg_table(df, self.iceberg_table, mode="append")
                LOGGER.info("Append fallback succeeded for %s", self.iceberg_table)
            except Exception as append_error:
                LOGGER.error(
                    "Append fallback also failed for %s: %s",
                    self.iceberg_table,
                    append_error,
                )
                raise RuntimeError(
                    f"Failed to write to {self.iceberg_table}: "
                    f"MERGE error: {e}, Append error: {append_error}"
                ) from append_error
        except (ValueError, RuntimeError) as e:
            LOGGER.warning(
                "MERGE failed for %s: %s. Falling back to append...",
                self.iceberg_table,
                e,
            )
            try:
                write_iceberg_table(df, self.iceberg_table, mode="append")
                LOGGER.info("Append fallback succeeded for %s", self.iceberg_table)
            except Exception as append_error:
                LOGGER.error(
                    "Append fallback also failed for %s: %s",
                    self.iceberg_table,
                    append_error,
                )
                raise RuntimeError(
                    f"Failed to write to {self.iceberg_table}: "
                    f"MERGE error: {e}, Append error: {append_error}"
                ) from append_error

    def write_overwrite(self, df: DataFrame) -> None:
        """Write DataFrame with full table overwrite.

        Args:
            df: DataFrame to write.
        """
        write_iceberg_table(df, self.iceberg_table, mode="overwrite")
        row_count = df.count()
        LOGGER.info("Wrote %d rows to %s (mode=overwrite)", row_count, self.iceberg_table)

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """Fetch raw data from external API.

        Returns:
            Pandas DataFrame with raw data.
        """
        raise NotImplementedError

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply Bronze-layer transformations.

        Args:
            df: Spark DataFrame from fetch_data().

        Returns:
            Transformed DataFrame ready for writing.
        """
        raise NotImplementedError

    def run(self) -> int:
        """Execute full ETL pipeline: fetch, transform, write.

        Returns:
            Number of rows written.

        Raises:
            ConnectionError: If API connection fails.
            ValueError: If data validation or transformation fails.
            RuntimeError: If Spark operations or write operations fail.
        """
        try:
            # Fetch data from API
            try:
                pandas_df = self.fetch_data()
            except (ConnectionError, TimeoutError, OSError) as e:
                LOGGER.error(
                    "API connection failed for %s: %s", self.iceberg_table, e
                )
                raise ConnectionError(
                    f"Failed to fetch data from API: {e}"
                ) from e
            except (ValueError, KeyError) as e:
                LOGGER.error("Data validation failed for %s: %s", self.iceberg_table, e)
                raise ValueError(f"Invalid data received from API: {e}") from e

            if pandas_df is None or pandas_df.empty:
                LOGGER.info("No data fetched; skipping writes.")
                return 0

            # Convert to Spark and transform
            try:
                spark_df = self.spark.createDataFrame(pandas_df)
                spark_df = self.transform(spark_df)
                spark_df = self.add_ingest_columns(spark_df)
            except (AnalysisException, ParseException) as e:
                LOGGER.error(
                    "Spark transformation failed for %s: %s", self.iceberg_table, e
                )
                raise RuntimeError(
                    f"Failed to transform data in Spark: {e}"
                ) from e
            except (ValueError, TypeError) as e:
                LOGGER.error(
                    "Data transformation logic failed for %s: %s",
                    self.iceberg_table,
                    e,
                )
                raise ValueError(f"Transformation error: {e}") from e

            # Write based on mode
            try:
                if self.options.mode == "backfill":
                    self.write_overwrite(spark_df)
                else:
                    self.write_merge(spark_df)
            except Exception as e:
                LOGGER.error(
                    "Write operation failed for %s: %s", self.iceberg_table, e
                )
                raise RuntimeError(
                    f"Failed to write to {self.iceberg_table}: {e}"
                ) from e

            row_count = spark_df.count()
            LOGGER.info("Loader completed: %d rows written to %s", row_count, self.iceberg_table)
            return row_count

        finally:
            self.close()


__all__ = ["BronzeLoadOptions", "BaseBronzeLoader"]
