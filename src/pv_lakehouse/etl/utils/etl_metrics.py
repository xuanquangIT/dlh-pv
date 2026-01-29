#!/usr/bin/env python3
"""ETL metrics tracking utilities for Bronze/Silver/Gold loaders.

Note:
    Type hints use Python 3.9+ syntax (e.g., tuple[...], list[...]).
    The `from __future__ import annotations` import ensures compatibility
    by treating all annotations as strings at runtime.
"""
from __future__ import annotations
import logging
import time
from typing import Any, Optional, Sequence
LOGGER = logging.getLogger(__name__)
class ETLTimer:
    """Context manager and timer for tracking ETL operation duration.

    Example:
        >>> timer = ETLTimer()
        >>> # ... do work ...
        >>> elapsed = timer.elapsed()
        >>> print(f"Completed in {elapsed:.2f} seconds")
    """

    def __init__(self) -> None:
        """Initialize timer."""
        self.start_time: float = time.time()

    def elapsed(self) -> float:
        """Get elapsed time in seconds.

        Returns:
            Elapsed time since timer creation.
        """
        return time.time() - self.start_time

    def __enter__(self) -> ETLTimer:
        """Enter context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager."""
        pass

def log_fetch_start(
    logger: logging.Logger,
    dataset_name: str,
    total_count: int,
    date_range: Optional[tuple[Any, Any]] = None,
) -> None:
    """Log start of data fetch operation.

    Args:
        logger: Logger instance to use.
        dataset_name: Name of dataset being fetched (e.g., "facilities", "weather").
        total_count: Total number of items to fetch (facilities, records, etc.).
        date_range: Optional tuple of (start, end) dates/datetimes.

    Example:
        >>> log_fetch_start(LOGGER, "weather", 8, (start_date, end_date))
    """
    if date_range:
        start, end = date_range
        logger.info(
            "Starting %s fetch for %d items (date range: %s to %s)",
            dataset_name,
            total_count,
            start,
            end,
        )
    else:
        logger.info("Starting %s fetch for %d items", dataset_name, total_count)

def log_fetch_summary(
    logger: logging.Logger,
    dataset_name: str,
    total_count: int,
    success_count: int,
    failed_items: Sequence[str],
    total_rows: int,
    elapsed_seconds: float,
) -> None:
    """Log summary of fetch operation with success/failure metrics.

    Args:
        logger: Logger instance to use.
        dataset_name: Name of dataset fetched.
        total_count: Total number of items attempted.
        success_count: Number of successful fetches.
        failed_items: List of failed item identifiers.
        total_rows: Total rows fetched.
        elapsed_seconds: Time taken in seconds.

    Example:
        >>> log_fetch_summary(
        ...     LOGGER, "weather", 8, 6, ["FAC1", "FAC2"], 1440, 45.2
        ... )
    """
    if failed_items:
        logger.error(
            "%s fetch completed with failures: %d/%d items failed - %s",
            dataset_name.capitalize(),
            len(failed_items),
            total_count,
            failed_items,
        )

    logger.info(
        "%s fetch completed: %d/%d items succeeded, %d rows fetched in %.2f seconds",
        dataset_name.capitalize(),
        success_count,
        total_count,
        total_rows,
        elapsed_seconds,
    )

def log_etl_start(
    logger: logging.Logger,
    table_name: str,
    mode: str,
) -> None:
    """Log start of ETL operation.

    Args:
        logger: Logger instance to use.
        table_name: Target table name.
        mode: ETL mode (backfill, incremental, full).

    Example:
        >>> log_etl_start(LOGGER, "lh.bronze.raw_facilities", "backfill")
    """
    logger.info(
        "Starting ETL for %s (mode=%s)",
        table_name,
        mode,
    )

def log_etl_summary(
    logger: logging.Logger,
    table_name: str,
    rows_written: int,
    elapsed_seconds: float,
    operation: str = "ETL",
) -> None:
    """Log summary of ETL operation.

    Args:
        logger: Logger instance to use.
        table_name: Target table name.
        rows_written: Number of rows written.
        elapsed_seconds: Time taken in seconds.
        operation: Operation name (default: "ETL").

    Example:
        >>> log_etl_summary(LOGGER, "lh.bronze.raw_facilities", 8, 12.5)
    """
    logger.info(
        "%s completed: %d rows written to %s in %.2f seconds",
        operation,
        rows_written,
        table_name,
        elapsed_seconds,
    )

__all__ = [
    "ETLTimer",
    "log_fetch_start",
    "log_fetch_summary",
    "log_etl_start",
    "log_etl_summary",
]
