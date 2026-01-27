#!/usr/bin/env python3
"""SQL query templates for Bronze layer with table whitelist validation."""

from __future__ import annotations

from typing import Sequence

# Whitelist of allowed Bronze table names to prevent SQL injection
ALLOWED_BRONZE_TABLES = frozenset({
    "lh.bronze.raw_facilities",
    "lh.bronze.raw_facility_energy",
    "lh.bronze.raw_facility_weather",
    "lh.bronze.raw_facility_air_quality",
})


def _validate_table(table: str) -> None:
    """Validate table name is in whitelist.

    Args:
        table: Fully qualified table name.

    Raises:
        ValueError: If table not in whitelist.
    """
    if table not in ALLOWED_BRONZE_TABLES:
        raise ValueError(f"Table '{table}' not in allowed list: {ALLOWED_BRONZE_TABLES}")


def build_max_timestamp_query(table: str, timestamp_column: str) -> str:
    """Build query to get max timestamp from table.

    Args:
        table: Fully qualified table name (validated against whitelist).
        timestamp_column: Column name containing timestamp.

    Returns:
        SQL query string.

    Raises:
        ValueError: If table not in whitelist.
    """
    _validate_table(table)
    # Safe to use format() since table name is validated against whitelist
    return f"SELECT MAX({timestamp_column}) FROM {table}"


def build_merge_query(
    table: str,
    source_view: str,
    keys: Sequence[str],
    timestamp_column: str,
) -> str:
    """Build MERGE INTO query with deduplication.

    Uses ROW_NUMBER() to keep only the latest record per key combination,
    then performs upsert into target table.

    Args:
        table: Fully qualified table name (validated against whitelist).
        source_view: Name of temp view containing source data.
        keys: Columns to use as merge keys.
        timestamp_column: Timestamp column for ordering (keep latest).

    Returns:
        SQL MERGE INTO query string.

    Raises:
        ValueError: If table not in whitelist or keys is empty.
    """
    _validate_table(table)
    if not keys:
        raise ValueError("merge_keys cannot be empty")

    # Build ON clause: target.key1 = source.key1 AND target.key2 = source.key2 ...
    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in keys)

    # Build PARTITION BY clause for deduplication
    partition_cols = ", ".join(keys)

    return f"""
    MERGE INTO {table} AS target
    USING (
        SELECT * FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY ingest_timestamp DESC) as rn
            FROM {source_view}
        ) WHERE rn = 1
    ) AS source
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """


def build_overwrite_with_dedup_query(
    table: str,
    source_view: str,
    keys: Sequence[str],
) -> str:
    """Build INSERT OVERWRITE query with deduplication.

    Args:
        table: Fully qualified table name (validated against whitelist).
        source_view: Name of temp view containing source data.
        keys: Columns to use for deduplication.

    Returns:
        SQL INSERT OVERWRITE query string.

    Raises:
        ValueError: If table not in whitelist or keys is empty.
    """
    _validate_table(table)
    if not keys:
        raise ValueError("keys cannot be empty")

    partition_cols = ", ".join(keys)

    return f"""
    INSERT OVERWRITE TABLE {table}
    SELECT * FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY ingest_timestamp DESC) as rn
        FROM {source_view}
    ) WHERE rn = 1
    """


__all__ = [
    "ALLOWED_BRONZE_TABLES",
    "build_max_timestamp_query",
    "build_merge_query",
    "build_overwrite_with_dedup_query",
]
