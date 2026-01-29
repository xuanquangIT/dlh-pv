#!/usr/bin/env python3
"""SQL query templates for Bronze layer with table whitelist validation."""
from __future__ import annotations
import re
from typing import Sequence
# Whitelist of allowed Bronze table names to prevent SQL injection
# Table names can be qualified (e.g., schema.table) and are validated via whitelist only
ALLOWED_BRONZE_TABLES = frozenset({
    "lh.bronze.raw_facilities",
    "lh.bronze.raw_facility_energy",
    "lh.bronze.raw_facility_weather",
    "lh.bronze.raw_facility_air_quality",
})

# Regex pattern for valid UNQUALIFIED SQL identifiers (column names, view names)
# Allows: ASCII alphanumeric, underscore, starts with ASCII letter/underscore
_SQL_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$", re.ASCII)

def _validate_table(table: str) -> None:
    """Validate table name is in whitelist (internal use).

    This is a private helper function used by query builders to ensure
    table names are in the allowed whitelist before SQL generation.

    Args:
        table: Fully qualified table name.

    Raises:
        ValueError: If table not in whitelist.
    """
    if table not in ALLOWED_BRONZE_TABLES:
        raise ValueError(f"Table '{table}' not in allowed list: {ALLOWED_BRONZE_TABLES}")

def _validate_sql_identifier(identifier: str, param_name: str) -> None:
    """Validate that a string is a safe UNQUALIFIED SQL identifier.

    Ensures the identifier contains only alphanumeric characters and underscores,
    and starts with a letter or underscore. This prevents SQL injection attacks
    through column names, view names, or other unqualified SQL identifiers.

    Note: This function validates UNQUALIFIED identifiers only (no dots allowed).
    Qualified table names (e.g., schema.table) are validated via ALLOWED_BRONZE_TABLES
    whitelist in _validate_table() function.

    Args:
        identifier: The unqualified SQL identifier to validate (column, view name, etc.).
        param_name: Name of the parameter for error messages.

    Raises:
        ValueError: If identifier is empty, None, contains unsafe characters, or is a SQL keyword.
    """
    if not identifier:
        raise ValueError(f"{param_name} cannot be empty or None")

    if not isinstance(identifier, str):
        raise ValueError(f"{param_name} must be a string, got {type(identifier).__name__}")

    if not _SQL_IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(
            f"Invalid SQL identifier for {param_name}: '{identifier}'. "
            f"Must contain only ASCII alphanumeric characters and underscores, "
            f"and start with a letter or underscore."
        )

def _validate_column_list(columns: Sequence[str], param_name: str) -> None:
    """Validate that all column names in a sequence are safe SQL identifiers.

    Args:
        columns: Sequence of column names to validate.
        param_name: Name of the parameter for error messages.

    Raises:
        ValueError: If any column name is invalid.
    """
    if not columns:
        raise ValueError(f"{param_name} cannot be empty")

    for i, col in enumerate(columns):
        try:
            _validate_sql_identifier(col, f"{param_name}[{i}]")
        except ValueError as e:
            raise ValueError(f"Invalid column in {param_name}: {e}") from e

def build_max_timestamp_query(table: str, timestamp_column: str) -> str:
    """Build query to get max timestamp from table.

    Args:
        table: Fully qualified table name (validated against whitelist).
        timestamp_column: Column name containing timestamp.

    Returns:
        SQL query string.

    Raises:
        ValueError: If table not in whitelist or timestamp_column is invalid.
    """
    _validate_table(table)
    _validate_sql_identifier(timestamp_column, "timestamp_column")
    # Safe to use f-string since all identifiers are validated above
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
        ValueError: If table not in whitelist, keys is empty, or any identifier is invalid.
    """
    _validate_table(table)
    _validate_sql_identifier(source_view, "source_view")
    _validate_column_list(keys, "keys")
    _validate_sql_identifier(timestamp_column, "timestamp_column")

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
        ValueError: If table not in whitelist, keys is empty, or any identifier is invalid.
    """
    _validate_table(table)
    _validate_sql_identifier(source_view, "source_view")
    _validate_column_list(keys, "keys")

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
