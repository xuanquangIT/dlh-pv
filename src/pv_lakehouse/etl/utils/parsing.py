from __future__ import annotations
from typing import List, Optional

def parse_csv_list(value: Optional[str]) -> List[str]:
    """Parse a comma-separated string into a list of cleaned tokens.

    Handles None values, empty strings, and whitespace-padded items.
    Empty items after stripping whitespace are discarded.

    Args:
        value: Comma-separated string to parse, or None.

    Returns:
        List of stripped, non-empty string tokens.

    Examples:
        >>> parse_csv_list("a, b, c")
        ['a', 'b', 'c']
        >>> parse_csv_list("  item1  ,  item2  ")
        ['item1', 'item2']
        >>> parse_csv_list(None)
        []
    """
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]

def parse_uppercase_csv_list(value: Optional[str]) -> List[str]:
    """Parse a comma-separated string into uppercase tokens.

    Combines CSV parsing with uppercase normalization, commonly used
    for facility codes which are case-insensitive identifiers.

    Args:
        value: Comma-separated string to parse, or None.

    Returns:
        List of stripped, uppercased, non-empty string tokens.

    Examples:
        >>> parse_uppercase_csv_list("abc, DEF, ghi")
        ['ABC', 'DEF', 'GHI']
        >>> parse_uppercase_csv_list(None)
        []
    """
    return [item.upper() for item in parse_csv_list(value)]

__all__ = [
    "parse_csv_list",
    "parse_uppercase_csv_list",
]
