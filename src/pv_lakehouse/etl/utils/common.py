"""Shared utility functions for ETL clients.

This module contains common utility functions used across multiple ETL modules
to avoid code duplication.
"""

from __future__ import annotations

from typing import List, Optional


def parse_csv(value: Optional[str]) -> List[str]:
    """Parse a comma-separated string into a list of trimmed strings.
    
    Args:
        value: Comma-separated string (e.g., "a, b, c") or None
        
    Returns:
        List of trimmed, non-empty strings
        
    Example:
        >>> parse_csv("FACILITY1, FACILITY2, FACILITY3")
        ['FACILITY1', 'FACILITY2', 'FACILITY3']
        >>> parse_csv(None)
        []
        >>> parse_csv("")
        []
    """
    return [item.strip() for item in (value or "").split(",") if item.strip()]


__all__ = ["parse_csv"]
