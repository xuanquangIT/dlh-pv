#!/usr/bin/env python3
"""Facility timezone mappings for Australia solar facilities."""

from __future__ import annotations

from typing import Dict

# Map facility codes to their local timezone
FACILITY_TIMEZONES: Dict[str, str] = {
    # "NYNGAN": "Australia/Sydney",      # NSW - UTC+10 (or +11 during DST)
}

# Default timezone if facility not found
DEFAULT_TIMEZONE = "Australia/Brisbane"


def get_facility_timezone(facility_code: str) -> str:
    """
    Get the local timezone for a facility.
    
    Args:
        facility_code: Facility code (e.g., "NYNGAN")
    
    Returns:
        IANA timezone string (e.g., "Australia/Sydney")
    """
    return FACILITY_TIMEZONES.get(facility_code.upper(), DEFAULT_TIMEZONE)
