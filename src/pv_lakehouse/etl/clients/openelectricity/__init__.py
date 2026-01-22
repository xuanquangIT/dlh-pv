"""OpenElectricity API Client Package.

This package provides a clean, testable interface to the OpenElectricity API
with support for:
- Type-safe responses via Pydantic models
- Dependency injection for HTTP client (testability)
- Exponential backoff retry logic (resilience)

Example usage:
    >>> from pv_lakehouse.etl.clients.openelectricity import OpenElectricityClient
    >>> client = OpenElectricityClient(api_key="your-key")
    >>> df = client.fetch_facilities()
    
    # Or use the backward-compatible module functions:
    >>> from pv_lakehouse.etl.clients.openelectricity import fetch_facilities_dataframe
    >>> df = fetch_facilities_dataframe(api_key="your-key")
"""

from __future__ import annotations

# Re-export main client class and functions
from .client import (
    OpenElectricityClient,
    HTTPClient,
    RequestsHTTPClient,
    load_api_key,
    fetch_facilities_dataframe,
    fetch_facility_timeseries_dataframe,
    CANDIDATE_ENV_KEYS,
    SUPPORTED_INTERVALS,
    INTERVAL_MAX_DAYS,
    TARGET_WINDOW_DAYS,
    MAX_LOOKBACK_WINDOWS,
)

# Re-export models
from .models import (
    FACILITY_SCHEMA,
    Location,
    Unit,
    Facility,
    FacilityMetadata,
    FacilitiesResponse,
    TimeseriesColumns,
    TimeseriesResult,
    TimeseriesSeries,
    TimeseriesResponse,
    FacilitySummary,
    TimeseriesRow,
    ClientConfig,
    DateRange,
)

# Re-export parsers (for advanced usage)
from .parsers import (
    get_timezone_offset_hours,
    get_timezone_id,
    resolve_network_timezone,
    parse_naive_datetime,
    format_naive_datetime,
    chunk_date_range,
    extract_max_days_from_error,
    load_default_facility_codes,
    safe_float,
    as_str,
    summarize_facility,
    parse_facility_metadata,
    build_unit_to_facility_map,
    flatten_timeseries,
    NETWORK_TIMEZONE_IDS,
    NETWORK_FALLBACK_OFFSETS,
)


__all__ = [
    # Main client
    "OpenElectricityClient",
    "HTTPClient",
    "RequestsHTTPClient",
    "load_api_key",
    # Backward-compatible functions
    "fetch_facilities_dataframe",
    "fetch_facility_timeseries_dataframe",
    # Models
    "FACILITY_SCHEMA",
    "Location",
    "Unit",
    "Facility",
    "FacilityMetadata",
    "FacilitiesResponse",
    "TimeseriesColumns",
    "TimeseriesResult",
    "TimeseriesSeries",
    "TimeseriesResponse",
    "FacilitySummary",
    "TimeseriesRow",
    "ClientConfig",
    "DateRange",
    # Parsers
    "get_timezone_offset_hours",
    "get_timezone_id",
    "resolve_network_timezone",
    "parse_naive_datetime",
    "format_naive_datetime",
    "chunk_date_range",
    "extract_max_days_from_error",
    "load_default_facility_codes",
    "safe_float",
    "as_str",
    "summarize_facility",
    "parse_facility_metadata",
    "build_unit_to_facility_map",
    "flatten_timeseries",
    # Constants
    "CANDIDATE_ENV_KEYS",
    "SUPPORTED_INTERVALS",
    "INTERVAL_MAX_DAYS",
    "TARGET_WINDOW_DAYS",
    "MAX_LOOKBACK_WINDOWS",
    "NETWORK_TIMEZONE_IDS",
    "NETWORK_FALLBACK_OFFSETS",
]
