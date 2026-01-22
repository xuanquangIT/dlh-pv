"""Pydantic models for OpenElectricity API responses.

Provides type safety and validation for API data structures.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator
from pyspark.sql import types as T


# ─────────────────────────────────────────────────────────────────────────────
# Spark Schema (for Bronze layer)
# ─────────────────────────────────────────────────────────────────────────────

FACILITY_SCHEMA = T.StructType(
    [
        T.StructField("facility_code", T.StringType(), True),
        T.StructField("facility_name", T.StringType(), True),
        T.StructField("network_id", T.StringType(), True),
        T.StructField("network_region", T.StringType(), True),
        T.StructField("facility_created_at", T.StringType(), True),
        T.StructField("facility_updated_at", T.StringType(), True),
        T.StructField("location_lat", T.DoubleType(), True),
        T.StructField("location_lng", T.DoubleType(), True),
        T.StructField("unit_count", T.IntegerType(), True),
        T.StructField("total_capacity_mw", T.DoubleType(), True),
        T.StructField("total_capacity_registered_mw", T.DoubleType(), True),
        T.StructField("total_capacity_maximum_mw", T.DoubleType(), True),
        T.StructField("total_capacity_storage_mwh", T.DoubleType(), True),
        T.StructField("unit_fueltech_summary", T.StringType(), True),
        T.StructField("unit_status_summary", T.StringType(), True),
        T.StructField("unit_dispatch_summary", T.StringType(), True),
        T.StructField("unit_codes", T.StringType(), True),
        T.StructField("facility_description", T.StringType(), True),
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# API Response Models
# ─────────────────────────────────────────────────────────────────────────────

class Location(BaseModel):
    """Geographic location of a facility."""

    lat: Optional[float] = None
    lng: Optional[float] = None


class Unit(BaseModel):
    """Unit within a facility (generator, battery, etc.)."""

    code: Optional[str] = None
    name: Optional[str] = None
    status_id: Optional[str] = Field(None, alias="status")
    fueltech_id: Optional[str] = Field(None, alias="fueltech")
    dispatch_type: Optional[str] = None
    capacity_mw: Optional[float] = Field(None, alias="capacity")
    capacity_registered: Optional[float] = None
    capacity_maximum: Optional[float] = None
    capacity_storage: Optional[float] = None

    model_config = {"populate_by_name": True, "extra": "allow"}

    @field_validator("capacity_mw", "capacity_registered", "capacity_maximum", "capacity_storage", mode="before")
    @classmethod
    def coerce_float(cls, v: Any) -> Optional[float]:
        """Coerce numeric strings to float."""
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


class Facility(BaseModel):
    """Facility metadata from API response."""

    code: Optional[str] = None
    name: Optional[str] = None
    network_id: Optional[str] = None
    network_region: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    description: Optional[str] = None
    location: Optional[Location] = None
    units: List[Unit] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class FacilityMetadata(BaseModel):
    """Minimal facility metadata used for timeseries lookups."""

    code: str
    name: Optional[str] = None
    network_id: str
    network_region: Optional[str] = None
    units: List[Dict[str, Any]] = Field(default_factory=list)


class FacilitiesResponse(BaseModel):
    """Response from /facilities endpoint."""

    success: bool = False
    data: List[Facility] = Field(default_factory=list)
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Timeseries Models
# ─────────────────────────────────────────────────────────────────────────────

class TimeseriesColumns(BaseModel):
    """Column identifiers for a timeseries result."""

    unit_code: Optional[str] = None
    facility_code: Optional[str] = None


class TimeseriesResult(BaseModel):
    """Single timeseries result (one unit/facility)."""

    columns: Optional[TimeseriesColumns] = None
    data: List[List[Any]] = Field(default_factory=list)  # [[timestamp, value], ...]


class TimeseriesSeries(BaseModel):
    """A series of timeseries data for a specific metric."""

    network_code: Optional[str] = None
    metric: Optional[str] = None
    interval: Optional[str] = None
    unit: Optional[str] = None  # measurement unit (e.g., "MWh")
    results: List[TimeseriesResult] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class TimeseriesResponse(BaseModel):
    """Response from /data/facilities/{network} endpoint."""

    success: bool = False
    data: List[TimeseriesSeries] = Field(default_factory=list)
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Flattened Row Models (for DataFrame output)
# ─────────────────────────────────────────────────────────────────────────────

class FacilitySummary(BaseModel):
    """Flattened facility summary for DataFrame output."""

    facility_code: Optional[str] = None
    facility_name: Optional[str] = None
    network_id: Optional[str] = None
    network_region: Optional[str] = None
    facility_created_at: Optional[str] = None
    facility_updated_at: Optional[str] = None
    location_lat: Optional[float] = None
    location_lng: Optional[float] = None
    unit_count: int = 0
    total_capacity_mw: Optional[float] = None
    total_capacity_registered_mw: Optional[float] = None
    total_capacity_maximum_mw: Optional[float] = None
    total_capacity_storage_mwh: Optional[float] = None
    unit_fueltech_summary: Optional[str] = None
    unit_status_summary: Optional[str] = None
    unit_dispatch_summary: Optional[str] = None
    unit_codes: Optional[str] = None
    facility_description: Optional[str] = None


class TimeseriesRow(BaseModel):
    """Flattened timeseries row for DataFrame output."""

    network_code: Optional[str] = None
    network_id: Optional[str] = None
    network_region: Optional[str] = None
    facility_code: Optional[str] = None
    facility_name: Optional[str] = None
    unit_code: Optional[str] = None
    metric: Optional[str] = None
    interval: Optional[str] = None
    value_unit: Optional[str] = None
    interval_start: Optional[str] = None
    value: Optional[float] = None


# ─────────────────────────────────────────────────────────────────────────────
# Configuration Models
# ─────────────────────────────────────────────────────────────────────────────

class ClientConfig(BaseModel):
    """Configuration for OpenElectricityClient."""

    base_url: str = "https://api.openelectricity.org.au/v4"
    timeout: int = 120
    max_retries: int = 3
    retry_min_wait: float = 1.0  # seconds
    retry_max_wait: float = 60.0  # seconds


class DateRange(BaseModel):
    """Date range for API queries."""

    start: datetime
    end: datetime

    @field_validator("end")
    @classmethod
    def end_after_start(cls, v: datetime, info) -> datetime:
        """Validate that end is after start."""
        if "start" in info.data and v <= info.data["start"]:
            raise ValueError("end must be after start")
        return v


__all__ = [
    # Spark schema
    "FACILITY_SCHEMA",
    # API response models
    "Location",
    "Unit",
    "Facility",
    "FacilityMetadata",
    "FacilitiesResponse",
    "TimeseriesColumns",
    "TimeseriesResult",
    "TimeseriesSeries",
    "TimeseriesResponse",
    # Output models
    "FacilitySummary",
    "TimeseriesRow",
    # Config models
    "ClientConfig",
    "DateRange",
]
