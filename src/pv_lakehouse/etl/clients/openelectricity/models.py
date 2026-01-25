from __future__ import annotations  # Allows using nested type annotations (e.g., List[Unit]) within class definitions
from datetime import datetime
from typing import Any, Dict, List, Optional  # Type annotations for variables and function parameters
from pydantic import BaseModel, Field, field_validator  # Data validation and model creation with automatic type checking
from pyspark.sql import types as T  # Schema definition for DataFrame

# Spark schema
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

class Location(BaseModel):
    """Geographic location coordinates.

    Attributes:
        lat: Latitude in decimal degrees.
        lng: Longitude in decimal degrees.
    """

    lat: Optional[float] = None
    lng: Optional[float] = None

class Unit(BaseModel):
    """Electricity generation unit within a facility.

    Attributes:
        code: Unique unit identifier.
        name: Human-readable unit name.
        status_id: Operating status (e.g., 'operating', 'retired').
        fueltech_id: Fuel technology type (e.g., 'solar_utility').
        dispatch_type: Dispatch category (e.g., 'GENERATOR', 'LOAD').
        capacity_mw: Nameplate capacity in megawatts.
        capacity_registered: Registered capacity in MW.
        capacity_maximum: Maximum output capacity in MW.
        capacity_storage: Energy storage capacity in MWh.
    """

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
        """Convert capacity values to float, handling None and empty strings."""
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

class Facility(BaseModel):
    """Power generation facility containing multiple units.

    Attributes:
        code: Unique facility identifier.
        name: Human-readable facility name.
        network_id: Network identifier (e.g., 'NEM', 'WEM').
        network_region: Network region (e.g., 'NSW1', 'VIC1').
        created_at: ISO timestamp when facility was created.
        updated_at: ISO timestamp when facility was last updated.
        description: Optional facility description.
        location: Geographic coordinates.
        units: List of generation units within this facility.
    """

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
    """Lightweight facility metadata for API lookups.

    Attributes:
        code: Unique facility identifier (required).
        name: Human-readable facility name.
        network_id: Network identifier (required).
        network_region: Network region.
        units: Raw unit data as list of dictionaries.
    """

    code: str
    name: Optional[str] = None
    network_id: str
    network_region: Optional[str] = None
    units: List[Dict[str, Any]] = Field(default_factory=list)


class FacilitiesResponse(BaseModel):
    """API response wrapper for facilities list.

    Attributes:
        success: Whether the API request succeeded.
        data: List of facility objects.
        error: Error message if request failed.
    """

    success: bool = False
    data: List[Facility] = Field(default_factory=list)
    error: Optional[str] = None

class TimeseriesColumns(BaseModel):
    """Column metadata for timeseries data points.

    Attributes:
        unit_code: The unit code for this data series.
        facility_code: The facility code for this data series.
    """

    unit_code: Optional[str] = None
    facility_code: Optional[str] = None


class TimeseriesResult(BaseModel):
    """Single result set within a timeseries series.

    Attributes:
        columns: Metadata identifying the data source.
        data: List of [timestamp, value] pairs.
    """

    columns: Optional[TimeseriesColumns] = None
    data: List[List[Any]] = Field(default_factory=list)


class TimeseriesSeries(BaseModel):
    """Timeseries data series for a specific metric.

    Attributes:
        network_code: Network identifier.
        metric: Metric name (e.g., 'power', 'energy').
        interval: Time interval (e.g., '1h', '5m').
        unit: Unit of measurement (e.g., 'MW', 'MWh').
        results: List of result sets.
    """

    network_code: Optional[str] = None
    metric: Optional[str] = None
    interval: Optional[str] = None
    unit: Optional[str] = None
    results: List[TimeseriesResult] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class TimeseriesResponse(BaseModel):
    """API response wrapper for timeseries data.

    Attributes:
        success: Whether the API request succeeded.
        data: List of timeseries data series.
        error: Error message if request failed.
    """

    success: bool = False
    data: List[TimeseriesSeries] = Field(default_factory=list)
    error: Optional[str] = None

class FacilitySummary(BaseModel):
    """Flattened facility summary for DataFrame output.

    Contains aggregated data from a facility and its units,
    suitable for tabular representation.

    Attributes:
        facility_code: Unique facility identifier.
        facility_name: Human-readable facility name.
        network_id: Network identifier.
        network_region: Network region.
        facility_created_at: Creation timestamp.
        facility_updated_at: Last update timestamp.
        location_lat: Latitude coordinate.
        location_lng: Longitude coordinate.
        unit_count: Number of units in facility.
        total_capacity_mw: Sum of unit capacities in MW.
        total_capacity_registered_mw: Sum of registered capacities.
        total_capacity_maximum_mw: Sum of maximum capacities.
        total_capacity_storage_mwh: Sum of storage capacities.
        unit_fueltech_summary: Fuel technology breakdown.
        unit_status_summary: Status breakdown.
        unit_dispatch_summary: Dispatch type breakdown.
        unit_codes: Comma-separated list of unit codes.
        facility_description: Facility description.
    """

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
    """Flattened timeseries data row for DataFrame output.

    Attributes:
        network_code: Network code from API.
        network_id: Network identifier.
        network_region: Network region.
        facility_code: Facility identifier.
        facility_name: Facility name.
        unit_code: Unit identifier.
        metric: Metric name (e.g., 'power').
        interval: Time interval.
        value_unit: Unit of measurement.
        interval_start: Timestamp for this data point.
        value: Measured value.
    """

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


class ClientConfig(BaseModel):
    """Configuration settings for the OpenElectricity client.

    Attributes:
        base_url: API base URL.
        timeout: Request timeout in seconds.
        max_retries: Maximum retry attempts for failed requests.
        retry_min_wait: Minimum wait between retries in seconds.
        retry_max_wait: Maximum wait between retries in seconds.
    """

    base_url: str = "https://api.openelectricity.org.au/v4"
    timeout: int = 120
    max_retries: int = 3
    retry_min_wait: float = 1.0
    retry_max_wait: float = 60.0


class DateRange(BaseModel):
    """Date range with validation.

    Attributes:
        start: Start datetime.
        end: End datetime (must be after start).
    """

    start: datetime
    end: datetime

    @field_validator("end")
    @classmethod
    def end_after_start(cls, v: datetime, info) -> datetime:
        """Validate that end datetime is after start datetime."""
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