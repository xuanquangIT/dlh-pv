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

# API response models
# Data model for geographic location
class Location(BaseModel):
    lat: Optional[float] = None  # Latitude
    lng: Optional[float] = None  # Longitude

# Data model for electricity unit
class Unit(BaseModel):
    code: Optional[str] = None  # Unit code
    name: Optional[str] = None  # Unit name
    status_id: Optional[str] = Field(None, alias="status")  # Status
    fueltech_id: Optional[str] = Field(None, alias="fueltech")  # Fuel type
    dispatch_type: Optional[str] = None  # Dispatch type
    capacity_mw: Optional[float] = Field(None, alias="capacity")  # Capacity
    capacity_registered: Optional[float] = None  # Registered capacity
    capacity_maximum: Optional[float] = None  # Maximum capacity
    capacity_storage: Optional[float] = None  # Storage capacity

    model_config = {"populate_by_name": True, "extra": "allow"} 

    @field_validator("capacity_mw", "capacity_registered", "capacity_maximum", "capacity_storage", mode="before")
    @classmethod  # Convert value to float
    def coerce_float(cls, v: Any) -> Optional[float]:
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

# Data model for facility
class Facility(BaseModel):
    code: Optional[str] = None  # Facility code
    name: Optional[str] = None  # Facility name
    network_id: Optional[str] = None  # Network ID
    network_region: Optional[str] = None  # Network region
    created_at: Optional[str] = None  # Creation date
    updated_at: Optional[str] = None  # Update date
    description: Optional[str] = None  # Description
    location: Optional[Location] = None  # Location
    units: List[Unit] = Field(default_factory=list)  # List of units

    model_config = {"extra": "allow"}

# Data model for facility metadata
class FacilityMetadata(BaseModel):
    code: str  # Facility code
    name: Optional[str] = None  # Facility name
    network_id: str  # Network ID
    network_region: Optional[str] = None  # Network region
    units: List[Dict[str, Any]] = Field(default_factory=list)  # List of units

# Data model for facilities response
class FacilitiesResponse(BaseModel):
    success: bool = False  # Success status
    data: List[Facility] = Field(default_factory=list)  # List of facilities
    error: Optional[str] = None  # Error message

# Data model for timeseries response
class TimeseriesColumns(BaseModel):
    unit_code: Optional[str] = None  # Unit code
    facility_code: Optional[str] = None  # Facility code

class TimeseriesResult(BaseModel):
    columns: Optional[TimeseriesColumns] = None  # Columns
    data: List[List[Any]] = Field(default_factory=list) 
class TimeseriesSeries(BaseModel):
    network_code: Optional[str] = None  # Network code
    metric: Optional[str] = None  # Metric name
    interval: Optional[str] = None  # Time interval
    unit: Optional[str] = None  # Unit of measurement
    results: List[TimeseriesResult] = Field(default_factory=list) 

    model_config = {"extra": "allow"}

class TimeseriesResponse(BaseModel):
    success: bool = False  # Success status
    data: List[TimeseriesSeries] = Field(default_factory=list) 
    error: Optional[str] = None  # Error message

# Facility summary model
class FacilitySummary(BaseModel):
    facility_code: Optional[str] = None  # Facility code
    facility_name: Optional[str] = None  # Facility name
    network_id: Optional[str] = None  # Network ID
    network_region: Optional[str] = None  # Network region
    facility_created_at: Optional[str] = None  # Creation date
    facility_updated_at: Optional[str] = None  # Update date
    location_lat: Optional[float] = None  # Latitude
    location_lng: Optional[float] = None  # Longitude
    unit_count: int = 0  # Number of units
    total_capacity_mw: Optional[float] = None  # Total capacity
    total_capacity_registered_mw: Optional[float] = None  # Total registered capacity
    total_capacity_maximum_mw: Optional[float] = None  # Total maximum capacity
    total_capacity_storage_mwh: Optional[float] = None  # Total storage capacity
    unit_fueltech_summary: Optional[str] = None  # Fuel technology summary
    unit_status_summary: Optional[str] = None  # Status summary
    unit_dispatch_summary: Optional[str] = None  # Dispatch type summary
    unit_codes: Optional[str] = None  # Unit codes
    facility_description: Optional[str] = None  # Description

class TimeseriesRow(BaseModel):
    network_code: Optional[str] = None  # Network code
    network_id: Optional[str] = None  # Network ID
    network_region: Optional[str] = None  # Network region
    facility_code: Optional[str] = None  # Facility code
    facility_name: Optional[str] = None  # Facility name
    unit_code: Optional[str] = None  # Unit code
    metric: Optional[str] = None  # Metric name
    interval: Optional[str] = None  # Time interval
    value_unit: Optional[str] = None  # Unit of measurement
    interval_start: Optional[str] = None  # Interval start time
    value: Optional[float] = None  # Value

class ClientConfig(BaseModel):
    base_url: str = "https://api.openelectricity.org.au/v4"
    timeout: int = 120
    max_retries: int = 3
    retry_min_wait: float = 1.0  # seconds
    retry_max_wait: float = 60.0  # seconds

class DateRange(BaseModel):
    start: datetime
    end: datetime

    @field_validator("end")
    @classmethod  # Validate that end must be after start
    def end_after_start(cls, v: datetime, info) -> datetime:
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