from __future__ import annotations
import datetime as dt
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from .models import (
    Facility,
    FacilityMetadata,
    FacilitySummary,
    TimeseriesRow,
)

NETWORK_TIMEZONE_IDS = {"NEM": "Australia/Brisbane", "WEM": "Australia/Perth"}
NETWORK_FALLBACK_OFFSETS = {"NEM": 10, "WEM": 8}
MAX_RANGE_ERROR_PATTERN = re.compile(r"Maximum range is (\d+) days", re.IGNORECASE)

#trả về giờ offset từ UTC của network
def get_timezone_offset_hours(network_id: str) -> int:
    return NETWORK_FALLBACK_OFFSETS.get(network_id, 10)
#trả về IANA timezone ID của network
def get_timezone_id(network_id: str) -> str:
    return NETWORK_TIMEZONE_IDS.get(network_id, "Australia/Brisbane")
#trả về timezone object của network
def resolve_network_timezone(network_code: str):
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None
    
    tz_name = NETWORK_TIMEZONE_IDS.get(network_code) #lấy tên timezone của network
    if ZoneInfo and tz_name:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            pass

    from datetime import timezone, timedelta
    offset_hours = NETWORK_FALLBACK_OFFSETS.get(network_code)
    if offset_hours is not None:
        return timezone(timedelta(hours=offset_hours)) #ví dụ: timezone(timedelta(hours=10)) trả về UTC +10
    return timezone.utc

#parse datetime string thành datetime object
def parse_naive_datetime(value: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid datetime '{value}'. Expected YYYY-MM-DDTHH:MM:SS") from exc
    if parsed.tzinfo is not None:
        raise ValueError("Datetime values must be timezone naive (network local time).")
    return parsed
#format datetime object thành datetime string
def format_naive_datetime(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S")

def chunk_date_range(
    start: dt.datetime,
    end: dt.datetime,
    max_days: Optional[int]
) -> List[Tuple[dt.datetime, dt.datetime]]:
    if not max_days or max_days <= 0:
        return [(start, end)]
    
    #list chứa các tuple (start, end)
    segments: List[Tuple[dt.datetime, dt.datetime]] = []
    delta = dt.timedelta(days=max_days) 
    current = start
    
    while current < end:
        next_dt = min(current + delta, end) # không vượt quá end
        segments.append((current, next_dt)) #thêm tuple (current, next_dt) vào list segments
        current = next_dt
    
    return segments

def extract_max_days_from_error(error_text: str) -> Optional[int]:
    match = MAX_RANGE_ERROR_PATTERN.search(error_text or "")
    if match:
        try:
            return int(match.group(1)) #group(1) trả về số lượng ngày tối đa
        except ValueError:
            return None
    return None

# đọc danh sách mã trạm từ facilities.js 
def load_default_facility_codes(override_path: Optional[Path] = None) -> List[str]:
    js_path = override_path or Path(__file__).resolve().parent / "../../bronze/facilities.js"
    js_path = js_path.resolve()
    
    if not js_path.exists():
        raise FileNotFoundError(f"Missing facilities.js at {js_path}")

    contents = js_path.read_text(encoding="utf-8")
    matches = re.findall(r'"([^"\n]+)"', contents)
    
    if not matches:
        raise ValueError("Unable to parse facility codes from facilities.js")

    return [code.upper() for code in matches if code.strip()]

# chuyển đổi giá trị sang float, nếu lỗi trả về 0.0
def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

# chuyển đổi giá trị sang string, nếu lỗi trả về None
def as_str(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    return str(value)

#facility parser
def summarize_facility(facility: Dict[str, Any]) -> FacilitySummary:
    units = facility.get("units") or []
    total_capacity = 0.0
    total_registered = 0.0
    total_maximum = 0.0
    total_storage = 0.0
    unit_codes: List[str] = []
    status_counts: Dict[str, int] = {}
    fueltech_counts: Dict[str, int] = {}
    dispatch_counts: Dict[str, int] = {}

    for unit in units:
        code = unit.get("code")
        if code:
            unit_codes.append(code)
        
        for capacity_key in ("capacity_mw", "capacity", "capacity_registered", "registered_capacity"):
            capacity_value = unit.get(capacity_key)
            if capacity_value is not None:
                value = safe_float(capacity_value)
                if value:
                    total_capacity += value
                    break

        total_registered += safe_float(
            unit.get("capacity_registered") or unit.get("registered_capacity")
        )
        total_maximum += safe_float(unit.get("capacity_maximum"))
        total_storage += safe_float(unit.get("capacity_storage"))

        status = unit.get("status_id") or unit.get("status")
        if isinstance(status, str) and status:
            key = status.lower()
            status_counts[key] = status_counts.get(key, 0) + 1

        fueltech = unit.get("fueltech_id") or unit.get("fueltech")
        if isinstance(fueltech, str) and fueltech:
            key = fueltech.lower()
            fueltech_counts[key] = fueltech_counts.get(key, 0) + 1

        dispatch = unit.get("dispatch_type")
        if isinstance(dispatch, str) and dispatch:
            key = dispatch.upper()
            dispatch_counts[key] = dispatch_counts.get(key, 0) + 1

    location = facility.get("location") or {}

    return FacilitySummary(
        facility_code=as_str(facility.get("code")),
        facility_name=as_str(facility.get("name")),
        network_id=as_str(facility.get("network_id")),
        network_region=as_str(facility.get("network_region")),
        facility_created_at=as_str(facility.get("created_at")),
        facility_updated_at=as_str(facility.get("updated_at")),
        location_lat=location.get("lat"),
        location_lng=location.get("lng"),
        unit_count=len(units),
        total_capacity_mw=round(total_capacity, 3) if total_capacity else None,
        total_capacity_registered_mw=round(total_registered, 3) if total_registered else None,
        total_capacity_maximum_mw=round(total_maximum, 3) if total_maximum else None,
        total_capacity_storage_mwh=round(total_storage, 3) if total_storage else None,
        unit_fueltech_summary="; ".join(
            f"{key}:{count}" for key, count in sorted(fueltech_counts.items())
        ) or None,
        unit_status_summary="; ".join(
            f"{key}:{count}" for key, count in sorted(status_counts.items())
        ) or None,
        unit_dispatch_summary="; ".join(
            f"{key}:{count}" for key, count in sorted(dispatch_counts.items())
        ) or None,
        unit_codes=",".join(unit_codes) if unit_codes else None,
        facility_description=as_str(facility.get("description")),
    )

def parse_facility_metadata(facility: Dict[str, Any]) -> Optional[FacilityMetadata]:
    code = facility.get("code")
    if not code:
        return None
    
    return FacilityMetadata(
        code=code,
        name=facility.get("name"),
        network_id=facility.get("network_id") or "",
        network_region=facility.get("network_region"),
        units=facility.get("units") or [],
    )

def build_unit_to_facility_map(facilities: Dict[str, FacilityMetadata]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for facility in facilities.values():
        for unit in facility.units:
            unit_code = unit.get("code")
            if unit_code and unit_code not in mapping: 
                mapping[unit_code] = facility.code
    return mapping

#timeseries parser
def flatten_timeseries(
    payload: Dict[str, Any],
    facilities: Dict[str, FacilityMetadata],
    unit_to_facility: Dict[str, str],
) -> List[TimeseriesRow]:
    rows: List[TimeseriesRow] = []
    #lấy data từ payload
    for series in payload.get("data", []):
        network_code = series.get("network_code")
        metric = series.get("metric")
        interval = series.get("interval")
        value_unit = series.get("unit")
        
        for result in series.get("results", []):
            columns = result.get("columns", {})
            unit_code = columns.get("unit_code")
            facility_code = columns.get("facility_code") or unit_to_facility.get(unit_code)
            facility_meta = facilities.get(facility_code) if facility_code else None
            facility_name = facility_meta.name if facility_meta else None
            network_id = facility_meta.network_id if facility_meta else None
            network_region = facility_meta.network_region if facility_meta else None
            
            for timestamp, value in result.get("data", []):
                rows.append(
                    TimeseriesRow(
                        network_code=network_code,
                        network_id=network_id,
                        network_region=network_region,
                        facility_code=facility_code,
                        facility_name=facility_name,
                        unit_code=unit_code,
                        metric=metric,
                        interval=interval,
                        value_unit=value_unit,
                        interval_start=timestamp,
                        value=value,
                    )
                )
    
    return rows

__all__ = [
    # Timezone helpers
    "get_timezone_offset_hours",
    "get_timezone_id",
    "resolve_network_timezone",
    # Date/time helpers
    "parse_naive_datetime",
    "format_naive_datetime",
    "chunk_date_range",
    "extract_max_days_from_error",
    # Facility code loader
    "load_default_facility_codes",
    # Value helpers
    "safe_float",
    "as_str",
    # Facility parsing
    "summarize_facility",
    "parse_facility_metadata",
    "build_unit_to_facility_map",
    # Timeseries parsing
    "flatten_timeseries",
    # Constants
    "NETWORK_TIMEZONE_IDS",
    "NETWORK_FALLBACK_OFFSETS",
]
