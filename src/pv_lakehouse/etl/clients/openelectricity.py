"""Client helpers for fetching OpenElectricity datasets into pandas DataFrames."""

from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import types as T
import requests

DEFAULT_BASE_URL = os.environ.get("OPENELECTRICITY_API_URL", "https://api.openelectricity.org.au/v4")
FACILITIES_ENDPOINT = f"{DEFAULT_BASE_URL.rstrip('/')}/facilities/"
DATA_ENDPOINT_TEMPLATE = f"{DEFAULT_BASE_URL.rstrip('/')}/data/facilities/{{network_code}}"

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

CANDIDATE_ENV_KEYS = [
    "OPENELECTRICITY_API_KEY",
    "OPEN_ELECTRICITY_API_KEY",
    "OPEN_NEM_PRIMARY",
    "OPEN_NEM_SECONDARY",
]

SUPPORTED_INTERVALS = {"5m", "1h", "1d", "7d", "1M", "3M", "season", "1y", "fy"}
INTERVAL_MAX_DAYS: Dict[str, int] = {"1h": 30}
TARGET_WINDOW_DAYS = 7
MAX_LOOKBACK_WINDOWS = 52
NETWORK_TIMEZONE_IDS = {"NEM": "Australia/Brisbane", "WEM": "Australia/Perth"}
NETWORK_FALLBACK_OFFSETS = {"NEM": 10, "WEM": 8}
MAX_RANGE_ERROR_PATTERN = re.compile(r"Maximum range is (\d+) days", re.IGNORECASE)


def load_default_facility_codes(override_path: Optional[Path] = None) -> List[str]:
    """Return the canonical list of facility codes shared with JS tooling."""

    js_path = override_path or Path(__file__).resolve().parent / "../bronze/facilities.js"
    js_path = js_path.resolve()
    if not js_path.exists():
        raise FileNotFoundError(f"Missing facilities.js at {js_path}")

    contents = js_path.read_text(encoding="utf-8")
    matches = re.findall(r'"([^"\n]+)"', contents)
    if not matches:
        raise ValueError("Unable to parse facility codes from facilities.js")

    return [code.upper() for code in matches if code.strip()]


@dataclass
class FacilityMetadata:
    """Minimal facility metadata used for timeseries lookups."""

    code: str
    name: Optional[str]
    network_id: str
    network_region: Optional[str]
    units: List[Dict[str, Any]]


def _read_env_file(env_path: Path) -> Dict[str, str]:
    if not env_path.exists():
        return {}
    values: Dict[str, str] = {}
    text = env_path.read_text(encoding="utf-8-sig")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_api_key(cli_key: Optional[str] = None) -> str:
    """Resolve an OpenElectricity API key from CLI, environment variables, or .env."""

    if cli_key:
        return cli_key

    for key in CANDIDATE_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            if key != "OPENELECTRICITY_API_KEY":
                os.environ.setdefault("OPENELECTRICITY_API_KEY", value)
            return value

    env_values = _read_env_file(Path.cwd() / ".env")
    for key in CANDIDATE_ENV_KEYS:
        value = env_values.get(key)
        if value:
            os.environ.setdefault(key, value)
            if key != "OPENELECTRICITY_API_KEY":
                os.environ.setdefault("OPENELECTRICITY_API_KEY", value)
            return value

    raise RuntimeError(
        "Missing API key. Provide via parameter, environment variable, or .env file."
    )


def _build_params(
    networks: Iterable[str],
    statuses: Iterable[str],
    fueltechs: Iterable[str],
    region: Optional[str],
) -> List[Tuple[str, str]]:
    params: List[Tuple[str, str]] = []
    params.extend(("network_id", value) for value in networks)
    params.extend(("status_id", value) for value in statuses)
    params.extend(("fueltech_id", value) for value in fueltechs)
    if region:
        params.append(("network_region", region))
    return params


def _request_json(url: str, headers: Dict[str, str], params: List[Tuple[str, str]]) -> Dict[str, Any]:
    response = requests.get(url, headers=headers, params=params, timeout=120)
    if response.status_code == 401:
        raise RuntimeError("401 Unauthorized: check your OpenElectricity credentials.")
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success", False):
        raise RuntimeError(payload.get("error") or "OpenElectricity API returned error")
    return payload


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _summarize_facility(facility: Dict[str, Any]) -> Dict[str, Any]:
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

        for capacity_key in (
            "capacity_mw",
            "capacity",
            "capacity_registered",
            "registered_capacity",
        ):
            capacity_value = unit.get(capacity_key)
            if capacity_value is not None:
                value = _safe_float(capacity_value)
                if value:
                    total_capacity += value
                    break

        total_registered += _safe_float(
            unit.get("capacity_registered") or unit.get("registered_capacity")
        )
        total_maximum += _safe_float(unit.get("capacity_maximum"))
        total_storage += _safe_float(unit.get("capacity_storage"))

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

    def _as_str(value: Any) -> Optional[str]:
        if value is None or value == "":
            return None
        return str(value)

    return {
        "facility_code": _as_str(facility.get("code")),
        "facility_name": _as_str(facility.get("name")),
        "network_id": _as_str(facility.get("network_id")),
        "network_region": _as_str(facility.get("network_region")),
        "facility_created_at": _as_str(facility.get("created_at")),
        "facility_updated_at": _as_str(facility.get("updated_at")),
        "location_lat": location.get("lat"),
        "location_lng": location.get("lng"),
        "unit_count": len(units),
        "total_capacity_mw": round(total_capacity, 3) if total_capacity else None,
        "total_capacity_registered_mw": round(total_registered, 3) if total_registered else None,
        "total_capacity_maximum_mw": round(total_maximum, 3) if total_maximum else None,
        "total_capacity_storage_mwh": round(total_storage, 3) if total_storage else None,
        "unit_fueltech_summary": "; ".join(
            f"{key}:{count}" for key, count in sorted(fueltech_counts.items())
        ),
        "unit_status_summary": "; ".join(
            f"{key}:{count}" for key, count in sorted(status_counts.items())
        ),
        "unit_dispatch_summary": "; ".join(
            f"{key}:{count}" for key, count in sorted(dispatch_counts.items())
        ),
        "unit_codes": ",".join(unit_codes) if unit_codes else None,
        "facility_description": _as_str(facility.get("description")),
    }


def fetch_facilities_dataframe(
    *,
    api_key: Optional[str] = None,
    selected_codes: Optional[Sequence[str]] = None,
    networks: Optional[Sequence[str]] = None,
    statuses: Optional[Sequence[str]] = None,
    fueltechs: Optional[Sequence[str]] = None,
    region: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch facility metadata and return as pandas DataFrame."""

    api_token = load_api_key(api_key)
    networks = [value.upper() for value in (networks or ["NEM", "WEM"]) if value]
    statuses = [value.lower() for value in (statuses or ["operating"]) if value]
    fueltechs = [value.lower() for value in (fueltechs or ["solar_utility"]) if value]
    region_filter = region.strip() if isinstance(region, str) else None
    if region_filter and region_filter.upper() == "ALL":
        region_filter = None

    params = _build_params(networks, statuses, fueltechs, region_filter)
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
    }
    payload = _request_json(FACILITIES_ENDPOINT, headers, params)
    rows = [_summarize_facility(item) for item in payload.get("data", [])]
    dataframe = pd.DataFrame(rows)

    if dataframe.empty:
        return dataframe

    dataframe = dataframe.sort_values(
        ["network_id", "network_region", "facility_name"], na_position="last"
    ).reset_index(drop=True)

    if selected_codes:
        desired = {code.upper() for code in selected_codes if code}
        if desired:
            dataframe["facility_code"] = dataframe["facility_code"].astype(str)
            dataframe = dataframe[
                dataframe["facility_code"].str.upper().isin(desired)
            ].reset_index(drop=True)

    return dataframe


def _fetch_facility_metadata(api_key: str, facility_codes: Sequence[str]) -> Dict[str, FacilityMetadata]:
    if not facility_codes:
        raise ValueError("facility_codes must not be empty")

    headers = {"Authorization": f"Bearer {api_key}"}
    params = [("facility_code", code) for code in facility_codes]
    payload = _request_json(FACILITIES_ENDPOINT, headers, params)

    facilities: Dict[str, FacilityMetadata] = {}
    for facility in payload.get("data", []):
        code = facility.get("code")
        if not code:
            continue
        facilities[code] = FacilityMetadata(
            code=code,
            name=facility.get("name"),
            network_id=facility.get("network_id"),
            network_region=facility.get("network_region"),
            units=facility.get("units") or [],
        )

    missing = [code for code in facility_codes if code not in facilities]
    if missing:
        raise ValueError(f"Unknown facility codes: {', '.join(missing)}")

    return facilities


def _build_unit_to_facility_map(facilities: Dict[str, FacilityMetadata]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for facility in facilities.values():
        for unit in facility.units:
            unit_code = unit.get("code")
            if unit_code and unit_code not in mapping:
                mapping[unit_code] = facility.code
    return mapping


def _resolve_network_timezone(network_code: str):
    try:
        from zoneinfo import ZoneInfo  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - Python < 3.9 fallback
        ZoneInfo = None  # type: ignore[assignment]

    tz_name = NETWORK_TIMEZONE_IDS.get(network_code)
    if ZoneInfo and tz_name:
        try:
            return ZoneInfo(tz_name)
        except Exception:  # pragma: no cover - best effort fallback
            pass

    from datetime import timezone, timedelta

    offset_hours = NETWORK_FALLBACK_OFFSETS.get(network_code)
    if offset_hours is not None:
        return timezone(timedelta(hours=offset_hours))
    return timezone.utc


def _default_time_window(network_code: str, interval: str) -> Tuple[dt.datetime, dt.datetime]:
    tzinfo = _resolve_network_timezone(network_code)
    now = dt.datetime.now(tz=tzinfo)
    if interval == "5m":
        start = now - dt.timedelta(hours=2)
    else:
        start = now - dt.timedelta(days=TARGET_WINDOW_DAYS)
    return start.replace(tzinfo=None), now.replace(tzinfo=None)


def _parse_naive_datetime(value: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - invalid inputs
        raise ValueError(f"Invalid datetime '{value}'. Expected YYYY-MM-DDTHH:MM:SS") from exc
    if parsed.tzinfo is not None:
        raise ValueError("Datetime values must be timezone naive (network local time).")
    return parsed


def _format_naive_datetime(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def _chunk_date_range(
    start: dt.datetime, end: dt.datetime, max_days: Optional[int]
) -> List[Tuple[dt.datetime, dt.datetime]]:
    if not max_days or max_days <= 0:
        return [(start, end)]
    segments: List[Tuple[dt.datetime, dt.datetime]] = []
    delta = dt.timedelta(days=max_days)
    current = start
    while current < end:
        next_dt = min(current + delta, end)
        segments.append((current, next_dt))
        current = next_dt
    return segments


def _extract_max_days_from_error(error_text: str) -> Optional[int]:
    match = MAX_RANGE_ERROR_PATTERN.search(error_text or "")
    if match:
        try:
            return int(match.group(1))
        except ValueError:  # pragma: no cover - defensive
            return None
    return None


def _fetch_timeseries_payloads(
    api_key: str,
    network_code: str,
    facility_codes: Sequence[str],
    metrics: Sequence[str],
    interval: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {api_key}"}
    params_base: List[Tuple[str, str]] = [("interval", interval)]
    params_base.extend(("metrics", metric) for metric in metrics)
    params_base.extend(("facility_code", code) for code in facility_codes)

    url = DATA_ENDPOINT_TEMPLATE.format(network_code=network_code)
    max_chunk_days = INTERVAL_MAX_DAYS.get(interval)

    while True:
        payloads: List[Dict[str, Any]] = []
        segments = _chunk_date_range(start_dt, end_dt, max_chunk_days)

        try:
            for chunk_start, chunk_end in segments:
                params = list(params_base)
                params.append(("date_start", _format_naive_datetime(chunk_start)))
                params.append(("date_end", _format_naive_datetime(chunk_end)))
                payload = _request_json(url, headers, params)
                payloads.append(payload)
            return payloads
        except requests.HTTPError as exc:  # pragma: no cover - depends on API behaviour
            response_text = ""
            if exc.response is not None:
                try:
                    response_text = exc.response.text
                except Exception:
                    response_text = ""
            max_days = _extract_max_days_from_error(response_text)
            if not max_days:
                raise
            if max_chunk_days and max_chunk_days <= max_days:
                if max_days <= 1:
                    raise
                max_chunk_days = max_days - 1
            else:
                max_chunk_days = max_days


def _flatten_timeseries(
    payload: Dict[str, Any],
    facilities: Dict[str, FacilityMetadata],
    unit_to_facility: Dict[str, str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
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
                    {
                        "network_code": network_code,
                        "network_id": network_id,
                        "network_region": network_region,
                        "facility_code": facility_code,
                        "facility_name": facility_name,
                        "unit_code": unit_code,
                        "metric": metric,
                        "interval": interval,
                        "value_unit": value_unit,
                        "interval_start": timestamp,
                        "value": value,
                    }
                )
    return rows


def fetch_facility_timeseries_dataframe(
    *,
    facility_codes: Sequence[str],
    metrics: Optional[Sequence[str]] = None,
    interval: str = "1h",
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    api_key: Optional[str] = None,
    target_window_days: int = TARGET_WINDOW_DAYS,
    max_lookback_windows: int = MAX_LOOKBACK_WINDOWS,
) -> pd.DataFrame:
    """Fetch facility production timeseries as pandas DataFrame."""

    if interval not in SUPPORTED_INTERVALS:
        raise ValueError(
            f"Invalid interval '{interval}'. Supported values: {', '.join(sorted(SUPPORTED_INTERVALS))}."
        )

    metrics_list = [metric.lower() for metric in (metrics or []) if metric]
    if not metrics_list:
        metrics_list = ["power", "energy", "market_value", "price"]

    facility_codes = [code.upper() for code in facility_codes]
    api_token = load_api_key(api_key)
    facilities = _fetch_facility_metadata(api_token, facility_codes)
    unit_to_facility = _build_unit_to_facility_map(facilities)

    rows: List[Dict[str, Any]] = []

    manual_start = _parse_naive_datetime(date_start) if date_start else None
    manual_end = _parse_naive_datetime(date_end) if date_end else None
    if bool(manual_start) != bool(manual_end):
        raise ValueError("Both date_start and date_end must be provided together.")
    if manual_start and manual_end and manual_end <= manual_start:
        raise ValueError("date_end must be after date_start.")

    for facility_code in facility_codes:
        facility_meta = facilities[facility_code]
        network_code = facility_meta.network_id

        def fetch_window(start_dt: dt.datetime, end_dt: dt.datetime) -> List[Dict[str, Any]]:
            payloads = _fetch_timeseries_payloads(
                api_key=api_token,
                network_code=network_code,
                facility_codes=[facility_code],
                metrics=metrics_list,
                interval=interval,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            window_rows: List[Dict[str, Any]] = []
            for payload in payloads:
                window_rows.extend(_flatten_timeseries(payload, facilities, unit_to_facility))
            return window_rows

        if manual_start and manual_end:
            window_rows = fetch_window(manual_start, manual_end)
            rows.extend(window_rows)
            continue

        attempt_start, attempt_end = _default_time_window(network_code, interval)
        attempts = 0
        best_rows: List[Dict[str, Any]] = []
        while attempts < max_lookback_windows and attempt_end > attempt_start:
            attempt_rows = fetch_window(attempt_start, attempt_end)
            if attempt_rows:
                best_rows = attempt_rows
                break
            attempt_end = attempt_start
            attempt_start = attempt_end - dt.timedelta(days=target_window_days)
            attempts += 1

        rows.extend(best_rows)

    dataframe = pd.DataFrame(rows)
    if dataframe.empty:
        return dataframe

    dataframe = dataframe.sort_values(
        ["network_id", "network_region", "facility_name"], na_position="last"
    ).reset_index(drop=True)
    return dataframe


__all__ = [
    "fetch_facilities_dataframe",
    "fetch_facility_timeseries_dataframe",
    "FacilityMetadata",
]
