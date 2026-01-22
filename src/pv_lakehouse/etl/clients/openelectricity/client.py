"""OpenElectricity API Client with dependency injection and retry logic.

This module provides a clean, testable client for the OpenElectricity API
with support for:
- Dependency injection for HTTP client (testability)
- Exponential backoff retry logic (resilience)
- Type-safe responses via Pydantic models
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    RetryError,
)

from .models import (
    ClientConfig,
    FacilityMetadata,
    FacilitySummary,
    TimeseriesRow,
)
from .parsers import (
    build_unit_to_facility_map,
    chunk_date_range,
    extract_max_days_from_error,
    flatten_timeseries,
    format_naive_datetime,
    parse_naive_datetime,
    resolve_network_timezone,
    summarize_facility,
    parse_facility_metadata,
)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# HTTP Client Protocol (for dependency injection)
# ─────────────────────────────────────────────────────────────────────────────

class HTTPClient(Protocol):
    """Protocol for HTTP client interface."""

    def get(
        self,
        url: str,
        headers: Dict[str, str],
        params: List[Tuple[str, str]],
        timeout: int,
    ) -> Dict[str, Any]:
        """Execute GET request and return JSON response."""
        ...


class RequestsHTTPClient:
    """Default HTTP client implementation using requests library."""

    def get(
        self,
        url: str,
        headers: Dict[str, str],
        params: List[Tuple[str, str]],
        timeout: int,
    ) -> Dict[str, Any]:
        """Execute GET request using requests library."""
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        
        if response.status_code == 401:
            raise RuntimeError("401 Unauthorized: check your OpenElectricity credentials.")
        
        response.raise_for_status()
        return response.json()


# ─────────────────────────────────────────────────────────────────────────────
# API Key Management
# ─────────────────────────────────────────────────────────────────────────────

def _read_env_file(env_path: Path) -> Dict[str, str]:
    """Read environment variables from a .env file."""
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

    # Check environment variables
    for key in CANDIDATE_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            if key != "OPENELECTRICITY_API_KEY":
                os.environ.setdefault("OPENELECTRICITY_API_KEY", value)
            return value

    # Check .env file
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


# ─────────────────────────────────────────────────────────────────────────────
# OpenElectricity Client
# ─────────────────────────────────────────────────────────────────────────────

class OpenElectricityClient:
    """Clean, testable client for OpenElectricity API.
    
    Supports dependency injection for HTTP client, making it easy to mock
    in tests. Uses tenacity for automatic retry with exponential backoff.
    
    Example:
        >>> client = OpenElectricityClient(api_key="your-key")
        >>> df = client.fetch_facilities()
        
        # With custom HTTP client (for testing):
        >>> mock_client = MockHTTPClient()
        >>> client = OpenElectricityClient(api_key="test", http_client=mock_client)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        config: Optional[ClientConfig] = None,
        http_client: Optional[HTTPClient] = None,
    ):
        """Initialize the client.
        
        Args:
            api_key: API key (or will be loaded from environment)
            config: Client configuration (optional)
            http_client: HTTP client implementation (optional, for testing)
        """
        self._api_key = load_api_key(api_key)
        self._config = config or ClientConfig()
        self._http_client = http_client or RequestsHTTPClient()
        
        # Build endpoint URLs
        base_url = self._config.base_url.rstrip("/")
        self._facilities_endpoint = f"{base_url}/facilities/"
        self._data_endpoint_template = f"{base_url}/data/facilities/{{network_code}}"

    @property
    def api_key(self) -> str:
        """Return the configured API key."""
        return self._api_key

    def _get_headers(self) -> Dict[str, str]:
        """Build request headers with authentication."""
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }

    def _create_retry_decorator(self) -> Callable:
        """Create a retry decorator with exponential backoff."""
        return retry(
            retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
            stop=stop_after_attempt(self._config.max_retries),
            wait=wait_exponential(
                multiplier=1,
                min=self._config.retry_min_wait,
                max=self._config.retry_max_wait,
            ),
            reraise=True,
        )

    def _request_json(
        self,
        url: str,
        params: List[Tuple[str, str]],
    ) -> Dict[str, Any]:
        """Execute an authenticated GET request with retry logic."""
        retry_decorator = self._create_retry_decorator()
        
        @retry_decorator
        def _do_request() -> Dict[str, Any]:
            payload = self._http_client.get(
                url=url,
                headers=self._get_headers(),
                params=params,
                timeout=self._config.timeout,
            )
            
            if not payload.get("success", False):
                raise RuntimeError(payload.get("error") or "OpenElectricity API returned error")
            
            return payload

        try:
            return _do_request()
        except RetryError as e:
            # Re-raise the original exception
            raise e.last_attempt.exception() from e

    def _build_params(
        self,
        networks: Iterable[str],
        statuses: Iterable[str],
        fueltechs: Iterable[str],
        region: Optional[str],
    ) -> List[Tuple[str, str]]:
        """Build query parameters for facility requests."""
        params: List[Tuple[str, str]] = []
        params.extend(("network_id", value) for value in networks)
        params.extend(("status_id", value) for value in statuses)
        params.extend(("fueltech_id", value) for value in fueltechs)
        if region:
            params.append(("network_region", region))
        return params

    # ─────────────────────────────────────────────────────────────────────────
    # Public API: Facilities
    # ─────────────────────────────────────────────────────────────────────────

    def fetch_facilities(
        self,
        *,
        selected_codes: Optional[Sequence[str]] = None,
        networks: Optional[Sequence[str]] = None,
        statuses: Optional[Sequence[str]] = None,
        fueltechs: Optional[Sequence[str]] = None,
        region: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch facility metadata and return as pandas DataFrame.
        
        Args:
            selected_codes: Filter to specific facility codes
            networks: Network IDs (default: ["NEM", "WEM"])
            statuses: Status filters (default: ["operating"])
            fueltechs: Fuel technology filters (default: ["solar_utility"])
            region: Network region filter
            
        Returns:
            DataFrame with facility metadata
        """
        networks_list = [v.upper() for v in (networks or ["NEM", "WEM"]) if v]
        statuses_list = [v.lower() for v in (statuses or ["operating"]) if v]
        fueltechs_list = [v.lower() for v in (fueltechs or ["solar_utility"]) if v]
        region_filter = region.strip() if isinstance(region, str) else None
        
        if region_filter and region_filter.upper() == "ALL":
            region_filter = None

        params = self._build_params(networks_list, statuses_list, fueltechs_list, region_filter)
        payload = self._request_json(self._facilities_endpoint, params)
        
        # Parse facilities to summaries
        rows = [summarize_facility(item).model_dump() for item in payload.get("data", [])]
        dataframe = pd.DataFrame(rows)

        if dataframe.empty:
            return dataframe

        # Sort by network, region, name
        dataframe = dataframe.sort_values(
            ["network_id", "network_region", "facility_name"], na_position="last"
        ).reset_index(drop=True)

        # Filter by selected codes if provided
        if selected_codes:
            desired = {code.upper() for code in selected_codes if code}
            if desired:
                dataframe["facility_code"] = dataframe["facility_code"].astype(str)
                dataframe = dataframe[
                    dataframe["facility_code"].str.upper().isin(desired)
                ].reset_index(drop=True)

        return dataframe

    def _fetch_facility_metadata(
        self, facility_codes: Sequence[str]
    ) -> Dict[str, FacilityMetadata]:
        """Fetch metadata for specific facilities."""
        if not facility_codes:
            raise ValueError("facility_codes must not be empty")

        params = [("facility_code", code) for code in facility_codes]
        payload = self._request_json(self._facilities_endpoint, params)

        facilities: Dict[str, FacilityMetadata] = {}
        for facility in payload.get("data", []):
            meta = parse_facility_metadata(facility)
            if meta:
                facilities[meta.code] = meta

        missing = [code for code in facility_codes if code not in facilities]
        if missing:
            raise ValueError(f"Unknown facility codes: {', '.join(missing)}")

        return facilities

    # ─────────────────────────────────────────────────────────────────────────
    # Public API: Timeseries
    # ─────────────────────────────────────────────────────────────────────────

    def _default_time_window(
        self, network_code: str, interval: str
    ) -> Tuple[dt.datetime, dt.datetime]:
        """Calculate default time window based on network timezone."""
        tzinfo = resolve_network_timezone(network_code)
        now = dt.datetime.now(tz=tzinfo)
        
        if interval == "5m":
            start = now - dt.timedelta(hours=2)
        else:
            start = now - dt.timedelta(days=TARGET_WINDOW_DAYS)
        
        return start.replace(tzinfo=None), now.replace(tzinfo=None)

    def _fetch_timeseries_payloads(
        self,
        network_code: str,
        facility_codes: Sequence[str],
        metrics: Sequence[str],
        interval: str,
        start_dt: dt.datetime,
        end_dt: dt.datetime,
    ) -> List[Dict[str, Any]]:
        """Fetch timeseries data, handling chunking for large date ranges."""
        params_base: List[Tuple[str, str]] = [("interval", interval)]
        params_base.extend(("metrics", metric) for metric in metrics)
        params_base.extend(("facility_code", code) for code in facility_codes)

        url = self._data_endpoint_template.format(network_code=network_code)
        max_chunk_days = INTERVAL_MAX_DAYS.get(interval)

        while True:
            payloads: List[Dict[str, Any]] = []
            segments = chunk_date_range(start_dt, end_dt, max_chunk_days)

            try:
                for chunk_start, chunk_end in segments:
                    params = list(params_base)
                    params.append(("date_start", format_naive_datetime(chunk_start)))
                    params.append(("date_end", format_naive_datetime(chunk_end)))
                    payload = self._request_json(url, params)
                    payloads.append(payload)
                return payloads
                
            except requests.HTTPError as exc:
                response_text = ""
                if exc.response is not None:
                    try:
                        response_text = exc.response.text
                    except Exception:
                        response_text = ""
                
                max_days = extract_max_days_from_error(response_text)
                if not max_days:
                    raise
                    
                if max_chunk_days and max_chunk_days <= max_days:
                    if max_days <= 1:
                        raise
                    max_chunk_days = max_days - 1
                else:
                    max_chunk_days = max_days

    def fetch_timeseries(
        self,
        *,
        facility_codes: Sequence[str],
        metrics: Optional[Sequence[str]] = None,
        interval: str = "1h",
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
        target_window_days: int = TARGET_WINDOW_DAYS,
        max_lookback_windows: int = MAX_LOOKBACK_WINDOWS,
    ) -> pd.DataFrame:
        """Fetch facility production timeseries as pandas DataFrame.
        
        Args:
            facility_codes: List of facility codes to fetch
            metrics: Metrics to fetch (default: ["power", "energy", "market_value", "price"])
            interval: Data interval (default: "1h")
            date_start: Start datetime (YYYY-MM-DDTHH:MM:SS)
            date_end: End datetime (YYYY-MM-DDTHH:MM:SS)
            target_window_days: Days per lookback window
            max_lookback_windows: Maximum lookback attempts
            
        Returns:
            DataFrame with timeseries data
        """
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(
                f"Invalid interval '{interval}'. Supported values: {', '.join(sorted(SUPPORTED_INTERVALS))}."
            )

        # Validate date range early (before any API calls)
        manual_start = parse_naive_datetime(date_start) if date_start else None
        manual_end = parse_naive_datetime(date_end) if date_end else None
        
        if bool(manual_start) != bool(manual_end):
            raise ValueError("Both date_start and date_end must be provided together.")
        if manual_start and manual_end and manual_end <= manual_start:
            raise ValueError("date_end must be after date_start.")

        metrics_list = [metric.lower() for metric in (metrics or []) if metric]
        if not metrics_list:
            metrics_list = ["power", "energy", "market_value", "price"]

        facility_codes = [code.upper() for code in facility_codes]
        facilities = self._fetch_facility_metadata(facility_codes)
        unit_to_facility = build_unit_to_facility_map(facilities)

        rows: List[Dict[str, Any]] = []

        for facility_code in facility_codes:
            facility_meta = facilities[facility_code]
            network_code = facility_meta.network_id

            def fetch_window(start_dt: dt.datetime, end_dt: dt.datetime) -> List[Dict[str, Any]]:
                payloads = self._fetch_timeseries_payloads(
                    network_code=network_code,
                    facility_codes=[facility_code],
                    metrics=metrics_list,
                    interval=interval,
                    start_dt=start_dt,
                    end_dt=end_dt,
                )
                window_rows: List[Dict[str, Any]] = []
                for payload in payloads:
                    ts_rows = flatten_timeseries(payload, facilities, unit_to_facility)
                    window_rows.extend([row.model_dump() for row in ts_rows])
                return window_rows

            if manual_start and manual_end:
                window_rows = fetch_window(manual_start, manual_end)
                rows.extend(window_rows)
                continue

            # Auto-detect time window with lookback
            attempt_start, attempt_end = self._default_time_window(network_code, interval)
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


# ─────────────────────────────────────────────────────────────────────────────
# Backward-Compatible Module Functions
# ─────────────────────────────────────────────────────────────────────────────

# These functions maintain backward compatibility with the old module API

def fetch_facilities_dataframe(
    *,
    api_key: Optional[str] = None,
    selected_codes: Optional[Sequence[str]] = None,
    networks: Optional[Sequence[str]] = None,
    statuses: Optional[Sequence[str]] = None,
    fueltechs: Optional[Sequence[str]] = None,
    region: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch facility metadata and return as pandas DataFrame.
    
    This is a backward-compatible wrapper around OpenElectricityClient.fetch_facilities().
    """
    client = OpenElectricityClient(api_key=api_key)
    return client.fetch_facilities(
        selected_codes=selected_codes,
        networks=networks,
        statuses=statuses,
        fueltechs=fueltechs,
        region=region,
    )


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
    """Fetch facility production timeseries as pandas DataFrame.
    
    This is a backward-compatible wrapper around OpenElectricityClient.fetch_timeseries().
    """
    client = OpenElectricityClient(api_key=api_key)
    return client.fetch_timeseries(
        facility_codes=facility_codes,
        metrics=metrics,
        interval=interval,
        date_start=date_start,
        date_end=date_end,
        target_window_days=target_window_days,
        max_lookback_windows=max_lookback_windows,
    )


__all__ = [
    # Main client class
    "OpenElectricityClient",
    # HTTP client protocol and implementation
    "HTTPClient",
    "RequestsHTTPClient",
    # API key management
    "load_api_key",
    # Backward-compatible functions
    "fetch_facilities_dataframe",
    "fetch_facility_timeseries_dataframe",
    # Constants
    "CANDIDATE_ENV_KEYS",
    "SUPPORTED_INTERVALS",
    "INTERVAL_MAX_DAYS",
    "TARGET_WINDOW_DAYS",
    "MAX_LOOKBACK_WINDOWS",
]
