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

# HTTP Client Protocol
class HTTPClient(Protocol):
    def get(
        self,
        url: str,
        headers: Dict[str, str],
        params: List[Tuple[str, str]],
        timeout: int,
    ) -> Dict[str, Any]:
        ...

class RequestsHTTPClient:    
    def __init__(self) -> None:
        self._session: Optional[requests.Session] = None
    
    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session
    
    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
    
    def __enter__(self) -> "RequestsHTTPClient":
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
    
    def get(
        self,
        url: str,
        headers: Dict[str, str],
        params: List[Tuple[str, str]],
        timeout: int,
    ) -> Dict[str, Any]:
        session = self._get_session()
        response: Optional[requests.Response] = None
        
        try:
            # Perform GET request using session for connection pooling
            response = session.get(url, headers=headers, params=params, timeout=timeout)
            
            if response.status_code == 401:
                raise RuntimeError("401 Unauthorized: check your OpenElectricity credentials.")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout as exc:
            raise requests.exceptions.Timeout(
                f"Request timed out after {timeout} seconds while connecting to {url}"
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            raise requests.exceptions.ConnectionError(
                f"Failed to establish connection to {url}: {exc}"
            ) from exc
        except requests.exceptions.HTTPError as exc:
            error_text = ""
            if response is not None:
                try:
                    error_text = response.text[:200]
                except Exception:
                    pass
            raise requests.exceptions.HTTPError(
                f"HTTP {response.status_code if response else 'unknown'} error for {url}: {error_text}"
            ) from exc
        finally:
            # Ensure response is properly closed to release connection back to pool
            if response is not None:
                response.close()

# Read environment file
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
# Load API key
def load_api_key(cli_key: Optional[str] = None) -> str:
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

# Main client class for interacting with OpenElectricity API 
class OpenElectricityClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        config: Optional[ClientConfig] = None,
        http_client: Optional[HTTPClient] = None,
    ):
        # Load API key
        self._api_key = load_api_key(api_key)
        # Config
        self._config = config or ClientConfig()
        # HTTP client - track if we own it for cleanup
        self._owns_http_client = http_client is None
        self._http_client = http_client or RequestsHTTPClient()
        
        # Build endpoint URLs
        base_url = self._config.base_url.rstrip("/")
        self._facilities_endpoint = f"{base_url}/facilities/"
        self._data_endpoint_template = f"{base_url}/data/facilities/{{network_code}}"

    def close(self) -> None:
        if self._owns_http_client and hasattr(self._http_client, "close"):
            self._http_client.close()
    
    def __enter__(self) -> "OpenElectricityClient":
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    @property
    # Return API key
    def api_key(self) -> str:
        return self._api_key
    # Return request headers
    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }
    # Create retry decorator 
    def _create_retry_decorator(self) -> Callable:
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
    # Execute request
    def _request_json(
        self,
        url: str,
        params: List[Tuple[str, str]],
    ) -> Dict[str, Any]:
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
            raise e.last_attempt.exception() from e

    # Build params for request
    def _build_params(
        self,
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

    # Fetch facilities list from API
    def fetch_facilities(
        self,
        *,
        selected_codes: Optional[Sequence[str]] = None,
        networks: Optional[Sequence[str]] = None,
        statuses: Optional[Sequence[str]] = None,
        fueltechs: Optional[Sequence[str]] = None,
        region: Optional[str] = None,
    ) -> pd.DataFrame:
        # Normalize parameters
        networks_list = [v.upper() for v in (networks or ["NEM", "WEM"]) if v]
        statuses_list = [v.lower() for v in (statuses or ["operating"]) if v]
        fueltechs_list = [v.lower() for v in (fueltechs or ["solar_utility"]) if v]
        region_filter = region.strip() if isinstance(region, str) else None
        
        # If region_filter is ALL, remove filter
        if region_filter and region_filter.upper() == "ALL":
            region_filter = None
        # Build request params
        params = self._build_params(networks_list, statuses_list, fueltechs_list, region_filter)
        payload = self._request_json(self._facilities_endpoint, params)
        
        # Parse facilities into DataFrame
        rows = [summarize_facility(item).model_dump() for item in payload.get("data", [])]
        dataframe = pd.DataFrame(rows)

        if dataframe.empty:
            return dataframe

        # Sort DataFrame by network, region, name
        dataframe = dataframe.sort_values(
            ["network_id", "network_region", "facility_name"], na_position="last"
        ).reset_index(drop=True)

        # Filter by selected_codes
        if selected_codes:
            desired = {code.upper() for code in selected_codes if code}
            if desired:
                dataframe["facility_code"] = dataframe["facility_code"].astype(str)
                dataframe = dataframe[
                    dataframe["facility_code"].str.upper().isin(desired)
                ].reset_index(drop=True)

        return dataframe
    # Fetch facility metadata
    def _fetch_facility_metadata(
        self, facility_codes: Sequence[str]
    ) -> Dict[str, FacilityMetadata]:
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
    
    # Calculate time window 
    def _default_time_window(
        self, network_code: str, interval: str
    ) -> Tuple[dt.datetime, dt.datetime]:
        # Get network timezone
        tzinfo = resolve_network_timezone(network_code)
        now = dt.datetime.now(tz=tzinfo)
        
        # Calculate start
        start = now - dt.timedelta(days=TARGET_WINDOW_DAYS)
        
        return start.replace(tzinfo=None), now.replace(tzinfo=None)

    # Fetch timeseries data 
    def _fetch_timeseries_payloads(
        self,
        network_code: str,
        facility_codes: Sequence[str],
        metrics: Sequence[str],
        interval: str,
        start_dt: dt.datetime,
        end_dt: dt.datetime,
    ) -> List[Dict[str, Any]]:
        # Create base params
        params_base: List[Tuple[str, str]] = [("interval", interval)]
        params_base.extend(("metrics", metric) for metric in metrics)
        params_base.extend(("facility_code", code) for code in facility_codes)
        # Create URL
        url = self._data_endpoint_template.format(network_code=network_code)
        max_chunk_days = INTERVAL_MAX_DAYS.get(interval)

        while True:
            # Create payload list
            payloads: List[Dict[str, Any]] = []
            # Split date range into segments
            segments = chunk_date_range(start_dt, end_dt, max_chunk_days)
            
            try:
                # Fetch data
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
                # Get max_days from error
                max_days = extract_max_days_from_error(response_text)
                if not max_days:
                    raise
                # If max_days is less than max_chunk_days
                if max_chunk_days and max_chunk_days <= max_days:
                    if max_days <= 1:
                        raise
                    max_chunk_days = max_days - 1
                else:
                    max_chunk_days = max_days

    # Fetch timeseries data
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
        # Validate interval
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(
                f"Invalid interval '{interval}'. Supported values: {', '.join(sorted(SUPPORTED_INTERVALS))}."
            )


        # Parse date range
        manual_start = parse_naive_datetime(date_start) if date_start else None
        manual_end = parse_naive_datetime(date_end) if date_end else None
        # Validate date range
        if bool(manual_start) != bool(manual_end):
            raise ValueError("Both date_start and date_end must be provided together.")
        if manual_start and manual_end and manual_end <= manual_start:
            raise ValueError("date_end must be after date_start.")
        # Validate metrics
        metrics_list = [metric.lower() for metric in (metrics or []) if metric]
        if not metrics_list:
            metrics_list = ["power", "energy"]
        # Validate facility codes
        facility_codes = [code.upper() for code in facility_codes]
        facilities = self._fetch_facility_metadata(facility_codes)
        unit_to_facility = build_unit_to_facility_map(facilities)

        # Fetch timeseries data
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
                # Flatten timeseries
                window_rows: List[Dict[str, Any]] = []
                for payload in payloads:
                    ts_rows = flatten_timeseries(payload, facilities, unit_to_facility)
                    window_rows.extend([row.model_dump() for row in ts_rows])
                return window_rows

            if manual_start and manual_end:
                window_rows = fetch_window(manual_start, manual_end)
                rows.extend(window_rows)
                continue

            # Auto-detect with lookback strategy 
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

        # Convert to dataframe
        dataframe = pd.DataFrame(rows)
        if dataframe.empty:
            return dataframe

        dataframe = dataframe.sort_values(
            ["network_id", "network_region", "facility_name"], na_position="last"  # Keep null values at the end
        ).reset_index(drop=True)
        
        return dataframe

# Backward-compatible functions
def fetch_facilities_dataframe(
    *,
    api_key: Optional[str] = None,
    selected_codes: Optional[Sequence[str]] = None,
    networks: Optional[Sequence[str]] = None,
    statuses: Optional[Sequence[str]] = None,
    fueltechs: Optional[Sequence[str]] = None,
    region: Optional[str] = None,
) -> pd.DataFrame:
    with OpenElectricityClient(api_key=api_key) as client:
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
    with OpenElectricityClient(api_key=api_key) as client:
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