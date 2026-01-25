from __future__ import annotations
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple
import pandas as pd
import requests

logger = logging.getLogger(__name__)
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
    """HTTP client implementation using requests library with connection pooling."""
    
    def __init__(self) -> None:
        """Initialize the HTTP client with lazy session creation."""
        self._session: Optional[requests.Session] = None
    
    def _get_session(self) -> requests.Session:
        """Get or create a requests session for connection pooling."""
        if self._session is None:
            self._session = requests.Session()
        return self._session
    
    def close(self) -> None:
        """Close the HTTP session and release resources."""
        if self._session is not None:
            self._session.close()
            self._session = None
    
    def __enter__(self) -> "RequestsHTTPClient":
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close the session."""
        self.close()
    
    def get(
        self,
        url: str,
        headers: Dict[str, str],
        params: List[Tuple[str, str]],
        timeout: int,
    ) -> Dict[str, Any]:
        """
        Perform an HTTP GET request and return JSON response.

        Args:
            url: The endpoint URL to request.
            headers: HTTP headers to include in the request.
            params: Query parameters as list of tuples.
            timeout: Request timeout in seconds.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            RuntimeError: If the request returns 401 Unauthorized.
            requests.exceptions.Timeout: If the request times out.
            requests.exceptions.ConnectionError: If connection fails.
            requests.exceptions.HTTPError: For other HTTP errors.
        """
        session = self._get_session()
        response: Optional[requests.Response] = None
        
        try:
            # Perform GET request using session for connection pooling
            response = session.get(url, headers=headers, params=params, timeout=timeout)
            
            if response.status_code == 401:
                raise RuntimeError("401 Unauthorized: check your OpenElectricity credentials.")
            
            response.raise_for_status()
            try:
                return response.json()
            except json.JSONDecodeError as json_exc:
                logger.error(
                    "Invalid JSON response",
                    extra={"url": url, "error": str(json_exc)}
                )
                raise ValueError(f"Failed to parse JSON response from {url}: {json_exc}") from json_exc
            
        except requests.exceptions.Timeout as exc:
            logger.error(
                "Request timeout",
                extra={"url": url, "timeout": timeout, "error": str(exc)}
            )
            raise requests.exceptions.Timeout(
                f"Request timed out after {timeout} seconds while connecting to {url}"
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            logger.error(
                "Connection failed",
                extra={"url": url, "error": str(exc)}
            )
            raise requests.exceptions.ConnectionError(
                f"Failed to establish connection to {url}: {exc}"
            ) from exc
        except requests.exceptions.HTTPError as exc:
            error_text = ""
            if response is not None:
                try:
                    error_text = response.text[:200]
                except (AttributeError, UnicodeDecodeError, IOError) as e:
                    logger.debug("Failed to read error response text", exc_info=e)
            logger.error(
                "HTTP error",
                extra={
                    "url": url,
                    "status_code": response.status_code if response else "unknown",
                    "error_preview": error_text
                }
            )
            raise requests.exceptions.HTTPError(
                f"HTTP {response.status_code if response else 'unknown'} error for {url}: {error_text}"
            ) from exc
        finally:
            # Ensure response is properly closed to release connection back to pool
            if response is not None:
                try:
                    response.close()
                except (OSError, IOError) as close_exc:
                    logger.warning(f"Failed to close response: {close_exc}")

def _read_env_file(env_path: Path) -> Dict[str, str]:
    """
    Read and parse a .env file.

    Args:
        env_path: Path to the .env file.

    Returns:
        Dictionary of key-value pairs from the file.
    """
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
    """
    Load OpenElectricity API key from CLI, environment, or .env file.

    Args:
        cli_key: Optional API key provided via CLI.

    Returns:
        The API key string.

    Raises:
        RuntimeError: If no API key is found.
    """
    if cli_key:
        logger.debug("Using API key from CLI parameter")
        return cli_key
    # Check environment variables
    for key in CANDIDATE_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            if key != "OPENELECTRICITY_API_KEY":
                os.environ.setdefault("OPENELECTRICITY_API_KEY", value)
            logger.debug(f"Using API key from environment variable: {key}")
            return value
    # Check .env file
    env_values = _read_env_file(Path.cwd() / ".env")
    for key in CANDIDATE_ENV_KEYS:
        value = env_values.get(key)
        if value:
            os.environ.setdefault(key, value)
            if key != "OPENELECTRICITY_API_KEY":
                os.environ.setdefault("OPENELECTRICITY_API_KEY", value)
            logger.debug(f"Using API key from .env file: {key}")
            return value
    logger.error("No API key found in any source")
    raise RuntimeError(
        "Missing API key. Provide via parameter, environment variable, or .env file."
    )

class OpenElectricityClient:
    """
    Client for interacting with the OpenElectricity API.

    Provides methods to fetch facility metadata and timeseries data.
    Supports connection pooling, retry logic, and API key management.

    Example:
        >>> with OpenElectricityClient(api_key="your-key") as client:
        ...     facilities = client.fetch_facilities(networks=["NEM"])
        ...     timeseries = client.fetch_timeseries(facility_codes=["BALBG1"])
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        config: Optional[ClientConfig] = None,
        http_client: Optional[HTTPClient] = None,
    ) -> None:
        """
        Initialize the OpenElectricity client.

        Args:
            api_key: Optional API key. If not provided, will be loaded from
                environment variables or .env file.
            config: Optional client configuration for timeouts and retries.
            http_client: Optional HTTP client for dependency injection (testing).
        """
        logger.info("Initializing OpenElectricity client")
        self._api_key = load_api_key(api_key)
        logger.debug("API key loaded successfully")
        self._config = config or ClientConfig()
        self._owns_http_client = http_client is None
        self._http_client = http_client or RequestsHTTPClient()
        
        base_url = self._config.base_url.rstrip("/")
        self._facilities_endpoint = f"{base_url}/facilities/"
        self._data_endpoint_template = f"{base_url}/data/facilities/{{network_code}}"

    def close(self) -> None:
        """Close the client and release resources."""
        if self._owns_http_client and hasattr(self._http_client, "close"):
            self._http_client.close()
    
    def __enter__(self) -> "OpenElectricityClient":
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close the client."""
        self.close()

    @property
    def api_key(self) -> str:
        """Return the API key used for authentication."""
        return self._api_key

    def _get_headers(self) -> Dict[str, str]:
        """Build HTTP headers for API requests."""
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }

    def _create_retry_decorator(self) -> Callable:
        """
        Create a tenacity retry decorator with configured settings.

        Returns:
            A retry decorator configured with exponential backoff.
        """
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
        """
        Execute an HTTP GET request with retry logic.

        Args:
            url: The API endpoint URL.
            params: Query parameters as list of tuples.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            RuntimeError: If the API returns an error response.
            requests.RequestException: If all retry attempts fail.
        """
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

    def _build_params(
        self,
        networks: Iterable[str],
        statuses: Iterable[str],
        fueltechs: Iterable[str],
        region: Optional[str],
    ) -> List[Tuple[str, str]]:
        """
        Build query parameters for facility filtering.

        Args:
            networks: Network IDs to filter by (e.g., "NEM", "WEM").
            statuses: Facility statuses to filter by (e.g., "operating").
            fueltechs: Fuel technology types to filter by.
            region: Optional network region to filter by.

        Returns:
            List of (key, value) tuples for query parameters.
        """
        params: List[Tuple[str, str]] = []
        params.extend(("network_id", value) for value in networks)
        params.extend(("status_id", value) for value in statuses)
        params.extend(("fueltech_id", value) for value in fueltechs)
        if region:
            params.append(("network_region", region))
        return params

    def fetch_facilities(
        self,
        *,
        selected_codes: Optional[Sequence[str]] = None,
        networks: Optional[Sequence[str]] = None,
        statuses: Optional[Sequence[str]] = None,
        fueltechs: Optional[Sequence[str]] = None,
        region: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch facility metadata from the OpenElectricity API.

        Args:
            selected_codes: Optional list of facility codes to filter.
            networks: Network IDs (default: ["NEM", "WEM"]).
            statuses: Facility statuses (default: ["operating"]).
            fueltechs: Fuel technologies (default: ["solar_utility"]).
            region: Optional network region filter.

        Returns:
            DataFrame with facility metadata.
        """
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
    def _fetch_facility_metadata(
        self, facility_codes: Sequence[str]
    ) -> Dict[str, FacilityMetadata]:
        """
        Fetch detailed metadata for specific facilities.

        Args:
            facility_codes: List of facility codes to fetch.

        Returns:
            Dictionary mapping facility codes to their metadata.

        Raises:
            ValueError: If facility_codes is empty or codes are not found.
        """
        if not facility_codes:
            raise ValueError("facility_codes must not be empty")

        # Validate facility codes
        validated_codes = []
        for code in facility_codes:
            if not isinstance(code, str):
                raise ValueError(f"facility_code must be string, got {type(code).__name__}")
            code_stripped = code.strip()
            if not code_stripped:
                raise ValueError("facility_code cannot be empty or whitespace")
            if len(code_stripped) > 100:  # Reasonable limit
                raise ValueError(f"facility_code too long: {len(code_stripped)} characters")
            validated_codes.append(code_stripped)

        params = [("facility_code", code) for code in validated_codes]
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
    
    def _default_time_window(
        self, network_code: str, interval: str
    ) -> Tuple[dt.datetime, dt.datetime]:
        """
        Calculate default time window for timeseries queries.

        Args:
            network_code: The network code (e.g., "NEM").
            interval: Data interval (e.g., "1h").

        Returns:
            Tuple of (start_datetime, end_datetime) in network timezone.
        """
        # Get network timezone
        tzinfo = resolve_network_timezone(network_code)
        now = dt.datetime.now(tz=tzinfo)
        
        # Calculate start
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
        """
        Fetch raw timeseries payloads from the API.

        Handles date range chunking and automatic retry on max_days errors.

        Args:
            network_code: The network code (e.g., "NEM").
            facility_codes: List of facility codes to fetch.
            metrics: List of metrics to fetch (e.g., ["power", "energy"]).
            interval: Data interval (e.g., "1h").
            start_dt: Start datetime.
            end_dt: End datetime.

        Returns:
            List of API response payloads.
        """
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
                    except (AttributeError, UnicodeDecodeError, IOError):
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
        """
        Fetch timeseries data for facilities.

        Args:
            facility_codes: List of facility codes to fetch data for.
            metrics: List of metrics (default: ["power", "energy"]).
            interval: Data interval (default: "1h").
            date_start: Optional start date (ISO format).
            date_end: Optional end date (ISO format).
            target_window_days: Days per lookback window for auto-detection.
            max_lookback_windows: Maximum lookback attempts.

        Returns:
            DataFrame with timeseries data.

        Raises:
            ValueError: If interval is invalid or date range is malformed.
        """
        # Validate numeric parameters
        if not isinstance(target_window_days, int) or target_window_days <= 0:
            raise ValueError(f"target_window_days must be positive integer, got {target_window_days}")
        if not isinstance(max_lookback_windows, int) or max_lookback_windows <= 0:
            raise ValueError(f"max_lookback_windows must be positive integer, got {max_lookback_windows}")
        if target_window_days > 365:
            raise ValueError(f"target_window_days too large: {target_window_days} (max: 365)")
        if max_lookback_windows > 100:
            raise ValueError(f"max_lookback_windows too large: {max_lookback_windows} (max: 100)")

        # Validate facility_codes
        if not facility_codes:
            raise ValueError("facility_codes must not be empty")
        if len(facility_codes) > 50:  # Reasonable batch limit
            raise ValueError(f"Too many facility_codes: {len(facility_codes)} (max: 50)")

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

def fetch_facilities_dataframe(
    *,
    api_key: Optional[str] = None,
    selected_codes: Optional[Sequence[str]] = None,
    networks: Optional[Sequence[str]] = None,
    statuses: Optional[Sequence[str]] = None,
    fueltechs: Optional[Sequence[str]] = None,
    region: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch facilities DataFrame (backward-compatible function).

    This is a convenience wrapper around OpenElectricityClient.fetch_facilities().

    Args:
        api_key: Optional API key for authentication.
        selected_codes: Optional list of facility codes to filter.
        networks: Network IDs (default: ["NEM", "WEM"]).
        statuses: Facility statuses (default: ["operating"]).
        fueltechs: Fuel technologies (default: ["solar_utility"]).
        region: Optional network region filter.

    Returns:
        DataFrame with facility metadata.
    """
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
    """
    Fetch facility timeseries DataFrame (backward-compatible function).

    This is a convenience wrapper around OpenElectricityClient.fetch_timeseries().

    Args:
        facility_codes: List of facility codes to fetch data for.
        metrics: List of metrics (default: ["power", "energy"]).
        interval: Data interval (default: "1h").
        date_start: Optional start date (ISO format).
        date_end: Optional end date (ISO format).
        api_key: Optional API key for authentication.
        target_window_days: Days per lookback window for auto-detection.
        max_lookback_windows: Maximum lookback attempts.

    Returns:
        DataFrame with timeseries data.
    """
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