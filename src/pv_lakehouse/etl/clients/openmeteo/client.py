from __future__ import annotations
import datetime as dt
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple
import pandas as pd
import requests
from .constants import (
    AIR_QUALITY_ENDPOINT,
    AIR_QUALITY_MAX_DAYS,
    DEFAULT_AIR_VARS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BACKOFF,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_WEATHER_VARS,
    FORECAST_MAX_DAYS,
    RETRYABLE_STATUS_CODES,
    WEATHER_ENDPOINTS,
)
from .models import (
    FacilityLocation,
    OpenMeteoAPIError,
    OpenMeteoConfigError,
    OpenMeteoRateLimitError,
)
from .rate_limiter import RateLimiter

LOGGER = logging.getLogger(__name__)
class EndpointStrategy(ABC):
    """Abstract base class for weather endpoint selection strategies."""
    @abstractmethod
    def select_endpoint(self, current_date: dt.date, archive_cutoff: dt.date) -> str:
        """
        Select the appropriate endpoint for a given date.

        Args:
            current_date: The date for which data is being requested.
            archive_cutoff: The cutoff date separating archive from forecast.

        Returns:
            Endpoint name: either "archive" or "forecast".
        """
        pass

class AutoEndpointStrategy(EndpointStrategy):
    """Automatically select archive vs forecast based on date relative to cutoff."""

    def select_endpoint(self, current_date: dt.date, archive_cutoff: dt.date) -> str:
        if current_date <= archive_cutoff:
            return "archive"
        return "forecast"

class FixedEndpointStrategy(EndpointStrategy):
    """Always use a fixed endpoint regardless of date."""

    def __init__(self, endpoint: str) -> None:
        if endpoint not in WEATHER_ENDPOINTS:
            raise OpenMeteoConfigError(f"Unknown endpoint: {endpoint}")
        self._endpoint = endpoint

    def select_endpoint(self, current_date: dt.date, archive_cutoff: dt.date) -> str:
        return self._endpoint

def create_endpoint_strategy(preference: str) -> EndpointStrategy:
    """
    Factory function to create an endpoint selection strategy.

    Args:
        preference: One of "auto", "archive", or "forecast".

    Returns:
        An EndpointStrategy instance.

    Raises:
        OpenMeteoConfigError: If preference is not recognized.
    """
    if preference == "auto":
        return AutoEndpointStrategy()
    if preference in WEATHER_ENDPOINTS:
        return FixedEndpointStrategy(preference)
    raise OpenMeteoConfigError(f"Unsupported weather endpoint preference: {preference}")

def _augment_http_error(exc: requests.HTTPError) -> OpenMeteoAPIError:
    """Convert requests.HTTPError to OpenMeteoAPIError with additional details."""
    response = exc.response
    status_code = response.status_code if response is not None else None
    detail: Optional[str] = None

    if response is not None:
        try:
            body = response.json()
            if isinstance(body, dict):
                detail = body.get("reason") or body.get("error")
        except (ValueError, requests.JSONDecodeError):
            detail = response.text

    message = f"{exc}"
    if detail:
        message = f"{message} (details: {detail})"

    if status_code == 429:
        return OpenMeteoRateLimitError(
            message, status_code=status_code, detail=detail, response=response
        )

    return OpenMeteoAPIError(
        message, status_code=status_code, detail=detail, response=response
    )

def _request_json(
    url: str,
    params: Dict[str, str],
    *,
    retries: int,
    backoff: float,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """
    Make an HTTP GET request with retry logic.

    Args:
        url: The API endpoint URL.
        params: Query parameters for the request.
        retries: Maximum number of retry attempts.
        backoff: Initial backoff delay in seconds (doubles on each retry).
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        OpenMeteoAPIError: If the request fails after all retries.
    """
    import time

    attempt = 0
    delay = max(backoff, 0.0)
    last_exc: Optional[Exception] = None

    while attempt < max(1, retries):
        attempt += 1
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            formatted = _augment_http_error(exc)
            status = exc.response.status_code if exc.response is not None else None

            # Don't retry client errors (except 408 timeout and 429 rate limit)
            if status is not None and status < 500 and status not in {408, 429}:
                raise formatted from exc
            last_exc = formatted
        except requests.RequestException as exc:
            last_exc = OpenMeteoAPIError(str(exc))

        if attempt >= max(1, retries):
            break
        if delay > 0:
            time.sleep(delay)
            delay *= 2

    if last_exc is not None:
        raise last_exc
    raise OpenMeteoAPIError("Open-Meteo request failed without raising an exception")


def _parse_hourly(payload: Dict[str, Any]) -> pd.DataFrame:
    """Parse the hourly data from an Open-Meteo API response."""
    hourly = payload.get("hourly") or {}
    if not hourly:
        return pd.DataFrame()
    frame = pd.DataFrame(hourly)
    if "time" in frame.columns:
        frame = frame.rename(columns={"time": "date"})
    return frame


def _chunk_dates(
    start: dt.date, end: dt.date, chunk_days: int
) -> Iterable[Tuple[dt.date, dt.date]]:
    """Split a date range into smaller chunks."""
    cursor = start
    delta = dt.timedelta(days=max(chunk_days, 1))
    while cursor <= end:
        chunk_end = min(cursor + delta - dt.timedelta(days=1), end)
        yield cursor, chunk_end
        cursor = chunk_end + dt.timedelta(days=1)


def fetch_weather_dataframe(
    facility: FacilityLocation,
    *,
    start: dt.date,
    end: dt.date,
    chunk_days: int = 30,
    hourly_variables: str = DEFAULT_WEATHER_VARS,
    endpoint_preference: str = "auto",
    timezone: str = "UTC",
    limiter: Optional[RateLimiter] = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    max_workers: int = 4,
) -> pd.DataFrame:
    """
    Fetch hourly weather data for a facility from the Open-Meteo API.

    Automatically selects between archive and forecast endpoints based on
    the date range unless a specific endpoint preference is provided.

    Args:
        facility: Location metadata for the facility.
        start: Start date for the data range.
        end: End date for the data range.
        chunk_days: Maximum days per API request (default: 30).
        hourly_variables: Comma-separated list of weather variables.
        endpoint_preference: "auto", "archive", or "forecast".
        timezone: Timezone for the returned data (default: "UTC").
        limiter: Optional rate limiter for API requests.
        max_retries: Maximum retry attempts for failed requests.
        retry_backoff: Initial backoff delay in seconds.
        max_workers: Maximum concurrent request threads.

    Returns:
        DataFrame with hourly weather data, or empty DataFrame on failure.

    Raises:
        OpenMeteoConfigError: If endpoint_preference is invalid.
    """
    limiter = limiter or RateLimiter(0)
    max_workers = max(1, max_workers)
    today = dt.date.today()
    archive_cutoff = today - dt.timedelta(days=FORECAST_MAX_DAYS)

    strategy = create_endpoint_strategy(endpoint_preference)

    # Build request windows
    windows: List[Tuple[dt.date, dt.date]] = []
    cursor = start
    while cursor <= end:
        endpoint_name = strategy.select_endpoint(cursor, archive_cutoff)
        max_span = chunk_days if endpoint_name == "archive" else min(chunk_days, FORECAST_MAX_DAYS)
        window_end = min(cursor + dt.timedelta(days=max_span) - dt.timedelta(days=1), end)

        if endpoint_name == "archive":
            window_end = min(window_end, archive_cutoff)
            if window_end < cursor:
                if endpoint_preference == "archive":
                    LOGGER.warning(
                        "Skipping archive window for %s; requested dates %s -> %s exceed archive availability",
                        facility.code,
                        cursor,
                        end,
                    )
                    break
                cursor = archive_cutoff + dt.timedelta(days=1)
                continue

        windows.append((cursor, window_end))
        cursor = window_end + dt.timedelta(days=1)

    if not windows:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []

    def process_window(window: Tuple[dt.date, dt.date]) -> pd.DataFrame:
        window_start, window_end = window
        endpoint_name = strategy.select_endpoint(window_start, archive_cutoff)
        params = {
            "latitude": f"{facility.latitude:.5f}",
            "longitude": f"{facility.longitude:.5f}",
            "hourly": hourly_variables,
            "start_date": window_start.isoformat(),
            "end_date": window_end.isoformat(),
            "timezone": timezone,
        }

        limiter.wait()
        url = WEATHER_ENDPOINTS[endpoint_name]

        try:
            payload = _request_json(url, params, retries=max_retries, backoff=retry_backoff)
        except OpenMeteoAPIError as exc:
            # Fallback to forecast if archive fails in auto mode
            if endpoint_name == "archive" and endpoint_preference == "auto":
                LOGGER.info(
                    "Archive request failed for %s (%s -> %s); retrying forecast. Reason: %s",
                    facility.code,
                    params["start_date"],
                    params["end_date"],
                    exc,
                )
                fallback_end = min(
                    window_start + dt.timedelta(days=FORECAST_MAX_DAYS - 1),
                    window_end,
                    end,
                )
                if fallback_end < window_start:
                    return pd.DataFrame()
                params["end_date"] = fallback_end.isoformat()
                limiter.wait()
                try:
                    payload = _request_json(
                        WEATHER_ENDPOINTS["forecast"],
                        params,
                        retries=max_retries,
                        backoff=retry_backoff,
                    )
                except OpenMeteoAPIError as inner_exc:
                    LOGGER.error(
                        "Weather fallback request failed for %s (%s -> %s): %s",
                        facility.code,
                        params["start_date"],
                        params["end_date"],
                        inner_exc,
                    )
                    return pd.DataFrame()
            else:
                LOGGER.error(
                    "Weather request failed for %s (%s -> %s) via %s: %s",
                    facility.code,
                    params["start_date"],
                    params["end_date"],
                    endpoint_name,
                    exc,
                )
                return pd.DataFrame()

        frame = _parse_hourly(payload)
        if frame.empty:
            return frame
        frame.insert(0, "facility_code", facility.code)
        frame.insert(1, "facility_name", facility.name)
        frame.insert(2, "latitude", facility.latitude)
        frame.insert(3, "longitude", facility.longitude)
        return frame

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_window, window): window for window in windows}
        for future in as_completed(futures):
            window_start, window_end = futures[future]
            try:
                frame = future.result()
            except (OpenMeteoAPIError, requests.RequestException, ValueError) as exc:
                LOGGER.error(
                    "Weather window task failed for %s (%s -> %s): %s",
                    facility.code,
                    window_start,
                    window_end,
                    exc,
                )
                continue
            except Exception as exc:  # pragma: no cover - unexpected errors
                LOGGER.exception(
                    "Unexpected error in weather window task for %s (%s -> %s)",
                    facility.code,
                    window_start,
                    window_end,
                )
                continue
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_air_quality_dataframe(
    facility: FacilityLocation,
    *,
    start: dt.date,
    end: dt.date,
    chunk_days: int = AIR_QUALITY_MAX_DAYS,
    hourly_variables: str = DEFAULT_AIR_VARS,
    timezone: str = "UTC",
    limiter: Optional[RateLimiter] = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    max_workers: int = 4,
) -> pd.DataFrame:
    """
    Fetch hourly air quality data for a facility from the Open-Meteo API.

    Args:
        facility: Location metadata for the facility.
        start: Start date for the data range.
        end: End date for the data range.
        chunk_days: Maximum days per API request (default: AIR_QUALITY_MAX_DAYS).
        hourly_variables: Comma-separated list of air quality variables.
        timezone: Timezone for the returned data (default: "UTC").
        limiter: Optional rate limiter for API requests.
        max_retries: Maximum retry attempts for failed requests.
        retry_backoff: Initial backoff delay in seconds.
        max_workers: Maximum concurrent request threads.

    Returns:
        DataFrame with hourly air quality data, or empty DataFrame on failure.
    """
    limiter = limiter or RateLimiter(0)
    max_workers = max(1, max_workers)

    windows = list(_chunk_dates(start, end, chunk_days))
    if not windows:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []

    def process_window(window: Tuple[dt.date, dt.date]) -> pd.DataFrame:
        chunk_start, chunk_end = window
        params = {
            "latitude": f"{facility.latitude:.5f}",
            "longitude": f"{facility.longitude:.5f}",
            "hourly": hourly_variables,
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
            "timezone": timezone,
        }
        limiter.wait()
        try:
            payload = _request_json(
                AIR_QUALITY_ENDPOINT, params, retries=max_retries, backoff=retry_backoff
            )
        except OpenMeteoAPIError as exc:
            LOGGER.error(
                "Air quality request failed for %s (%s -> %s): %s",
                facility.code,
                params["start_date"],
                params["end_date"],
                exc,
            )
            return pd.DataFrame()
        frame = _parse_hourly(payload)
        if frame.empty:
            return frame
        frame.insert(0, "facility_code", facility.code)
        frame.insert(1, "facility_name", facility.name)
        frame.insert(2, "latitude", facility.latitude)
        frame.insert(3, "longitude", facility.longitude)
        return frame

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_window, window): window for window in windows}
        for future in as_completed(futures):
            window_start, window_end = futures[future]
            try:
                frame = future.result()
            except (OpenMeteoAPIError, requests.RequestException, ValueError) as exc:
                LOGGER.error(
                    "Air quality window task failed for %s (%s -> %s): %s",
                    facility.code,
                    window_start,
                    window_end,
                    exc,
                )
                continue
            except Exception as exc:  # pragma: no cover - unexpected errors
                LOGGER.exception(
                    "Unexpected error in air quality window task for %s (%s -> %s)",
                    facility.code,
                    window_start,
                    window_end,
                )
                continue
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


__all__ = [
    "EndpointStrategy",
    "AutoEndpointStrategy",
    "FixedEndpointStrategy",
    "create_endpoint_strategy",
    "fetch_weather_dataframe",
    "fetch_air_quality_dataframe",
]
