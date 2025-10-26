"""Client helpers for fetching Open-Meteo datasets into pandas DataFrames."""

from __future__ import annotations

import datetime as dt
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

LOGGER = logging.getLogger(__name__)

WEATHER_ENDPOINTS = {
    "archive": "https://archive-api.open-meteo.com/v1/archive",
    "forecast": "https://api.open-meteo.com/v1/forecast",
}
FORECAST_MAX_DAYS = 16
AIR_QUALITY_MAX_DAYS = 14
AIR_QUALITY_ENDPOINT = "https://air-quality-api.open-meteo.com/v1/air-quality"
DEFAULT_WEATHER_VARS = (
    "shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance," \
    "terrestrial_radiation,temperature_2m,dew_point_2m,cloud_cover,cloud_cover_low," \
    "cloud_cover_mid,cloud_cover_high,precipitation,is_day,sunshine_duration," \
    "total_column_integrated_water_vapour,boundary_layer_height,wind_gusts_10m," \
    "wet_bulb_temperature_2m,wind_speed_10m,wind_direction_10m,pressure_msl"
)
DEFAULT_AIR_VARS = (
    "pm2_5,pm10,dust,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide,uv_index,uv_index_clear_sky"
)
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BACKOFF = 2.0


@dataclass
class FacilityLocation:
    """Minimal facility metadata for location-based requests."""

    code: str
    name: str
    latitude: float
    longitude: float


class RateLimiter:
    def __init__(self, max_requests_per_minute: float) -> None:
        self._interval = 0.0
        if max_requests_per_minute > 0:
            self._interval = 60.0 / max_requests_per_minute
        self._last_request: Optional[float] = None
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self._interval <= 0:
            return
        import time

        with self._lock:
            now = time.monotonic()
            if self._last_request is not None:
                delay = self._last_request + self._interval - now
                if delay > 0:
                    time.sleep(delay)
                    now = time.monotonic()
            self._last_request = now


def _augment_http_error(exc: requests.HTTPError) -> requests.HTTPError:
    response = exc.response
    detail: Optional[str] = None
    if response is not None:
        try:
            body = response.json()
            if isinstance(body, dict):
                detail = body.get("reason") or body.get("error")
        except Exception:  # noqa: BLE001 - best effort
            detail = response.text
    message = f"{exc}"
    if detail:
        message = f"{message} (details: {detail})"
    return requests.HTTPError(message, response=response)


def _request_json(url: str, params: Dict[str, str], *, retries: int, backoff: float) -> Dict:
    attempt = 0
    delay = max(backoff, 0.0)
    last_exc: Optional[requests.RequestException] = None
    while attempt < max(1, retries):
        attempt += 1
        try:
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:  # pragma: no cover - network / API dependency
            formatted = _augment_http_error(exc)
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and status < 500 and status not in {408, 429}:
                raise formatted
            last_exc = formatted
        except requests.RequestException as exc:  # pragma: no cover - network / API dependency
            last_exc = exc

        if attempt >= max(1, retries):
            break
        if delay > 0:
            import time

            time.sleep(delay)
            delay *= 2

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Open-Meteo request failed without raising an exception")


def _parse_hourly(payload: Dict) -> pd.DataFrame:
    hourly = payload.get("hourly") or {}
    if not hourly:
        return pd.DataFrame()
    frame = pd.DataFrame(hourly)
    if "time" in frame.columns:
        frame = frame.rename(columns={"time": "date"})
    return frame


def _chunk_dates(start: dt.date, end: dt.date, chunk_days: int) -> Iterable[tuple[dt.date, dt.date]]:
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
    limiter = limiter or RateLimiter(0)
    max_workers = max(1, max_workers)
    today = dt.date.today()
    archive_cutoff = today - dt.timedelta(days=FORECAST_MAX_DAYS)

    windows: List[Tuple[dt.date, dt.date]] = []
    cursor = start
    while cursor <= end:
        endpoint_name = _resolve_weather_endpoint(endpoint_preference, cursor, archive_cutoff)
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
        endpoint_name = _resolve_weather_endpoint(endpoint_preference, window_start, archive_cutoff)
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
        except requests.HTTPError as exc:
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
                        WEATHER_ENDPOINTS["forecast"], params, retries=max_retries, backoff=retry_backoff
                    )
                except requests.RequestException as inner_exc:
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
        except requests.RequestException as exc:
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
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.error(
                    "Weather window task failed for %s (%s -> %s): %s",
                    facility.code,
                    window_start,
                    window_end,
                    exc,
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
            payload = _request_json(AIR_QUALITY_ENDPOINT, params, retries=max_retries, backoff=retry_backoff)
        except requests.RequestException as exc:
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
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.error(
                    "Air quality window task failed for %s (%s -> %s): %s",
                    facility.code,
                    window_start,
                    window_end,
                    exc,
                )
                continue
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _resolve_weather_endpoint(preference: str, current_date: dt.date, archive_cutoff: dt.date) -> str:
    if preference in WEATHER_ENDPOINTS:
        return preference
    if preference != "auto":
        raise ValueError(f"Unsupported weather endpoint preference: {preference}")
    if current_date <= archive_cutoff:
        return "archive"
    return "forecast"


__all__ = [
    "FacilityLocation",
    "RateLimiter",
    "fetch_weather_dataframe",
    "fetch_air_quality_dataframe",
]
