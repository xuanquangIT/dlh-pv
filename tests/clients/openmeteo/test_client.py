"""Unit tests for OpenMeteo client functions."""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pv_lakehouse.etl.clients.openmeteo import (
    FacilityLocation,
    RateLimiter,
)
from pv_lakehouse.etl.clients.openmeteo.client import (
    _chunk_dates,
    _parse_hourly,
    fetch_air_quality_dataframe,
    fetch_weather_dataframe,
)
from pv_lakehouse.etl.clients.openmeteo.constants import (
    AIR_QUALITY_ENDPOINT,
    AIR_QUALITY_MAX_DAYS,
    FORECAST_MAX_DAYS,
    WEATHER_ENDPOINTS,
)


@pytest.fixture
def sample_facility() -> FacilityLocation:
    """Create a sample facility for testing."""
    return FacilityLocation(
        code="TEST01",
        name="Test Facility",
        latitude=-33.8688,
        longitude=151.2093,
    )


class TestParseHourly:
    """Tests for _parse_hourly helper function."""

    def test_parses_valid_hourly_data(self) -> None:
        """Should parse hourly data and rename 'time' to 'date'."""
        payload = {
            "hourly": {
                "time": ["2025-01-01T00:00", "2025-01-01T01:00"],
                "temperature_2m": [20.5, 21.0],
            }
        }
        result = _parse_hourly(payload)
        assert "date" in result.columns
        assert "time" not in result.columns
        assert len(result) == 2

    def test_returns_empty_for_missing_hourly(self) -> None:
        """Should return empty DataFrame if no hourly data."""
        assert _parse_hourly({}).empty
        assert _parse_hourly({"hourly": {}}).empty
        assert _parse_hourly({"hourly": None}).empty


class TestChunkDates:
    """Tests for _chunk_dates helper function."""

    def test_single_chunk_for_small_range(self) -> None:
        """Small date ranges should produce single chunk."""
        chunks = list(_chunk_dates(dt.date(2025, 1, 1), dt.date(2025, 1, 5), chunk_days=30))
        assert len(chunks) == 1
        assert chunks[0] == (dt.date(2025, 1, 1), dt.date(2025, 1, 5))

    def test_multiple_chunks_for_large_range(self) -> None:
        """Large date ranges should be split into multiple chunks."""
        chunks = list(_chunk_dates(dt.date(2025, 1, 1), dt.date(2025, 1, 31), chunk_days=10))
        assert len(chunks) == 4
        assert chunks[0] == (dt.date(2025, 1, 1), dt.date(2025, 1, 10))
        assert chunks[1] == (dt.date(2025, 1, 11), dt.date(2025, 1, 20))
        assert chunks[2] == (dt.date(2025, 1, 21), dt.date(2025, 1, 30))
        assert chunks[3] == (dt.date(2025, 1, 31), dt.date(2025, 1, 31))

    def test_empty_for_invalid_range(self) -> None:
        """Should produce no chunks if start > end."""
        chunks = list(_chunk_dates(dt.date(2025, 1, 10), dt.date(2025, 1, 1), chunk_days=30))
        assert len(chunks) == 0


class TestFetchWeatherDataframe:
    """Tests for fetch_weather_dataframe function."""

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_returns_dataframe_with_facility_columns(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should return DataFrame with facility metadata columns."""
        mock_request.return_value = {
            "hourly": {
                "time": ["2025-01-01T00:00"],
                "temperature_2m": [20.5],
            }
        }
        result = fetch_weather_dataframe(
            sample_facility,
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 1),
            max_workers=1,
        )
        assert "facility_code" in result.columns
        assert "facility_name" in result.columns
        assert "latitude" in result.columns
        assert "longitude" in result.columns
        assert result["facility_code"].iloc[0] == "TEST01"

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_uses_archive_endpoint_for_old_dates(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should use archive endpoint for dates older than FORECAST_MAX_DAYS."""
        mock_request.return_value = {"hourly": {"time": [], "temperature_2m": []}}
        
        old_date = dt.date.today() - dt.timedelta(days=FORECAST_MAX_DAYS + 10)
        fetch_weather_dataframe(
            sample_facility,
            start=old_date,
            end=old_date,
            max_workers=1,
        )
        
        # Check that archive endpoint was called
        call_args = mock_request.call_args_list[0]
        assert WEATHER_ENDPOINTS["archive"] in str(call_args)

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_uses_forecast_endpoint_for_recent_dates(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should use forecast endpoint for recent dates."""
        mock_request.return_value = {"hourly": {"time": [], "temperature_2m": []}}
        
        recent_date = dt.date.today()
        fetch_weather_dataframe(
            sample_facility,
            start=recent_date,
            end=recent_date,
            max_workers=1,
        )
        
        # Check that forecast endpoint was called
        call_args = mock_request.call_args_list[0]
        assert WEATHER_ENDPOINTS["forecast"] in str(call_args)


class TestFetchAirQualityDataframe:
    """Tests for fetch_air_quality_dataframe function."""

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_returns_dataframe_with_facility_columns(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should return DataFrame with facility metadata columns."""
        mock_request.return_value = {
            "hourly": {
                "time": ["2025-01-01T00:00"],
                "pm2_5": [15.0],
                "pm10": [25.0],
            }
        }
        result = fetch_air_quality_dataframe(
            sample_facility,
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 1),
            max_workers=1,
        )
        assert "facility_code" in result.columns
        assert "facility_name" in result.columns
        assert result["facility_code"].iloc[0] == "TEST01"
        assert "pm2_5" in result.columns
        assert "pm10" in result.columns

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_uses_air_quality_endpoint(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should always use the air quality endpoint."""
        mock_request.return_value = {"hourly": {"time": [], "pm2_5": []}}
        
        fetch_air_quality_dataframe(
            sample_facility,
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 1),
            max_workers=1,
        )
        
        # Check that air quality endpoint was called
        call_args = mock_request.call_args_list[0]
        assert AIR_QUALITY_ENDPOINT in str(call_args)

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_chunks_large_date_ranges(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should split large date ranges into chunks."""
        mock_request.return_value = {"hourly": {"time": [], "pm2_5": []}}
        
        # Request 30 days, which should be split with AIR_QUALITY_MAX_DAYS
        fetch_air_quality_dataframe(
            sample_facility,
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 30),
            chunk_days=AIR_QUALITY_MAX_DAYS,
            max_workers=1,
        )
        
        # Should have made multiple calls (30 days / 14 days = 3 chunks)
        assert mock_request.call_count >= 2

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_returns_empty_on_api_error(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should return empty DataFrame on API error (logs error)."""
        from pv_lakehouse.etl.clients.openmeteo.models import OpenMeteoAPIError
        
        mock_request.side_effect = OpenMeteoAPIError("API Error", status_code=500)
        
        result = fetch_air_quality_dataframe(
            sample_facility,
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 1),
            max_workers=1,
        )
        assert result.empty

    @patch("pv_lakehouse.etl.clients.openmeteo.client._request_json")
    def test_respects_rate_limiter(
        self, mock_request: MagicMock, sample_facility: FacilityLocation
    ) -> None:
        """Should call rate limiter wait() before each request."""
        mock_request.return_value = {"hourly": {"time": [], "pm2_5": []}}
        
        limiter = MagicMock(spec=RateLimiter)
        
        fetch_air_quality_dataframe(
            sample_facility,
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 1),
            limiter=limiter,
            max_workers=1,
        )
        
        limiter.wait.assert_called()
