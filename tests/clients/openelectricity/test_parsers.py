"""Unit tests for OpenElectricity parsers."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from pv_lakehouse.etl.clients.openelectricity.parsers import (
    as_str,
    build_unit_to_facility_map,
    chunk_date_range,
    extract_max_days_from_error,
    flatten_timeseries,
    format_naive_datetime,
    get_timezone_id,
    get_timezone_offset_hours,
    parse_naive_datetime,
    safe_float,
    summarize_facility,
)
from pv_lakehouse.etl.clients.openelectricity.models import FacilityMetadata


class TestTimezoneHelpers:
    """Tests for timezone helper functions."""

    def test_get_timezone_offset_nem(self):
        """Test NEM timezone offset."""
        assert get_timezone_offset_hours("NEM") == 10

    def test_get_timezone_offset_wem(self):
        """Test WEM timezone offset."""
        assert get_timezone_offset_hours("WEM") == 8

    def test_get_timezone_offset_unknown(self):
        """Test unknown network defaults to 10."""
        assert get_timezone_offset_hours("UNKNOWN") == 10

    def test_get_timezone_id_nem(self):
        """Test NEM timezone ID."""
        assert get_timezone_id("NEM") == "Australia/Brisbane"

    def test_get_timezone_id_wem(self):
        """Test WEM timezone ID."""
        assert get_timezone_id("WEM") == "Australia/Perth"

    def test_get_timezone_id_unknown(self):
        """Test unknown network defaults to Brisbane."""
        assert get_timezone_id("UNKNOWN") == "Australia/Brisbane"


class TestDateTimeHelpers:
    """Tests for date/time helper functions."""

    def test_parse_naive_datetime_valid(self):
        """Test parsing valid datetime."""
        result = parse_naive_datetime("2024-01-15T14:30:00")
        assert result == dt.datetime(2024, 1, 15, 14, 30, 0)
        assert result.tzinfo is None

    def test_parse_naive_datetime_invalid_format(self):
        """Test parsing invalid format raises error."""
        with pytest.raises(ValueError, match="Invalid datetime"):
            parse_naive_datetime("not-a-date")

    def test_parse_naive_datetime_with_timezone(self):
        """Test that timezone-aware datetime raises error."""
        with pytest.raises(ValueError, match="timezone naive"):
            parse_naive_datetime("2024-01-15T14:30:00+10:00")

    def test_format_naive_datetime(self):
        """Test formatting datetime."""
        result = format_naive_datetime(dt.datetime(2024, 1, 15, 14, 30, 0))
        assert result == "2024-01-15T14:30:00"


class TestChunkDateRange:
    """Tests for chunk_date_range function."""

    def test_no_chunking_needed(self):
        """Test when no chunking is needed."""
        start = dt.datetime(2024, 1, 1)
        end = dt.datetime(2024, 1, 7)
        result = chunk_date_range(start, end, None)
        assert len(result) == 1
        assert result[0] == (start, end)

    def test_chunking_with_max_days(self):
        """Test chunking with max_days."""
        start = dt.datetime(2024, 1, 1)
        end = dt.datetime(2024, 1, 31)
        result = chunk_date_range(start, end, 10)
        assert len(result) == 3
        # First chunk: Jan 1-11
        assert result[0][0] == start
        assert result[0][1] == dt.datetime(2024, 1, 11)
        # Second chunk: Jan 11-21
        assert result[1][0] == dt.datetime(2024, 1, 11)
        assert result[1][1] == dt.datetime(2024, 1, 21)
        # Third chunk: Jan 21-31
        assert result[2][0] == dt.datetime(2024, 1, 21)
        assert result[2][1] == end

    def test_chunking_zero_max_days(self):
        """Test with max_days=0 means no chunking."""
        start = dt.datetime(2024, 1, 1)
        end = dt.datetime(2024, 1, 31)
        result = chunk_date_range(start, end, 0)
        assert len(result) == 1


class TestErrorParsing:
    """Tests for error message parsing."""

    def test_extract_max_days_from_error_found(self):
        """Test extracting max days from error message."""
        error = "Maximum range is 30 days for this endpoint"
        result = extract_max_days_from_error(error)
        assert result == 30

    def test_extract_max_days_from_error_not_found(self):
        """Test when pattern not found."""
        error = "Some other error message"
        result = extract_max_days_from_error(error)
        assert result is None

    def test_extract_max_days_from_empty(self):
        """Test with empty string."""
        assert extract_max_days_from_error("") is None
        assert extract_max_days_from_error(None) is None  # type: ignore


class TestValueHelpers:
    """Tests for value helper functions."""

    def test_safe_float_valid(self):
        """Test safe_float with valid numbers."""
        assert safe_float(42) == 42.0
        assert safe_float(3.14) == 3.14
        assert safe_float("123.45") == 123.45

    def test_safe_float_invalid(self):
        """Test safe_float with invalid values."""
        assert safe_float(None) == 0.0
        assert safe_float("not-a-number") == 0.0
        assert safe_float({}) == 0.0

    def test_as_str_valid(self):
        """Test as_str with valid values."""
        assert as_str("hello") == "hello"
        assert as_str(123) == "123"
        assert as_str(3.14) == "3.14"

    def test_as_str_empty(self):
        """Test as_str with empty values."""
        assert as_str(None) is None
        assert as_str("") is None


class TestSummarizeFacility:
    """Tests for summarize_facility function."""

    def test_summarize_empty_facility(self):
        """Test summarizing facility with no units."""
        facility_data = {
            "code": "FAC1",
            "name": "Test Facility",
            "network_id": "NEM",
            "network_region": "NSW1",
            "units": [],
        }
        result = summarize_facility(facility_data)
        assert result.facility_code == "FAC1"
        assert result.facility_name == "Test Facility"
        assert result.network_id == "NEM"
        assert result.unit_count == 0
        assert result.total_capacity_mw is None

    def test_summarize_facility_with_units(self):
        """Test summarizing facility with units."""
        facility_data = {
            "code": "FAC1",
            "name": "Solar Farm",
            "network_id": "NEM",
            "network_region": "NSW1",
            "location": {"lat": -33.0, "lng": 151.0},
            "units": [
                {
                    "code": "U1",
                    "capacity_mw": 50.0,
                    "status_id": "operating",
                    "fueltech_id": "solar_utility",
                    "dispatch_type": "GENERATOR",
                },
                {
                    "code": "U2",
                    "capacity_mw": 50.0,
                    "status_id": "operating",
                    "fueltech_id": "solar_utility",
                    "dispatch_type": "GENERATOR",
                },
            ],
        }
        result = summarize_facility(facility_data)
        assert result.facility_code == "FAC1"
        assert result.unit_count == 2
        assert result.total_capacity_mw == 100.0
        assert result.location_lat == -33.0
        assert result.location_lng == 151.0
        assert "solar_utility:2" in result.unit_fueltech_summary
        assert "operating:2" in result.unit_status_summary
        assert result.unit_codes == "U1,U2"


class TestBuildUnitToFacilityMap:
    """Tests for build_unit_to_facility_map function."""

    def test_empty_facilities(self):
        """Test with empty facilities dict."""
        result = build_unit_to_facility_map({})
        assert result == {}

    def test_facilities_with_units(self):
        """Test mapping units to facilities."""
        facilities = {
            "FAC1": FacilityMetadata(
                code="FAC1",
                name="Facility 1",
                network_id="NEM",
                units=[{"code": "U1"}, {"code": "U2"}],
            ),
            "FAC2": FacilityMetadata(
                code="FAC2",
                name="Facility 2",
                network_id="NEM",
                units=[{"code": "U3"}],
            ),
        }
        result = build_unit_to_facility_map(facilities)
        assert result["U1"] == "FAC1"
        assert result["U2"] == "FAC1"
        assert result["U3"] == "FAC2"


class TestFlattenTimeseries:
    """Tests for flatten_timeseries function."""

    def test_flatten_empty_payload(self):
        """Test with empty payload."""
        payload = {"data": []}
        result = flatten_timeseries(payload, {}, {})
        assert len(result) == 0

    def test_flatten_basic_timeseries(self):
        """Test flattening basic timeseries data."""
        payload = {
            "data": [
                {
                    "network_code": "NEM",
                    "metric": "energy",
                    "interval": "1h",
                    "unit": "MWh",
                    "results": [
                        {
                            "columns": {"facility_code": "FAC1", "unit_code": "U1"},
                            "data": [
                                ["2024-01-01T00:00:00", 100.0],
                                ["2024-01-01T01:00:00", 110.0],
                            ],
                        }
                    ],
                }
            ]
        }
        facilities = {
            "FAC1": FacilityMetadata(
                code="FAC1",
                name="Test Facility",
                network_id="NEM",
                network_region="NSW1",
                units=[],
            )
        }
        unit_map = {"U1": "FAC1"}

        result = flatten_timeseries(payload, facilities, unit_map)
        assert len(result) == 2
        assert result[0].network_code == "NEM"
        assert result[0].facility_code == "FAC1"
        assert result[0].facility_name == "Test Facility"
        assert result[0].metric == "energy"
        assert result[0].value == 100.0
        assert result[1].value == 110.0
