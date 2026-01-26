"""Tests for facility utilities module."""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pv_lakehouse.etl.utils.facility_utils import (
    load_facility_locations,
    resolve_facility_codes,
)


class TestResolveFacilityCodes:
    """Tests for resolve_facility_codes function."""

    def test_resolve_with_codes(self) -> None:
        """Test resolving provided facility codes."""
        result = resolve_facility_codes("YARRANL1, GULLRWF1")
        assert result == ["YARRANL1", "GULLRWF1"]

    def test_resolve_with_lowercase_codes(self) -> None:
        """Test lowercase codes are converted to uppercase."""
        result = resolve_facility_codes("yarranl1, gullrwf1")
        assert result == ["YARRANL1", "GULLRWF1"]

    def test_resolve_mixed_case(self) -> None:
        """Test mixed case codes are normalized."""
        result = resolve_facility_codes("YarrAnL1, gullRWF1")
        assert result == ["YARRANL1", "GULLRWF1"]

    def test_resolve_no_uppercase(self) -> None:
        """Test disabling uppercase conversion."""
        result = resolve_facility_codes("yarranl1, GULLRWF1", uppercase=False)
        assert result == ["yarranl1", "GULLRWF1"]

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_resolve_none_uses_defaults(self, mock_oe: MagicMock) -> None:
        """Test None input falls back to default codes."""
        mock_oe.load_default_facility_codes.return_value = ["DEFAULT1", "DEFAULT2"]
        result = resolve_facility_codes(None)
        assert result == ["DEFAULT1", "DEFAULT2"]
        mock_oe.load_default_facility_codes.assert_called_once()

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_resolve_empty_string_uses_defaults(self, mock_oe: MagicMock) -> None:
        """Test empty string falls back to default codes."""
        mock_oe.load_default_facility_codes.return_value = ["DEFAULT1", "DEFAULT2"]
        result = resolve_facility_codes("")
        assert result == ["DEFAULT1", "DEFAULT2"]
        mock_oe.load_default_facility_codes.assert_called_once()

    def test_resolve_whitespace_handling(self) -> None:
        """Test whitespace in codes is handled correctly."""
        result = resolve_facility_codes("  YARRANL1  ,  GULLRWF1  ")
        assert result == ["YARRANL1", "GULLRWF1"]

    def test_resolve_empty_items_filtered(self) -> None:
        """Test empty items are filtered out."""
        result = resolve_facility_codes("YARRANL1,,GULLRWF1,  ,")
        assert result == ["YARRANL1", "GULLRWF1"]


class TestLoadFacilityLocations:
    """Tests for load_facility_locations function."""

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_basic(self, mock_oe: MagicMock) -> None:
        """Test basic facility location loading."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1", "GULLRWF1"],
            "facility_name": ["Yarranlea Solar Farm", "Gullrook Wind Farm"],
            "location_lat": [-27.5, -28.0],
            "location_lng": [151.0, 152.0],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        result = load_facility_locations(["YARRANL1", "GULLRWF1"])

        assert len(result) == 2
        assert result[0].code == "YARRANL1"
        assert result[0].latitude == -27.5
        assert result[1].code == "GULLRWF1"
        assert result[1].longitude == 152.0

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_empty_response_raises(self, mock_oe: MagicMock) -> None:
        """Test empty API response raises ValueError."""
        mock_oe.fetch_facilities_dataframe.return_value = pd.DataFrame()

        with pytest.raises(ValueError, match="No facilities returned"):
            load_facility_locations(["YARRANL1"])

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_missing_coordinates_raises(self, mock_oe: MagicMock) -> None:
        """Test missing coordinates raises ValueError."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1"],
            "facility_name": ["Yarranlea Solar Farm"],
            "location_lat": [None],
            "location_lng": [None],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        with pytest.raises(ValueError, match="missing latitude/longitude"):
            load_facility_locations(["YARRANL1"])

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_filters_by_code(self, mock_oe: MagicMock) -> None:
        """Test filtering by facility codes."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1", "GULLRWF1", "OTHER1"],
            "facility_name": ["Yarranlea", "Gullrook", "Other"],
            "location_lat": [-27.5, -28.0, -29.0],
            "location_lng": [151.0, 152.0, 153.0],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        result = load_facility_locations(["YARRANL1"])

        assert len(result) == 1
        assert result[0].code == "YARRANL1"

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_uses_code_as_name_if_missing(self, mock_oe: MagicMock) -> None:
        """Test facility code used as name when name is missing."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1"],
            "facility_name": [None],
            "location_lat": [-27.5],
            "location_lng": [151.0],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        result = load_facility_locations(["YARRANL1"])

        assert result[0].name == "YARRANL1"

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_with_api_key(self, mock_oe: MagicMock) -> None:
        """Test API key is passed to fetch function."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1"],
            "facility_name": ["Yarranlea"],
            "location_lat": [-27.5],
            "location_lng": [151.0],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        load_facility_locations(["YARRANL1"], api_key="test-key")

        mock_oe.fetch_facilities_dataframe.assert_called_once()
        call_kwargs = mock_oe.fetch_facilities_dataframe.call_args.kwargs
        assert call_kwargs["api_key"] == "test-key"

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_custom_filters(self, mock_oe: MagicMock) -> None:
        """Test custom network/status/fueltech filters."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1"],
            "facility_name": ["Yarranlea"],
            "location_lat": [-27.5],
            "location_lng": [151.0],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        load_facility_locations(
            ["YARRANL1"],
            networks=["NEM"],
            statuses=["operating"],
            fueltechs=["wind"],
        )

        call_kwargs = mock_oe.fetch_facilities_dataframe.call_args.kwargs
        assert call_kwargs["networks"] == ["NEM"]
        assert call_kwargs["statuses"] == ["operating"]
        assert call_kwargs["fueltechs"] == ["wind"]

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_uppercase_conversion(self, mock_oe: MagicMock) -> None:
        """Test input codes are converted to uppercase."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1"],
            "facility_name": ["Yarranlea"],
            "location_lat": [-27.5],
            "location_lng": [151.0],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        load_facility_locations(["yarranl1"])

        call_kwargs = mock_oe.fetch_facilities_dataframe.call_args.kwargs
        assert call_kwargs["selected_codes"] == ["YARRANL1"]

    @patch("pv_lakehouse.etl.utils.facility_utils.openelectricity")
    def test_load_empty_codes_filtered(self, mock_oe: MagicMock) -> None:
        """Test empty codes are filtered out."""
        mock_df = pd.DataFrame({
            "facility_code": ["YARRANL1"],
            "facility_name": ["Yarranlea"],
            "location_lat": [-27.5],
            "location_lng": [151.0],
        })
        mock_oe.fetch_facilities_dataframe.return_value = mock_df

        load_facility_locations(["YARRANL1", "", "  "])

        call_kwargs = mock_oe.fetch_facilities_dataframe.call_args.kwargs
        assert call_kwargs["selected_codes"] == ["YARRANL1"]
