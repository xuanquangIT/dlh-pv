"""Integration tests for OpenElectricity API client.

These tests require a valid API key and network connectivity.
Run with: pytest tests/clients/openelectricity/test_integration.py -v --run-integration
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from pv_lakehouse.etl.clients.openelectricity import (
    OpenElectricityClient,
    fetch_facilities_dataframe,
    fetch_facility_timeseries_dataframe,
    load_default_facility_codes,
)


# Skip integration tests unless explicitly enabled
pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION_TESTS"),
    reason="Integration tests require --run-integration flag and API key",
)


def pytest_addoption(parser):
    """Add custom pytest option for integration tests."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires API key)",
    )


@pytest.fixture
def api_key():
    """Get API key from environment."""
    key = os.environ.get("OPENELECTRICITY_API_KEY")
    if not key:
        pytest.skip("OPENELECTRICITY_API_KEY not set")
    return key


@pytest.fixture
def client(api_key):
    """Create authenticated client."""
    return OpenElectricityClient(api_key=api_key)


class TestFacilitiesIntegration:
    """Integration tests for facility fetching."""

    def test_fetch_all_solar_facilities(self, client):
        """Test fetching all solar utility facilities."""
        df = client.fetch_facilities(
            networks=["NEM", "WEM"],
            statuses=["operating"],
            fueltechs=["solar_utility"],
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "facility_code" in df.columns
        assert "network_id" in df.columns
        print(f"Found {len(df)} solar utility facilities")

    def test_fetch_facilities_by_region(self, client):
        """Test fetching facilities filtered by region."""
        df = client.fetch_facilities(
            networks=["NEM"],
            region="NSW1",
        )

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert all(df["network_region"] == "NSW1")

    def test_fetch_specific_facilities(self, client):
        """Test fetching specific facility codes."""
        # First get some facility codes
        all_df = client.fetch_facilities()
        if all_df.empty:
            pytest.skip("No facilities available")

        sample_codes = all_df["facility_code"].head(3).tolist()

        df = client.fetch_facilities(selected_codes=sample_codes)
        assert len(df) <= len(sample_codes)


class TestTimeseriesIntegration:
    """Integration tests for timeseries fetching."""

    def test_fetch_timeseries_recent(self, client):
        """Test fetching recent timeseries data."""
        # First get a facility code
        facilities_df = client.fetch_facilities()
        if facilities_df.empty:
            pytest.skip("No facilities available")

        facility_code = facilities_df["facility_code"].iloc[0]

        df = client.fetch_timeseries(
            facility_codes=[facility_code],
            metrics=["energy"],
            interval="1h",
        )

        assert isinstance(df, pd.DataFrame)
        print(f"Fetched {len(df)} timeseries records for {facility_code}")

    def test_fetch_timeseries_date_range(self, client):
        """Test fetching timeseries with specific date range."""
        facilities_df = client.fetch_facilities()
        if facilities_df.empty:
            pytest.skip("No facilities available")

        facility_code = facilities_df["facility_code"].iloc[0]

        df = client.fetch_timeseries(
            facility_codes=[facility_code],
            metrics=["energy"],
            interval="1h",
            date_start="2024-01-01T00:00:00",
            date_end="2024-01-02T00:00:00",
        )

        assert isinstance(df, pd.DataFrame)


class TestBackwardCompatibleFunctions:
    """Integration tests for backward-compatible module functions."""

    def test_fetch_facilities_dataframe_legacy(self, api_key):
        """Test legacy fetch_facilities_dataframe function."""
        df = fetch_facilities_dataframe(api_key=api_key)
        assert isinstance(df, pd.DataFrame)

    def test_fetch_facility_timeseries_dataframe_legacy(self, api_key):
        """Test legacy fetch_facility_timeseries_dataframe function."""
        # Get a facility code first
        facilities_df = fetch_facilities_dataframe(api_key=api_key)
        if facilities_df.empty:
            pytest.skip("No facilities available")

        facility_code = facilities_df["facility_code"].iloc[0]

        df = fetch_facility_timeseries_dataframe(
            api_key=api_key,
            facility_codes=[facility_code],
            metrics=["energy"],
            interval="1h",
        )
        assert isinstance(df, pd.DataFrame)


class TestFacilityCodeLoader:
    """Integration tests for facility code loader."""

    def test_load_default_facility_codes(self):
        """Test loading default facility codes from JS file."""
        try:
            codes = load_default_facility_codes()
            assert isinstance(codes, list)
            assert len(codes) > 0
            assert all(isinstance(code, str) for code in codes)
            print(f"Loaded {len(codes)} default facility codes")
        except FileNotFoundError:
            pytest.skip("facilities.js not found")
