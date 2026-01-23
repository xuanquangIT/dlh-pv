from __future__ import annotations
import pytest
from pv_lakehouse.etl.clients.openelectricity.models import (
    ClientConfig,
    DateRange,
    Facility,
    FacilityMetadata,
    FacilitySummary,
    Location,
    TimeseriesRow,
    Unit,
)
from datetime import datetime

class TestLocation:
    def test_location_with_values(self):
        loc = Location(lat=-33.8688, lng=151.2093)
        assert loc.lat == -33.8688
        assert loc.lng == 151.2093

    def test_location_empty(self):
        loc = Location()
        assert loc.lat is None
        assert loc.lng is None

class TestUnit:
    def test_unit_basic(self):
        unit = Unit(
            code="UNIT1",
            name="Solar Unit 1",
            status_id="operating",
            fueltech_id="solar_utility",
            capacity_mw=100.5,
        )
        assert unit.code == "UNIT1"
        assert unit.name == "Solar Unit 1"
        assert unit.status_id == "operating"
        assert unit.fueltech_id == "solar_utility"
        assert unit.capacity_mw == 100.5

    def test_unit_coerce_capacity_from_string(self):
        unit = Unit(capacity_mw="50.5")  # type: ignore
        assert unit.capacity_mw == 50.5

    def test_unit_coerce_capacity_from_invalid(self):
        unit = Unit(capacity_mw="invalid")  # type: ignore
        assert unit.capacity_mw is None

    def test_unit_with_alias(self):
        data = {"code": "U1", "capacity": 100.0, "status": "operating", "fueltech": "solar"}
        unit = Unit.model_validate(data)
        assert unit.code == "U1"
        assert unit.capacity_mw == 100.0
        assert unit.status_id == "operating"
        assert unit.fueltech_id == "solar"


class TestFacility:
    def test_facility_basic(self):
        facility = Facility(
            code="FAC1",
            name="Solar Farm 1",
            network_id="NEM",
            network_region="NSW1",
        )
        assert facility.code == "FAC1"
        assert facility.name == "Solar Farm 1"
        assert facility.network_id == "NEM"
        assert facility.network_region == "NSW1"
        assert facility.units == []

    def test_facility_with_units(self):
        facility = Facility(
            code="FAC1",
            name="Solar Farm 1",
            network_id="NEM",
            units=[
                Unit(code="U1", capacity_mw=50.0),
                Unit(code="U2", capacity_mw=50.0),
            ],
        )
        assert len(facility.units) == 2
        assert facility.units[0].code == "U1"
        assert facility.units[1].code == "U2"

    def test_facility_with_location(self):
        facility = Facility(
            code="FAC1",
            name="Solar Farm 1",
            network_id="NEM",
            location=Location(lat=-33.0, lng=151.0),
        )
        assert facility.location is not None
        assert facility.location.lat == -33.0
        assert facility.location.lng == 151.0


class TestFacilityMetadata:
    def test_facility_metadata_basic(self):
        meta = FacilityMetadata(
            code="FAC1",
            name="Test Facility",
            network_id="NEM",
        )
        assert meta.code == "FAC1"
        assert meta.name == "Test Facility"
        assert meta.network_id == "NEM"
        assert meta.units == []

    def test_facility_metadata_requires_code(self):
        with pytest.raises(Exception):
            FacilityMetadata(name="Test", network_id="NEM")  # type: ignore


class TestFacilitySummary:
    def test_facility_summary_defaults(self):
        summary = FacilitySummary()
        assert summary.unit_count == 0
        assert summary.facility_code is None
        assert summary.total_capacity_mw is None

    def test_facility_summary_full(self):
        summary = FacilitySummary(
            facility_code="FAC1",
            facility_name="Test Facility",
            network_id="NEM",
            network_region="NSW1",
            unit_count=5,
            total_capacity_mw=250.0,
            unit_fueltech_summary="solar_utility:5",
            unit_status_summary="operating:5",
        )
        assert summary.facility_code == "FAC1"
        assert summary.unit_count == 5
        assert summary.total_capacity_mw == 250.0


class TestTimeseriesRow:
    def test_timeseries_row_basic(self):
        row = TimeseriesRow(
            network_code="NEM",
            facility_code="FAC1",
            metric="energy",
            interval="1h",
            interval_start="2024-01-01T00:00:00",
            value=123.45,
        )
        assert row.network_code == "NEM"
        assert row.facility_code == "FAC1"
        assert row.metric == "energy"
        assert row.value == 123.45


class TestClientConfig:
    def test_default_config(self):
        config = ClientConfig()
        assert config.base_url == "https://api.openelectricity.org.au/v4"
        assert config.timeout == 120
        assert config.max_retries == 3
        assert config.retry_min_wait == 1.0
        assert config.retry_max_wait == 60.0

    def test_custom_config(self):
        config = ClientConfig(
            base_url="https://custom.api.com/v1",
            timeout=60,
            max_retries=5,
        )
        assert config.base_url == "https://custom.api.com/v1"
        assert config.timeout == 60
        assert config.max_retries == 5


class TestDateRange:
    def test_valid_date_range(self):
        dr = DateRange(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
        )
        assert dr.start == datetime(2024, 1, 1)
        assert dr.end == datetime(2024, 1, 31)

    def test_invalid_date_range(self):
        """Test that end before start raises error."""
        with pytest.raises(ValueError, match="end must be after start"):
            DateRange(
                start=datetime(2024, 1, 31),
                end=datetime(2024, 1, 1),
            )
