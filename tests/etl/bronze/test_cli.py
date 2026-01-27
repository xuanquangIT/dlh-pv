#!/usr/bin/env python3
"""Unit tests for Bronze CLI module."""

from __future__ import annotations

import datetime as dt

import pytest

from pv_lakehouse.etl.bronze.cli import (
    _parse_date,
    _parse_datetime,
    build_parser,
)


class TestDateParsing:
    """Test date/datetime parsing functions."""

    def test_parse_date_valid(self):
        """Test parsing valid date string."""
        result = _parse_date("2025-01-15")
        assert result == dt.date(2025, 1, 15)

    def test_parse_date_none(self):
        """Test parsing None returns None."""
        result = _parse_date(None)
        assert result is None

    def test_parse_date_invalid_format(self):
        """Test invalid date format raises error."""
        with pytest.raises(Exception):  # argparse.ArgumentTypeError
            _parse_date("15-01-2025")

    def test_parse_date_invalid_date(self):
        """Test invalid date values raise error."""
        with pytest.raises(Exception):
            _parse_date("2025-13-32")

    def test_parse_datetime_valid_iso(self):
        """Test parsing valid ISO datetime."""
        result = _parse_datetime("2025-01-15T14:30:00")
        assert result == dt.datetime(2025, 1, 15, 14, 30, 0)

    def test_parse_datetime_none(self):
        """Test parsing None returns None."""
        result = _parse_datetime(None)
        assert result is None

    def test_parse_datetime_invalid_format(self):
        """Test invalid datetime format raises error."""
        with pytest.raises(Exception):
            _parse_datetime("15/01/2025 14:30")


class TestArgumentParser:
    """Test CLI argument parser."""

    def test_build_parser_returns_parser(self):
        """Test build_parser returns ArgumentParser."""
        parser = build_parser()
        assert parser is not None
        assert parser.prog is not None

    def test_parse_facilities_dataset(self):
        """Test parsing facilities dataset."""
        parser = build_parser()
        args = parser.parse_args(["facilities"])
        assert args.dataset == "facilities"
        assert args.mode == "incremental"  # default

    def test_parse_energy_dataset(self):
        """Test parsing energy dataset."""
        parser = build_parser()
        args = parser.parse_args(["energy", "--mode", "backfill"])
        assert args.dataset == "energy"
        assert args.mode == "backfill"

    def test_parse_weather_dataset_with_dates(self):
        """Test parsing weather dataset with date range."""
        parser = build_parser()
        args = parser.parse_args([
            "weather",
            "--start", "2025-01-01",
            "--end", "2025-01-31",
        ])
        assert args.dataset == "weather"
        assert args.start == dt.date(2025, 1, 1)
        assert args.end == dt.date(2025, 1, 31)

    def test_parse_air_quality_dataset(self):
        """Test parsing air_quality dataset."""
        parser = build_parser()
        args = parser.parse_args(["air_quality"])
        assert args.dataset == "air_quality"

    def test_parse_facility_codes(self):
        """Test parsing facility codes argument."""
        parser = build_parser()
        args = parser.parse_args([
            "facilities",
            "--facility-codes", "TEST1,TEST2,TEST3",
        ])
        assert args.facility_codes == "TEST1,TEST2,TEST3"

    def test_parse_api_key(self):
        """Test parsing API key override."""
        parser = build_parser()
        args = parser.parse_args([
            "facilities",
            "--api-key", "test-key-12345",
        ])
        assert args.api_key == "test-key-12345"

    def test_parse_max_workers(self):
        """Test parsing max workers."""
        parser = build_parser()
        args = parser.parse_args([
            "weather",
            "--max-workers", "8",
        ])
        assert args.max_workers == 8

    def test_parse_app_name(self):
        """Test parsing custom app name."""
        parser = build_parser()
        args = parser.parse_args([
            "facilities",
            "--app-name", "custom-bronze-app",
        ])
        assert args.app_name == "custom-bronze-app"

    def test_parse_datetime_args_for_energy(self):
        """Test parsing datetime args for energy dataset."""
        parser = build_parser()
        args = parser.parse_args([
            "energy",
            "--date-start", "2025-01-01T00:00:00",
            "--date-end", "2025-01-31T23:59:59",
        ])
        assert args.date_start == dt.datetime(2025, 1, 1, 0, 0, 0)
        assert args.date_end == dt.datetime(2025, 1, 31, 23, 59, 59)

    def test_invalid_dataset_raises_error(self):
        """Test invalid dataset name raises error."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["invalid_dataset"])

    def test_invalid_mode_raises_error(self):
        """Test invalid mode raises error."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["facilities", "--mode", "invalid"])


class TestLoaderRegistry:
    """Test loader registry functionality."""

    def test_get_loader_registry_imports(self):
        """Test loader registry imports all loaders."""
        from pv_lakehouse.etl.bronze.cli import _get_loader_registry
        registry = _get_loader_registry()
        
        assert "facilities" in registry
        assert "energy" in registry
        assert "weather" in registry
        assert "air_quality" in registry
        assert len(registry) == 4

    def test_registry_classes_are_loaders(self):
        """Test all registry entries are BaseBronzeLoader subclasses."""
        from pv_lakehouse.etl.bronze.base import BaseBronzeLoader
        from pv_lakehouse.etl.bronze.cli import _get_loader_registry
        
        registry = _get_loader_registry()
        for loader_cls in registry.values():
            assert issubclass(loader_cls, BaseBronzeLoader)
