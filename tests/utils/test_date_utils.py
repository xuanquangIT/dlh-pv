"""Tests for date utilities module."""

from __future__ import annotations

import argparse
import datetime as dt

import pytest

from pv_lakehouse.etl.utils.date_utils import (
    DateRange,
    parse_date,
    parse_date_argparse,
    parse_datetime,
)


class TestDateRange:
    """Tests for DateRange dataclass."""

    def test_date_range_creation(self) -> None:
        """Test basic DateRange creation."""
        start = dt.date(2024, 1, 1)
        end = dt.date(2024, 1, 31)
        date_range = DateRange(start=start, end=end)
        assert date_range.start == start
        assert date_range.end == end

    def test_date_range_invalid_raises_error(self) -> None:
        """Test that start > end raises ValueError."""
        with pytest.raises(ValueError, match="must not be after"):
            DateRange(start=dt.date(2024, 2, 1), end=dt.date(2024, 1, 1))

    def test_date_range_same_day_valid(self) -> None:
        """Test same start and end date is valid."""
        same_day = dt.date(2024, 1, 15)
        date_range = DateRange(start=same_day, end=same_day)
        assert date_range.start == date_range.end

    def test_date_range_days_property(self) -> None:
        """Test days property calculation."""
        date_range = DateRange(
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 1, 31),
        )
        assert date_range.days == 30

    def test_date_range_days_same_day(self) -> None:
        """Test days property for same day."""
        same_day = dt.date(2024, 1, 15)
        date_range = DateRange(start=same_day, end=same_day)
        assert date_range.days == 0

    def test_date_range_is_valid_property(self) -> None:
        """Test is_valid property."""
        date_range = DateRange(
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 1, 31),
        )
        assert date_range.is_valid is True

    def test_date_range_from_strings(self) -> None:
        """Test from_strings class method."""
        date_range = DateRange.from_strings("2024-01-01", "2024-01-31")
        assert date_range.start == dt.date(2024, 1, 1)
        assert date_range.end == dt.date(2024, 1, 31)

    def test_date_range_from_strings_with_defaults(self) -> None:
        """Test from_strings with default values."""
        default_start = dt.date(2024, 1, 1)
        default_end = dt.date(2024, 1, 31)
        date_range = DateRange.from_strings(
            None,
            None,
            default_start=default_start,
            default_end=default_end,
        )
        assert date_range.start == default_start
        assert date_range.end == default_end

    def test_date_range_from_strings_missing_start(self) -> None:
        """Test from_strings raises error when start is missing."""
        with pytest.raises(ValueError, match="Start date is required"):
            DateRange.from_strings(None, "2024-01-31")

    def test_date_range_from_strings_missing_end(self) -> None:
        """Test from_strings raises error when end is missing."""
        with pytest.raises(ValueError, match="End date is required"):
            DateRange.from_strings("2024-01-01", None)

    def test_date_range_from_strings_mixed_defaults(self) -> None:
        """Test from_strings with mixed string and default values."""
        date_range = DateRange.from_strings(
            "2024-01-15",
            None,
            default_end=dt.date(2024, 1, 31),
        )
        assert date_range.start == dt.date(2024, 1, 15)
        assert date_range.end == dt.date(2024, 1, 31)

    def test_date_range_yesterday_to_today(self) -> None:
        """Test yesterday_to_today class method."""
        date_range = DateRange.yesterday_to_today()
        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)
        assert date_range.start == yesterday
        assert date_range.end == today

    def test_date_range_last_n_days(self) -> None:
        """Test last_n_days class method."""
        date_range = DateRange.last_n_days(7)
        today = dt.date.today()
        week_ago = today - dt.timedelta(days=7)
        assert date_range.start == week_ago
        assert date_range.end == today

    def test_date_range_last_n_days_invalid(self) -> None:
        """Test last_n_days raises error for non-positive days."""
        with pytest.raises(ValueError, match="Days must be positive"):
            DateRange.last_n_days(0)
        with pytest.raises(ValueError, match="Days must be positive"):
            DateRange.last_n_days(-1)

    def test_date_range_immutable(self) -> None:
        """Test DateRange is immutable (frozen)."""
        date_range = DateRange(
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 1, 31),
        )
        with pytest.raises(AttributeError):
            date_range.start = dt.date(2024, 2, 1)  # type: ignore[misc]


class TestParseDate:
    """Tests for parse_date function."""

    def test_parse_date_valid(self) -> None:
        """Test valid date parsing."""
        result = parse_date("2024-01-15")
        assert result == dt.date(2024, 1, 15)

    def test_parse_date_invalid_format(self) -> None:
        """Test invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date"):
            parse_date("01-15-2024")

    def test_parse_date_invalid_date(self) -> None:
        """Test invalid date raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date"):
            parse_date("2024-13-01")

    def test_parse_date_empty_string(self) -> None:
        """Test empty string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date"):
            parse_date("")

    def test_parse_date_with_time(self) -> None:
        """Test date string with time raises ValueError."""
        with pytest.raises(ValueError):
            parse_date("2024-01-15T10:30:00")


class TestParseDateArgparse:
    """Tests for parse_date_argparse function."""

    def test_parse_date_argparse_valid(self) -> None:
        """Test valid date parsing for argparse."""
        result = parse_date_argparse("2024-01-15")
        assert result == dt.date(2024, 1, 15)

    def test_parse_date_argparse_invalid(self) -> None:
        """Test invalid date raises ArgumentTypeError."""
        with pytest.raises(argparse.ArgumentTypeError, match="Invalid date"):
            parse_date_argparse("invalid")

    def test_parse_date_argparse_wrong_format(self) -> None:
        """Test wrong format raises ArgumentTypeError."""
        with pytest.raises(argparse.ArgumentTypeError, match="Invalid date"):
            parse_date_argparse("01/15/2024")


class TestParseDatetime:
    """Tests for parse_datetime function."""

    def test_parse_datetime_basic(self) -> None:
        """Test basic datetime parsing."""
        result = parse_datetime("2024-01-15T10:30:00")
        assert result == dt.datetime(2024, 1, 15, 10, 30, 0)

    def test_parse_datetime_with_seconds(self) -> None:
        """Test datetime parsing with seconds."""
        result = parse_datetime("2024-01-15T10:30:45")
        assert result == dt.datetime(2024, 1, 15, 10, 30, 45)

    def test_parse_datetime_with_timezone(self) -> None:
        """Test datetime parsing with timezone offset."""
        result = parse_datetime("2024-01-15T10:30:00+00:00")
        assert result.tzinfo is not None

    def test_parse_datetime_invalid(self) -> None:
        """Test invalid datetime raises ValueError."""
        with pytest.raises(ValueError, match="Invalid datetime"):
            parse_datetime("invalid")

    def test_parse_datetime_date_only(self) -> None:
        """Test date-only string returns datetime at midnight."""
        result = parse_datetime("2024-01-15")
        assert result == dt.datetime(2024, 1, 15, 0, 0, 0)
