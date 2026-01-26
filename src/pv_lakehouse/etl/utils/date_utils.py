from __future__ import annotations
import argparse
import datetime as dt
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class DateRange:
    """Immutable date range for ETL jobs.

    Provides a consistent container for start/end date pairs with validation
    and helper methods for common date operations.

    Attributes:
        start: Start date (inclusive).
        end: End date (inclusive).

    Examples:
        >>> date_range = DateRange(
        ...     start=dt.date(2024, 1, 1),
        ...     end=dt.date(2024, 1, 31)
        ... )
        >>> date_range.days
        30
        >>> date_range.is_valid
        True
    """
    start: dt.date
    end: dt.date
    def __post_init__(self) -> None:
        """Validate that start date is not after end date."""
        if self.start > self.end:
            raise ValueError(
                f"Start date ({self.start}) must not be after end date ({self.end})"
            )

    @property
    def days(self) -> int:
        """Return number of days between start and end (exclusive of end).

        Returns:
            Number of days in the range.
        """
        return (self.end - self.start).days

    @property
    def is_valid(self) -> bool:
        """Check if the date range is valid (start <= end).

        Returns:
            True if valid, False otherwise.
        """
        return self.start <= self.end

    @classmethod
    def from_strings(
        cls,
        start: Optional[str],
        end: Optional[str],
        default_start: Optional[dt.date] = None,
        default_end: Optional[dt.date] = None,
    ) -> "DateRange":
        """Create DateRange from ISO format strings with optional defaults.

        Args:
            start: Start date string in YYYY-MM-DD format, or None.
            end: End date string in YYYY-MM-DD format, or None.
            default_start: Default start date if start is None.
            default_end: Default end date if end is None.

        Returns:
            DateRange instance.

        Raises:
            ValueError: If date string is invalid or required date is missing.

        Examples:
            >>> DateRange.from_strings(
            ...     "2024-01-01",
            ...     "2024-01-31"
            ... )
            DateRange(start=datetime.date(2024, 1, 1), end=datetime.date(2024, 1, 31))
        """
        parsed_start = parse_date(start) if start else default_start
        parsed_end = parse_date(end) if end else default_end

        if parsed_start is None:
            raise ValueError("Start date is required when no default is provided")
        if parsed_end is None:
            raise ValueError("End date is required when no default is provided")

        return cls(start=parsed_start, end=parsed_end)

    @classmethod
    def yesterday_to_today(cls) -> "DateRange":
        """Create a DateRange from yesterday to today.

        Returns:
            DateRange from yesterday (inclusive) to today (inclusive).

        Examples:
            >>> # If today is 2024-01-15
            >>> DateRange.yesterday_to_today()
            DateRange(start=datetime.date(2024, 1, 14), end=datetime.date(2024, 1, 15))
        """
        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)
        return cls(start=yesterday, end=today)

    @classmethod
    def last_n_days(cls, days: int) -> "DateRange":
        """Create a DateRange for the last N days ending today.

        Args:
            days: Number of days to look back (must be positive).

        Returns:
            DateRange from (today - days) to today.

        Raises:
            ValueError: If days is not positive.

        Examples:
            >>> # If today is 2024-01-15
            >>> DateRange.last_n_days(7)
            DateRange(start=datetime.date(2024, 1, 8), end=datetime.date(2024, 1, 15))
        """
        if days <= 0:
            raise ValueError(f"Days must be positive, got {days}")
        today = dt.date.today()
        start = today - dt.timedelta(days=days)
        return cls(start=start, end=today)


def parse_date(value: str) -> dt.date:
    """Parse an ISO format date string (YYYY-MM-DD).

    Args:
        value: Date string in YYYY-MM-DD format.

    Returns:
        Parsed date object.

    Raises:
        ValueError: If the date string is invalid.

    Examples:
        >>> parse_date("2024-01-15")
        datetime.date(2024, 1, 15)
    """
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Expected YYYY-MM-DD format.") from exc


def parse_date_argparse(value: str) -> dt.date:
    """Parse a date string for argparse arguments.

    Same as parse_date but raises argparse.ArgumentTypeError for
    integration with argparse type= parameter.

    Args:
        value: Date string in YYYY-MM-DD format.

    Returns:
        Parsed date object.

    Raises:
        argparse.ArgumentTypeError: If the date string is invalid.

    Examples:
        >>> import argparse
        >>> parser = argparse.ArgumentParser()
        >>> parser.add_argument("--date", type=parse_date_argparse)
    """
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYY-MM-DD format."
        ) from exc


def parse_datetime(value: str) -> dt.datetime:
    """Parse an ISO format datetime string.

    Supports formats:
    - YYYY-MM-DDTHH:MM:SS
    - YYYY-MM-DDTHH:MM:SS+HH:MM (with timezone)

    Args:
        value: Datetime string in ISO format.

    Returns:
        Parsed datetime object.

    Raises:
        ValueError: If the datetime string is invalid.

    Examples:
        >>> parse_datetime("2024-01-15T10:30:00")
        datetime.datetime(2024, 1, 15, 10, 30)
    """
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid datetime '{value}'. Expected ISO format (YYYY-MM-DDTHH:MM:SS)."
        ) from exc


__all__ = [
    "DateRange",
    "parse_date",
    "parse_date_argparse",
    "parse_datetime",
]
