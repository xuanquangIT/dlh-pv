"""Unit tests for OpenMeteo endpoint selection strategies."""

from __future__ import annotations

import datetime as dt

import pytest

from pv_lakehouse.etl.clients.openmeteo.client import (
    AutoEndpointStrategy,
    FixedEndpointStrategy,
    create_endpoint_strategy,
)
from pv_lakehouse.etl.clients.openmeteo.models import OpenMeteoConfigError


class TestAutoEndpointStrategy:
    """Tests for AutoEndpointStrategy."""

    def test_selects_archive_before_cutoff(self) -> None:
        """Dates before or on cutoff should use archive endpoint."""
        strategy = AutoEndpointStrategy()
        cutoff = dt.date(2025, 1, 15)
        
        # Before cutoff
        assert strategy.select_endpoint(dt.date(2025, 1, 10), cutoff) == "archive"
        # On cutoff
        assert strategy.select_endpoint(dt.date(2025, 1, 15), cutoff) == "archive"

    def test_selects_forecast_after_cutoff(self) -> None:
        """Dates after cutoff should use forecast endpoint."""
        strategy = AutoEndpointStrategy()
        cutoff = dt.date(2025, 1, 15)
        
        assert strategy.select_endpoint(dt.date(2025, 1, 16), cutoff) == "forecast"
        assert strategy.select_endpoint(dt.date(2025, 1, 20), cutoff) == "forecast"


class TestFixedEndpointStrategy:
    """Tests for FixedEndpointStrategy."""

    def test_archive_strategy_always_returns_archive(self) -> None:
        """Archive strategy should always return 'archive'."""
        strategy = FixedEndpointStrategy("archive")
        cutoff = dt.date(2025, 1, 15)
        
        # Before cutoff
        assert strategy.select_endpoint(dt.date(2025, 1, 10), cutoff) == "archive"
        # After cutoff
        assert strategy.select_endpoint(dt.date(2025, 1, 20), cutoff) == "archive"

    def test_forecast_strategy_always_returns_forecast(self) -> None:
        """Forecast strategy should always return 'forecast'."""
        strategy = FixedEndpointStrategy("forecast")
        cutoff = dt.date(2025, 1, 15)
        
        # Before cutoff
        assert strategy.select_endpoint(dt.date(2025, 1, 10), cutoff) == "forecast"
        # After cutoff
        assert strategy.select_endpoint(dt.date(2025, 1, 20), cutoff) == "forecast"

    def test_invalid_endpoint_raises_error(self) -> None:
        """Invalid endpoint name should raise OpenMeteoConfigError."""
        with pytest.raises(OpenMeteoConfigError, match="Unknown endpoint"):
            FixedEndpointStrategy("invalid")


class TestCreateEndpointStrategy:
    """Tests for the create_endpoint_strategy factory function."""

    def test_auto_preference_returns_auto_strategy(self) -> None:
        """'auto' preference should return AutoEndpointStrategy."""
        strategy = create_endpoint_strategy("auto")
        assert isinstance(strategy, AutoEndpointStrategy)

    def test_archive_preference_returns_fixed_strategy(self) -> None:
        """'archive' preference should return FixedEndpointStrategy."""
        strategy = create_endpoint_strategy("archive")
        assert isinstance(strategy, FixedEndpointStrategy)
        assert strategy.select_endpoint(dt.date(2025, 1, 1), dt.date(2024, 1, 1)) == "archive"

    def test_forecast_preference_returns_fixed_strategy(self) -> None:
        """'forecast' preference should return FixedEndpointStrategy."""
        strategy = create_endpoint_strategy("forecast")
        assert isinstance(strategy, FixedEndpointStrategy)
        assert strategy.select_endpoint(dt.date(2025, 1, 1), dt.date(2024, 1, 1)) == "forecast"

    def test_invalid_preference_raises_error(self) -> None:
        """Invalid preference should raise OpenMeteoConfigError."""
        with pytest.raises(OpenMeteoConfigError, match="Unsupported weather endpoint"):
            create_endpoint_strategy("unknown")
        
        with pytest.raises(OpenMeteoConfigError, match="Unsupported weather endpoint"):
            create_endpoint_strategy("")
