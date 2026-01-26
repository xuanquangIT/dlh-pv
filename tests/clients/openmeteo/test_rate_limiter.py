"""Unit tests for OpenMeteo rate limiter."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from pv_lakehouse.etl.clients.openmeteo.rate_limiter import RateLimiter


class TestRateLimiterInit:
    """Tests for RateLimiter initialization."""

    def test_positive_rate_sets_interval(self) -> None:
        """Positive rate should set a non-zero interval."""
        limiter = RateLimiter(max_requests_per_minute=60)
        assert limiter.interval == pytest.approx(1.0)
        assert limiter.is_enabled is True

    def test_zero_rate_disables_limiting(self) -> None:
        """Zero rate should disable rate limiting."""
        limiter = RateLimiter(max_requests_per_minute=0)
        assert limiter.interval == 0.0
        assert limiter.is_enabled is False

    def test_negative_rate_disables_limiting(self) -> None:
        """Negative rate should disable rate limiting."""
        limiter = RateLimiter(max_requests_per_minute=-10)
        assert limiter.interval == 0.0
        assert limiter.is_enabled is False

    def test_high_rate_small_interval(self) -> None:
        """High request rate should produce a small interval."""
        limiter = RateLimiter(max_requests_per_minute=600)
        assert limiter.interval == pytest.approx(0.1)


class TestRateLimiterWait:
    """Tests for RateLimiter.wait() behavior."""

    def test_disabled_limiter_returns_immediately(self) -> None:
        """Disabled limiter should not sleep."""
        limiter = RateLimiter(max_requests_per_minute=0)
        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        assert elapsed < 0.01

    def test_first_call_does_not_sleep(self) -> None:
        """First call to wait() should not sleep."""
        limiter = RateLimiter(max_requests_per_minute=60)
        with patch("time.sleep") as mock_sleep:
            limiter.wait()
            mock_sleep.assert_not_called()

    def test_second_call_sleeps_for_interval(self) -> None:
        """Second call should sleep for at least the interval duration."""
        limiter = RateLimiter(max_requests_per_minute=6000)  # 0.01s interval
        limiter.wait()  # First call
        start = time.monotonic()
        limiter.wait()  # Second call should wait
        elapsed = time.monotonic() - start
        # Allow some tolerance for timing
        assert elapsed >= 0.008  # ~80% of interval

    def test_no_sleep_if_enough_time_passed(self) -> None:
        """Should not sleep if enough time has passed since last request."""
        limiter = RateLimiter(max_requests_per_minute=6000)  # 0.01s interval
        limiter.wait()
        time.sleep(0.02)  # Wait longer than interval
        with patch("time.sleep") as mock_sleep:
            limiter.wait()
            mock_sleep.assert_not_called()


class TestRateLimiterReset:
    """Tests for RateLimiter.reset() behavior."""

    def test_reset_allows_immediate_request(self) -> None:
        """After reset, next wait() should not sleep."""
        limiter = RateLimiter(max_requests_per_minute=60)
        limiter.wait()  # First request
        limiter.reset()
        with patch("time.sleep") as mock_sleep:
            limiter.wait()  # Should be immediate after reset
            mock_sleep.assert_not_called()


class TestRateLimiterThreadSafety:
    """Tests for thread safety of RateLimiter."""

    def test_concurrent_waits_are_serialized(self) -> None:
        """Concurrent calls should be properly serialized."""
        limiter = RateLimiter(max_requests_per_minute=600)  # 0.1s interval
        results: list[float] = []
        errors: list[Exception] = []

        def worker() -> None:
            try:
                limiter.wait()
                results.append(time.monotonic())
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        start = time.monotonic()

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert len(errors) == 0
        assert len(results) == 5

        # Check that calls were spaced out
        results.sort()
        for i in range(1, len(results)):
            gap = results[i] - results[i - 1]
            # With 0.1s interval, gaps should be ~0.1s (allow some tolerance)
            assert gap >= 0.08, f"Gap {i} was only {gap}s"
