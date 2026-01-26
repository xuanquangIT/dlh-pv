"""Thread-safe rate limiter for Open-Meteo API requests."""
from __future__ import annotations
import threading
import time
from typing import Optional

class RateLimiter:
    """
    Thread-safe rate limiter using a simple interval-based approach.

    Ensures that requests are spaced at least `60 / max_requests_per_minute`
    seconds apart. When `max_requests_per_minute` is 0 or negative, no rate
    limiting is applied.

    Example:
        >>> limiter = RateLimiter(max_requests_per_minute=60)  # 1 req/sec
        >>> limiter.wait()  # Blocks if needed
        >>> make_request()
    """

    def __init__(self, max_requests_per_minute: float) -> None:
        """
        Initialize the rate limiter.

        Args:
            max_requests_per_minute: Maximum number of requests allowed per minute.
                Use 0 or negative to disable rate limiting.
        """
        self._interval = 0.0
        if max_requests_per_minute > 0:
            self._interval = 60.0 / max_requests_per_minute
        self._last_request: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def interval(self) -> float:
        """Return the minimum interval between requests in seconds."""
        return self._interval

    @property
    def is_enabled(self) -> bool:
        """Return True if rate limiting is active."""
        return self._interval > 0

    def wait(self) -> None:
        """
        Block until the next request is allowed.

        Thread-safe implementation using a lock to ensure accurate timing
        across concurrent calls.
        """
        if self._interval <= 0:
            return

        with self._lock:
            now = time.monotonic()
            if self._last_request is not None:
                delay = self._last_request + self._interval - now
                if delay > 0:
                    time.sleep(delay)
                    now = time.monotonic()
            self._last_request = now

    def reset(self) -> None:
        """Reset the rate limiter state, allowing an immediate request."""
        with self._lock:
            self._last_request = None


__all__ = ["RateLimiter"]
