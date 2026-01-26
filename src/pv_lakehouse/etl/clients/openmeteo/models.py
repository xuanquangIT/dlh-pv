"""Data models and custom exceptions for Open-Meteo API client."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import requests

@dataclass
class FacilityLocation:
    """Minimal facility metadata for location-based requests."""
    code: str
    name: str
    latitude: float
    longitude: float

class OpenMeteoError(Exception):
    """Base exception for all Open-Meteo client errors."""
    pass

class OpenMeteoAPIError(OpenMeteoError):
    """API request failed with an error response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        detail: Optional[str] = None,
        response: Optional[requests.Response] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail
        self.response = response

class OpenMeteoRateLimitError(OpenMeteoAPIError):
    """Rate limit exceeded (HTTP 429)."""
    pass

class OpenMeteoConfigError(OpenMeteoError):
    """Invalid configuration (endpoint preference, etc.)."""
    pass

__all__ = [
    "FacilityLocation",
    "OpenMeteoError",
    "OpenMeteoAPIError",
    "OpenMeteoRateLimitError",
    "OpenMeteoConfigError",
]
