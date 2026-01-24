from __future__ import annotations
from .client import (
    AutoEndpointStrategy,
    EndpointStrategy,
    FixedEndpointStrategy,
    create_endpoint_strategy,
    fetch_air_quality_dataframe,
    fetch_weather_dataframe,
)
from .constants import (
    AIR_QUALITY_ENDPOINT,
    AIR_QUALITY_MAX_DAYS,
    DEFAULT_AIR_VARS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BACKOFF,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_WEATHER_VARS,
    FORECAST_MAX_DAYS,
    WEATHER_ENDPOINTS,
)
from .models import (
    FacilityLocation,
    OpenMeteoAPIError,
    OpenMeteoConfigError,
    OpenMeteoError,
    OpenMeteoRateLimitError,
)
from .rate_limiter import RateLimiter

__all__ = [
    # Client functions
    "fetch_weather_dataframe",
    "fetch_air_quality_dataframe",
    # Strategy pattern
    "EndpointStrategy",
    "AutoEndpointStrategy",
    "FixedEndpointStrategy",
    "create_endpoint_strategy",
    # Models
    "FacilityLocation",
    # Rate limiter
    "RateLimiter",
    # Exceptions
    "OpenMeteoError",
    "OpenMeteoAPIError",
    "OpenMeteoRateLimitError",
    "OpenMeteoConfigError",
    # Constants
    "WEATHER_ENDPOINTS",
    "AIR_QUALITY_ENDPOINT",
    "FORECAST_MAX_DAYS",
    "AIR_QUALITY_MAX_DAYS",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_RETRY_BACKOFF",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_WEATHER_VARS",
    "DEFAULT_AIR_VARS",
]
