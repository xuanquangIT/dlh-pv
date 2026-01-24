from __future__ import annotations
# API Endpoints
WEATHER_ENDPOINTS = {
    "archive": "https://archive-api.open-meteo.com/v1/archive",
    "forecast": "https://api.open-meteo.com/v1/forecast",
}
AIR_QUALITY_ENDPOINT = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Date range limits (days)
FORECAST_MAX_DAYS = 16
AIR_QUALITY_MAX_DAYS = 14

# Request retry configuration
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BACKOFF = 2.0
DEFAULT_TIMEOUT_SECONDS = 120

# Default hourly weather variables
DEFAULT_WEATHER_VARS = (
    "shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,"
    "terrestrial_radiation,temperature_2m,dew_point_2m,cloud_cover,cloud_cover_low,"
    "cloud_cover_mid,cloud_cover_high,precipitation,is_day,sunshine_duration,"
    "total_column_integrated_water_vapour,boundary_layer_height,wind_gusts_10m,"
    "wet_bulb_temperature_2m,wind_speed_10m,wind_direction_10m,pressure_msl"
)

# Default hourly air quality variables
DEFAULT_AIR_VARS = (
    "pm2_5,pm10,dust,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide,uv_index,uv_index_clear_sky"
)

# HTTP status codes for retry logic
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

__all__ = [
    "WEATHER_ENDPOINTS",
    "AIR_QUALITY_ENDPOINT",
    "FORECAST_MAX_DAYS",
    "AIR_QUALITY_MAX_DAYS",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_RETRY_BACKOFF",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_WEATHER_VARS",
    "DEFAULT_AIR_VARS",
    "RETRYABLE_STATUS_CODES",
]
