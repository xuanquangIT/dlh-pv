#!/usr/bin/env python3
"""Bronze ingestion job for facility-level Open-Meteo weather data."""

from __future__ import annotations

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pv_lakehouse.etl.bronze.base import BaseBronzeLoader
from pv_lakehouse.etl.bronze.facility_timezones import get_facility_timezone
from pv_lakehouse.etl.clients import openmeteo
from pv_lakehouse.etl.clients.openmeteo import RateLimiter
from pv_lakehouse.etl.utils import load_facility_locations

LOGGER = logging.getLogger(__name__)


class WeatherLoader(BaseBronzeLoader):
    """Bronze loader for facility weather data."""

    iceberg_table = "lh.bronze.raw_facility_weather"
    timestamp_column = "weather_timestamp"
    merge_keys = ("facility_code", "weather_timestamp")

    def _initialize_date_range(self) -> None:
        """Initialize date range for incremental weather loads."""
        today = dt.date.today()
        if self.options.mode == "incremental" and self.options.start is None:
            max_ts = self.get_max_timestamp()
            self.options.start = max_ts.date() if max_ts else today - dt.timedelta(days=1)
        else:
            self.options.start = self.options.start or (today - dt.timedelta(days=1))
        self.options.end = self.options.end or today

    def fetch_data(self) -> pd.DataFrame:
        """Fetch weather data from Open-Meteo API."""
        facilities = load_facility_locations(self.resolve_facilities(), self.options.api_key)
        limiter, frames, failed = RateLimiter(30.0), [], []

        def fetch_one(facility):
            return openmeteo.fetch_weather_dataframe(
                facility,
                start=self.options.start,
                end=self.options.end,
                chunk_days=30,
                hourly_variables=openmeteo.DEFAULT_WEATHER_VARS,
                endpoint_preference="auto",
                timezone=get_facility_timezone(facility.code),
                limiter=limiter,
                max_retries=openmeteo.DEFAULT_MAX_RETRIES,
                retry_backoff=openmeteo.DEFAULT_RETRY_BACKOFF,
                max_workers=self.options.max_workers,
            )

        with ThreadPoolExecutor(max_workers=self.options.max_workers) as executor:
            futures = {executor.submit(fetch_one, f): f for f in facilities}
            for future in as_completed(futures):
                facility = futures[future]
                try:
                    df = future.result()
                    if not df.empty:
                        frames.append(df)
                except (ConnectionError, TimeoutError, OSError) as e:
                    LOGGER.warning("Failed to fetch weather for %s: %s", facility.code, e)
                    failed.append(facility.code)
        if failed:
            LOGGER.error(
                "Weather fetch failed for %d/%d facilities: %s",
                len(failed),
                len(facilities),
                failed,
            )
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform weather data with NaN handling and timestamp columns."""
        for col in ["total_column_integrated_water_vapour", "boundary_layer_height"]:
            if col in df.columns:
                df = df.withColumn(col, F.when(F.isnan(F.col(col)), None).otherwise(F.col(col)))

        return (
            df.withColumn("weather_timestamp", F.to_timestamp("date"))
            .withColumn("weather_date", F.to_date("weather_timestamp"))
            .filter(F.col("weather_timestamp").isNotNull())
        )


if __name__ == "__main__":
    from pv_lakehouse.etl.bronze.cli import run_cli
    run_cli()

