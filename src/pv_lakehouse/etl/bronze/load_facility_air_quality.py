#!/usr/bin/env python3
"""Bronze ingestion job for facility-level Open-Meteo air quality data."""

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
from pv_lakehouse.etl.utils.etl_metrics import (
    ETLTimer,
    log_fetch_start,
    log_fetch_summary,
)

LOGGER = logging.getLogger(__name__)


class AirQualityLoader(BaseBronzeLoader):
    """Bronze loader for facility air quality data."""

    iceberg_table = "lh.bronze.raw_facility_air_quality"
    timestamp_column = "air_timestamp"
    merge_keys = ("facility_code", "air_timestamp")

    def _initialize_date_range(self) -> None:
        """Initialize date range for incremental air quality loads."""
        today = dt.date.today()
        if self.options.mode == "incremental" and self.options.start is None:
            max_ts = self.get_max_timestamp()
            self.options.start = max_ts.date() if max_ts else today - dt.timedelta(days=1)
        else:
            self.options.start = self.options.start or (today - dt.timedelta(days=1))
        self.options.end = self.options.end or today

    def fetch_data(self) -> pd.DataFrame:
        """Fetch air quality data from Open-Meteo API."""
        timer = ETLTimer()
        facilities = load_facility_locations(self.resolve_facilities(), self.options.api_key)
        total_facilities = len(facilities)
        
        log_fetch_start(
            LOGGER,
            "air quality",
            total_facilities,
            date_range=(self.options.start, self.options.end),
        )
        
        limiter = RateLimiter(30.0)
        frames: list[pd.DataFrame] = []
        failed: list[str] = []

        def fetch_one(facility):
            return openmeteo.fetch_air_quality_dataframe(
                facility,
                start=self.options.start,
                end=self.options.end,
                chunk_days=openmeteo.AIR_QUALITY_MAX_DAYS,
                hourly_variables=openmeteo.DEFAULT_AIR_VARS,
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
                    LOGGER.warning("Failed to fetch air quality for %s: %s", facility.code, e)
                    failed.append(facility.code)
        
        # Summary logging
        result_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        log_fetch_summary(
            LOGGER,
            "air quality",
            total_facilities,
            total_facilities - len(failed),
            failed,
            len(result_df),
            timer.elapsed(),
        )
        return result_df

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform air quality data with timestamp columns."""
        return (
            df.withColumn("air_timestamp", F.to_timestamp("date"))
            .withColumn("air_date", F.to_date("air_timestamp"))
            .filter(F.col("air_timestamp").isNotNull())
        )


if __name__ == "__main__":
    from pv_lakehouse.etl.bronze.cli import run_cli
    run_cli()

