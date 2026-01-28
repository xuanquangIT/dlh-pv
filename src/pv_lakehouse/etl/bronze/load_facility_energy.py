#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility energy data."""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pv_lakehouse.etl.bronze.base import BaseBronzeLoader
from pv_lakehouse.etl.clients import openelectricity

LOGGER = logging.getLogger(__name__)


class EnergyLoader(BaseBronzeLoader):
    """Bronze loader for facility energy data."""

    iceberg_table = "lh.bronze.raw_facility_energy"
    timestamp_column = "interval_ts"
    merge_keys = ("facility_code", "interval_ts", "metric")

    def _initialize_date_range(self) -> None:
        """Initialize date range for incremental energy loads."""
        if self.options.mode == "incremental" and self.options.start is None:
            max_ts = self.get_max_timestamp()
            if max_ts:
                self.options.start = max_ts + dt.timedelta(hours=1)
                self.options.end = self.options.end or dt.datetime.now(dt.timezone.utc)
                LOGGER.info("Incremental: from %s (last: %s)", self.options.start, max_ts)

    def fetch_data(self) -> pd.DataFrame:
        """Fetch energy data from OpenElectricity API."""
        frames, skipped = [], []
        fmt = lambda v: v.strftime("%Y-%m-%dT%H:%M:%S") if v else None

        for code in self.resolve_facilities():
            try:
                LOGGER.info("Fetching: %s", code)
                df = openelectricity.fetch_facility_timeseries_dataframe(
                    facility_codes=[code],
                    metrics=["energy"],
                    interval="1h",
                    date_start=fmt(self.options.start),
                    date_end=fmt(self.options.end),
                    api_key=self.options.api_key,
                    target_window_days=7,
                    max_lookback_windows=52,
                )
                if not df.empty:
                    frames.append(df)
            except Exception as e:
                # 400: Bad Request, 403: Forbidden, 404: Not Found, 416: Range Not Satisfiable, 429: Rate Limit
                import requests
                if isinstance(e, requests.exceptions.HTTPError):
                    # Safely extract status code from HTTPError response using hasattr
                    status_code = None
                    if hasattr(e, 'response') and e.response is not None:
                        if hasattr(e.response, 'status_code'):
                            status_code = e.response.status_code
                    
                    # Handle facility-specific 4xx errors (not server errors)
                    if status_code and 400 <= status_code < 500:
                        LOGGER.warning(
                            "Skipping facility %s: HTTP %d - %s",
                            code,
                            status_code,
                            str(e),
                        )
                        skipped.append(code)
                        continue
                # Re-raise all other exceptions (5xx, network errors, etc.)
                raise

        if skipped:
            LOGGER.warning("Skipped facilities (403/416): %s", skipped)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform energy data with timestamp columns."""
        return df.withColumn("interval_ts", F.to_timestamp("interval_start")).withColumn(
            "interval_date", F.to_date("interval_ts")
        )


if __name__ == "__main__":
    from pv_lakehouse.etl.bronze.cli import run_cli
    run_cli()

