#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility energy data."""
from __future__ import annotations
import argparse, datetime as dt, logging
from typing import List, Optional
import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pv_lakehouse.etl.bronze.base import BaseBronzeLoader, BronzeLoadOptions
from pv_lakehouse.etl.clients import openelectricity

LOGGER = logging.getLogger(__name__)


class EnergyLoader(BaseBronzeLoader):
    """Bronze loader for facility energy data."""
    iceberg_table = "lh.bronze.raw_facility_energy"
    timestamp_column = "interval_ts"
    merge_keys = ("facility_code", "interval_ts", "metric")

    def __init__(self, options: Optional[BronzeLoadOptions] = None) -> None:
        super().__init__(options)
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
                    facility_codes=[code], metrics=["energy"], interval="1h",
                    date_start=fmt(self.options.start), date_end=fmt(self.options.end),
                    api_key=self.options.api_key, target_window_days=7, max_lookback_windows=52,
                )
                if not df.empty:
                    frames.append(df)
            except (ConnectionError, TimeoutError, OSError) as e:
                msg = str(e)
                if "403" in msg or "416" in msg:
                    skipped.append(code)
                else:
                    raise
        if skipped:
            LOGGER.warning("Skipped: %s", skipped)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("interval_ts", F.to_timestamp("interval_start")).withColumn(
            "interval_date", F.to_date("interval_ts")
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load facility energy into Bronze")
    p.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    p.add_argument("--facility-codes", help="Comma-separated facility codes")
    p.add_argument("--date-start", help="Start (YYYY-MM-DDTHH:MM:SS)")
    p.add_argument("--date-end", help="End (YYYY-MM-DDTHH:MM:SS)")
    p.add_argument("--api-key", help="Override API key")
    p.add_argument("--app-name", default="bronze-energy")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    parse_dt = lambda v: dt.datetime.fromisoformat(v) if v else None
    EnergyLoader(BronzeLoadOptions(
        mode=args.mode, start=parse_dt(args.date_start), end=parse_dt(args.date_end),
        facility_codes=args.facility_codes, api_key=args.api_key, app_name=args.app_name,
    )).run()


if __name__ == "__main__":
    main()
