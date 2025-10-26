#!/usr/bin/env python3
"""Bronze ingestion job for Open-Meteo weather and air-quality datasets."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import Iterable, List, Optional

import pandas as pd
from pyspark.sql import functions as F

from pv_lakehouse.etl.clients import openmeteo, openelectricity
from pv_lakehouse.etl.clients.openmeteo import FacilityLocation, RateLimiter
from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table

S3_WEATHER_BASE = "s3a://lakehouse/bronze/raw_facility_weather"
S3_AIR_BASE = "s3a://lakehouse/bronze/raw_facility_air_quality"
ICEBERG_WEATHER_TABLE = "lh.bronze.raw_facility_weather"
ICEBERG_AIR_TABLE = "lh.bronze.raw_facility_air_quality"


def parse_csv(value: Optional[str]) -> List[str]:
    return [token.strip() for token in (value or "").split(",") if token.strip()]


def parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - invalid user input
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Open-Meteo weather and air-quality data into Bronze zone"
    )
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--facility-codes", help="Comma separated facility codes to include")
    parser.add_argument("--start", required=True, type=parse_date, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, type=parse_date, help="End date (YYYY-MM-DD)")
    parser.add_argument("--weather-chunk-days", type=int, default=30)
    parser.add_argument("--air-chunk-days", type=int, default=openmeteo.AIR_QUALITY_MAX_DAYS)
    parser.add_argument("--weather-vars", default=openmeteo.DEFAULT_WEATHER_VARS)
    parser.add_argument("--air-vars", default=openmeteo.DEFAULT_AIR_VARS)
    parser.add_argument(
        "--weather-endpoint",
        choices=sorted({"auto", *openmeteo.WEATHER_ENDPOINTS.keys()}),
        default="auto",
    )
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("--max-requests-per-minute", type=float, default=30.0)
    parser.add_argument("--max-retries", type=int, default=openmeteo.DEFAULT_MAX_RETRIES)
    parser.add_argument("--retry-backoff", type=float, default=openmeteo.DEFAULT_RETRY_BACKOFF)
    parser.add_argument("--s3-weather-path", default=S3_WEATHER_BASE)
    parser.add_argument("--s3-air-path", default=S3_AIR_BASE)
    parser.add_argument("--iceberg-weather-table", default=ICEBERG_WEATHER_TABLE)
    parser.add_argument("--iceberg-air-table", default=ICEBERG_AIR_TABLE)
    parser.add_argument(
        "--openelectricity-api-key",
        help="Override OpenElectricity API key used to resolve facility locations",
    )
    parser.add_argument("--app-name", default="bronze-openmeteo")
    return parser.parse_args()


def resolve_facility_codes(args: argparse.Namespace) -> List[str]:
    codes = parse_csv(args.facility_codes)
    if codes:
        return [code.upper() for code in codes]
    return openelectricity.load_default_facility_codes()


def build_rate_limiter(args: argparse.Namespace) -> RateLimiter:
    return RateLimiter(args.max_requests_per_minute)


def load_facility_locations(
    facility_codes: Iterable[str],
    api_key: Optional[str],
) -> List[FacilityLocation]:
    selected_codes = [code.upper() for code in facility_codes if code]
    facilities_df = openelectricity.fetch_facilities_dataframe(
        api_key=api_key,
        selected_codes=selected_codes or None,
        networks=["NEM", "WEM"],
        statuses=["operating"],
        fueltechs=["solar_utility"],
    )

    if facilities_df.empty:
        raise ValueError("No facilities returned from OpenElectricity metadata API")

    facilities_df = facilities_df.dropna(subset=["location_lat", "location_lng"])
    if selected_codes:
        facilities_df = facilities_df[
            facilities_df["facility_code"].str.upper().isin(selected_codes)
        ]

    if facilities_df.empty:
        raise ValueError("Requested facilities missing latitude/longitude data")

    facilities: List[FacilityLocation] = []
    for row in facilities_df.itertuples(index=False):
        facilities.append(
            FacilityLocation(
                code=str(row.facility_code),
                name=str(row.facility_name or row.facility_code),
                latitude=float(row.location_lat),
                longitude=float(row.location_lng),
            )
        )
    return facilities


def collect_weather_and_air_data(
    facilities: List[FacilityLocation],
    args: argparse.Namespace,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    limiter = build_rate_limiter(args)
    weather_frames: List[pd.DataFrame] = []
    air_frames: List[pd.DataFrame] = []

    for facility in facilities:
        print(f"Fetching weather for facility {facility.code} ({facility.name})")
        weather_frame = openmeteo.fetch_weather_dataframe(
            facility,
            start=args.start,
            end=args.end,
            chunk_days=args.weather_chunk_days,
            hourly_variables=args.weather_vars,
            endpoint_preference=args.weather_endpoint,
            timezone=args.timezone,
            limiter=limiter,
            max_retries=args.max_retries,
            retry_backoff=args.retry_backoff,
        )
        if not weather_frame.empty:
            weather_frames.append(weather_frame)

        print(f"Fetching air quality for facility {facility.code} ({facility.name})")
        air_frame = openmeteo.fetch_air_quality_dataframe(
            facility,
            start=args.start,
            end=args.end,
            chunk_days=args.air_chunk_days,
            hourly_variables=args.air_vars,
            timezone=args.timezone,
            limiter=limiter,
            max_retries=args.max_retries,
            retry_backoff=args.retry_backoff,
        )
        if not air_frame.empty:
            air_frames.append(air_frame)

    weather_df = pd.concat(weather_frames, ignore_index=True) if weather_frames else pd.DataFrame()
    air_df = pd.concat(air_frames, ignore_index=True) if air_frames else pd.DataFrame()
    return weather_df, air_df


def write_dataset(
    spark_df,  # type: ignore[valid-type]
    *,
    s3_base_path: str,
    iceberg_table: str,
    args: argparse.Namespace,
    ingest_date: str,
    label: str,
) -> None:
    write_mode = "overwrite" if args.mode == "backfill" else "append"
    s3_target = f"{s3_base_path}/ingest_date={ingest_date}"
    (
        spark_df.write.mode(write_mode)
        .format("parquet")
        .option("compression", "snappy")
        .save(s3_target)
    )
    print(f"Wrote {label} parquet to {s3_target}")

    write_iceberg_table(
        spark_df,
        iceberg_table,
        mode=write_mode,
    )
    print(f"Upserted {label} data into Iceberg table {iceberg_table}")


def main() -> None:
    args = parse_args()

    if args.end < args.start:
        raise SystemExit("End date must not be before start date")

    facility_codes = resolve_facility_codes(args)
    facilities = load_facility_locations(facility_codes, args.openelectricity_api_key)

    weather_df, air_df = collect_weather_and_air_data(facilities, args)

    if weather_df.empty and air_df.empty:
        print("No Open-Meteo data retrieved; nothing to write.")
        return

    spark = create_spark_session(args.app_name)
    ingest_ts = F.current_timestamp()
    ingest_date = dt.date.today().isoformat()

    if not weather_df.empty:
        weather_spark_df = spark.createDataFrame(weather_df)
        weather_spark_df = (
            weather_spark_df.withColumn("ingest_mode", F.lit(args.mode))
            .withColumn("ingest_timestamp", ingest_ts)
            .withColumn("weather_timestamp", F.to_timestamp("date"))
            .withColumn("weather_date", F.to_date("weather_timestamp"))
        )
        write_dataset(
            weather_spark_df,
            s3_base_path=args.s3_weather_path,
            iceberg_table=args.iceberg_weather_table,
            args=args,
            ingest_date=ingest_date,
            label="weather",
        )

    if not air_df.empty:
        air_spark_df = spark.createDataFrame(air_df)
        air_spark_df = (
            air_spark_df.withColumn("ingest_mode", F.lit(args.mode))
            .withColumn("ingest_timestamp", ingest_ts)
            .withColumn("air_timestamp", F.to_timestamp("date"))
            .withColumn("air_date", F.to_date("air_timestamp"))
        )
        write_dataset(
            air_spark_df,
            s3_base_path=args.s3_air_path,
            iceberg_table=args.iceberg_air_table,
            args=args,
            ingest_date=ingest_date,
            label="air-quality",
        )

    spark.stop()


if __name__ == "__main__":
    main()
