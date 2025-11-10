"""Prefect flow to orchestrate hourly Bronze and Silver incremental loaders via spark-submit.        # Check for "End date must not be before start date" - trying to load future data
        if "end date must not be before start date" in stdout_lower:
            logger.warning("⚠️ Start date is in the future - no data to load yet. Skipping this run.")
            return  # Gracefully skip instead of failing
        
        # Check for "date_end must be after date_start" - Python ValueError from OpenElectricity client
        if "date_end must be after date_start" in stdout_lower:
            logger.warning("⚠️ End date is before start date - likely requesting future data. Skipping this run.")
            return  # Gracefully skip instead of failing
        
        # Check for "No timeseries records returned" - also not an error
        if "no timeseries records returned" in stdout_lower or "nothing to write" in stdout_lower:module invokes both Bronze and Silver ETL scripts inside the spark-master container
using docker compose. Each task runs in incremental mode and is scheduled to run
automatically every hour using Prefect deployments with interval scheduling.

Bronze loaders fetch new data from external APIs and write to Bronze Iceberg tables.
Silver loaders transform Bronze data into clean, validated Silver tables.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Iterable, Sequence

from prefect import flow, get_run_logger, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

_DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_ROOT = os.environ.get("PV_LAKEHOUSE_ROOT") or str(_DEFAULT_PROJECT_ROOT)
DOCKER_COMPOSE_FILE = os.environ.get("DOCKER_COMPOSE_FILE") or str(
    Path(PROJECT_ROOT) / "docker" / "docker-compose.yml"
)
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit")
DOCKER_COMPOSE_CMD = os.environ.get("DOCKER_COMPOSE_CMD", "docker compose")
DEFAULT_WORK_POOL = os.environ.get("PREFECT_WORK_POOL", "default-pool")
DEFAULT_DEPLOYMENT_NAME = "bronze-silver-hourly"

# Bronze script paths inside the spark-master container (mapped from host PROJECT_ROOT to /opt/workdir)
BRONZE_SCRIPTS = {
    "facilities": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facilities.py",
    "air_quality": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py",
    "timeseries": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py",
    "weather": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py",
}

# Silver CLI script path
SILVER_CLI_SCRIPT = "/opt/workdir/src/pv_lakehouse/etl/silver/cli.py"


def _build_shell_command(script: str, extra_args: Sequence[str]) -> Sequence[str]:
    """Construct the docker compose exec command for spark-submit."""

    spark_command = " ".join(
        [shlex.quote(SPARK_SUBMIT), shlex.quote(script), *map(shlex.quote, extra_args)]
    )

    # Use bash -lc to ensure PATH and environment variables expand correctly.
    shell_line = f"set -euo pipefail; PYTHONPATH=/opt/workdir/src {spark_command}"

    compose_parts = [
        *shlex.split(DOCKER_COMPOSE_CMD),
        "-f",
        DOCKER_COMPOSE_FILE,
        "exec",
        "-T",
        "spark-master",
        "bash",
        "-lc",
        shell_line,
    ]
    return compose_parts


def _run_subprocess(command: Sequence[str], env_overrides: Iterable[tuple[str, str]] | None = None) -> None:
    """Execute a subprocess command with optional environment overrides."""

    env = os.environ.copy()
    if env_overrides:
        for key, value in env_overrides:
            env[key] = value

    result = subprocess.run(command, check=False, capture_output=True, text=True, env=env)
    logger = get_run_logger()
    
    if result.stdout:
        logger.info(result.stdout.rstrip())
    if result.stderr:
        logger.warning(result.stderr.rstrip())
    
    if result.returncode != 0:
        # Check if this is a 416 Range Not Satisfiable error (data not yet available)
        stdout_lower = result.stdout.lower()
        if "416" in stdout_lower and "range not satisfiable" in stdout_lower:
            logger.warning("API returned 416 Range Not Satisfiable - data not yet available (likely requesting future data). Skipping this run.")
            return  # Gracefully skip instead of failing
        
        # Check for "End date must not be before start date" - trying to load future data
        if "end date must not be before start date" in stdout_lower:
            logger.warning("⚠️ Start date is in the future - no data to load yet. Skipping this run.")
            return  # Gracefully skip instead of failing
        
        # Check for "date_end must be after date_start" - ValueError from OpenElectricity API
        if "date_end must be after date_start" in stdout_lower or "valueerror" in stdout_lower:
            logger.warning("⚠️ Date validation error - likely requesting future data. Skipping this run.")
            return  # Gracefully skip instead of failing
        
        # Check for "No timeseries records returned" - also not an error
        if "no timeseries records returned" in stdout_lower or "nothing to write" in stdout_lower:
            logger.info("ℹNo new data available. Skipping this run.")
            return
        
        # For other errors, raise exception
        raise RuntimeError(
            "\n".join(
                [
                    f"Command exited with {result.returncode}: {' '.join(map(shlex.quote, command))}",
                    "STDOUT:",
                    result.stdout.strip() or "<empty>",
                    "STDERR:",
                    result.stderr.strip() or "<empty>",
                ]
            )
        )


# ==================== BRONZE TASKS ====================

@task(name="bronze_facilities_loader", retries=1, retry_delay_seconds=30)
def run_bronze_facilities_loader() -> None:
    """Load facility metadata from OpenElectricity API into Bronze.
    
    Retries once after 30 seconds if there's a transient error.
    """
    command = _build_shell_command(
        BRONZE_SCRIPTS["facilities"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="bronze_timeseries_loader", retries=1, retry_delay_seconds=30)
def run_bronze_timeseries_loader() -> None:
    """Load energy timeseries data from OpenElectricity API into Bronze.
    
    Retries once after 30 seconds if there's a transient error.
    Gracefully skips if data is not yet available (416 error).
    """
    command = _build_shell_command(
        BRONZE_SCRIPTS["timeseries"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="bronze_weather_loader", retries=1, retry_delay_seconds=30)
def run_bronze_weather_loader() -> None:
    """Load weather data from Open-Meteo API into Bronze.
    
    Retries once after 30 seconds if there's a transient error.
    Gracefully skips if data is not yet available.
    """
    command = _build_shell_command(
        BRONZE_SCRIPTS["weather"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="bronze_air_quality_loader", retries=1, retry_delay_seconds=30)
def run_bronze_air_quality_loader() -> None:
    """Load air quality data from Open-Meteo API into Bronze.
    
    Retries once after 30 seconds if there's a transient error.
    Gracefully skips if data is not yet available.
    """
    command = _build_shell_command(
        BRONZE_SCRIPTS["air_quality"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


# ==================== SILVER TASKS ====================

@task(name="silver_facility_master_loader", retries=1, retry_delay_seconds=30)
def run_silver_facility_master_loader() -> None:
    """Transform Bronze facility data into Silver facility master table.
    
    Retries once after 30 seconds if there's a transient error.
    """
    command = _build_shell_command(
        SILVER_CLI_SCRIPT,
        ["facility_master", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="silver_hourly_energy_loader", retries=1, retry_delay_seconds=30)
def run_silver_hourly_energy_loader() -> None:
    """Transform Bronze timeseries data into Silver clean hourly energy table.
    
    Retries once after 30 seconds if there's a transient error.
    """
    command = _build_shell_command(
        SILVER_CLI_SCRIPT,
        ["hourly_energy", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="silver_hourly_weather_loader", retries=1, retry_delay_seconds=30)
def run_silver_hourly_weather_loader() -> None:
    """Transform Bronze weather data into Silver clean hourly weather table.
    
    Retries once after 30 seconds if there's a transient error.
    """
    command = _build_shell_command(
        SILVER_CLI_SCRIPT,
        ["hourly_weather", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="silver_hourly_air_quality_loader", retries=1, retry_delay_seconds=30)
def run_silver_hourly_air_quality_loader() -> None:
    """Transform Bronze air quality data into Silver clean hourly air quality table.
    
    Retries once after 30 seconds if there's a transient error.
    """
    command = _build_shell_command(
        SILVER_CLI_SCRIPT,
        ["hourly_air_quality", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


# ==================== MAIN FLOW ====================

@flow(name="bronze-silver-hourly-incremental")
def bronze_silver_hourly_incremental_flow() -> None:
    """
    Run all Bronze and Silver incremental loaders sequentially.
    
    Execution order:
    1. Bronze: Load raw data from external APIs
       - Facilities metadata
       - Energy timeseries
       - Weather data
       - Air quality data
    
    2. Silver: Transform Bronze data into clean tables
       - Facility master
       - Hourly energy
       - Hourly weather
       - Hourly air quality
    
    This flow is designed to run automatically every hour via Prefect scheduling.
    """
    logger = get_run_logger()
    
    # ==================== BRONZE LAYER ====================
    logger.info("Starting Bronze layer incremental load...")
    
    run_bronze_facilities_loader()
    run_bronze_timeseries_loader()
    run_bronze_weather_loader()
    run_bronze_air_quality_loader()
    
    logger.info("Bronze layer incremental load completed.")
    
    # ==================== SILVER LAYER ====================
    logger.info("Starting Silver layer incremental transformation...")
    
    run_silver_facility_master_loader()
    run_silver_hourly_energy_loader()
    run_silver_hourly_weather_loader()
    run_silver_hourly_air_quality_loader()
    
    logger.info("Silver layer incremental transformation completed.")
    logger.info("Bronze-to-Silver hourly incremental pipeline finished successfully.")


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run or manage the Bronze-Silver hourly incremental Prefect flow."
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Build and apply a Prefect deployment with hourly interval schedule.",
    )
    parser.add_argument(
        "--work-pool",
        default=DEFAULT_WORK_POOL,
        help="Work pool name to target when building the deployment (default: %(default)s)",
    )
    parser.add_argument(
        "--deployment-name",
        default=DEFAULT_DEPLOYMENT_NAME,
        help="Friendly name for the Prefect deployment (default: %(default)s)",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Skip running the flow immediately (only meaningful without --deploy).",
    )
    return parser.parse_args()


def _build_and_apply_deployment(work_pool: str, deployment_name: str) -> None:
    """
    Build and apply a Prefect deployment with cron scheduling.
    
    Args:
        work_pool: Name of the Prefect work pool
        deployment_name: Friendly name for the deployment
    
    Schedule: Runs at 1 minute past every hour (e.g., 21:01, 22:01, 23:01)
    """
    # Cron schedule: minute=1, hour=every hour
    # Format: "minute hour day month day_of_week"
    schedule = CronSchedule(cron="1 * * * *", timezone="UTC")
    
    deployment = Deployment.build_from_flow(
        flow=bronze_silver_hourly_incremental_flow,
        name=deployment_name,
        work_pool_name=work_pool,
        schedule=schedule,
        description="Hourly incremental Bronze and Silver ETL pipeline (runs at :01 of every hour)",
        tags=["bronze", "silver", "incremental", "hourly", "etl"],
    )
    deployment.apply()
    
    print(f"Deployment '{deployment_name}' created successfully!")
    print(f"Schedule: Every hour at minute 1 (e.g., 21:01, 22:01, 23:01)")
    print(f"Cron Expression: 1 * * * * (UTC timezone)")
    print(f"Work Pool: {work_pool}")
    print("\nTo start the deployment, run:")
    print(f"  prefect deployment run 'bronze-silver-hourly-incremental/{deployment_name}'")


if __name__ == "__main__":
    args = _parse_cli_args()

    if args.deploy:
        _build_and_apply_deployment(args.work_pool, args.deployment_name)

    if not args.skip_run:
        bronze_silver_hourly_incremental_flow()
