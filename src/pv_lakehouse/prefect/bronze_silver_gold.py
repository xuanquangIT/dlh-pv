"""Prefect flow to orchestrate Bronze → Silver → Gold incremental loaders via spark-submit.

This mirrors bronze_silver_hourly_incremental but continues into Gold to build:
  - Gold dim_date
  - Gold fact_solar_environmental
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
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
DEFAULT_DEPLOYMENT_NAME = "bronze-silver-gold-hourly"

# Bronze script paths inside the spark-master container (mapped from host PROJECT_ROOT to /opt/workdir)
BRONZE_SCRIPTS = {
    "facilities": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facilities.py",
    "air_quality": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py",
    "timeseries": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py",
    "weather": "/opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py",
}

# Silver & Gold CLI script paths
SILVER_CLI_SCRIPT = "/opt/workdir/src/pv_lakehouse/etl/silver/cli.py"
GOLD_CLI_SCRIPT = "/opt/workdir/src/pv_lakehouse/etl/gold/cli.py"


def _build_shell_command(script: str, extra_args: Sequence[str]) -> Sequence[str]:
    """Construct the docker compose exec command for spark-submit."""

    spark_command = " ".join(
        [shlex.quote(SPARK_SUBMIT), shlex.quote(script), *map(shlex.quote, extra_args)]
    )

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
        stdout_lower = result.stdout.lower()
        # Graceful skip conditions for common data-not-ready scenarios
        if "416" in stdout_lower and "range not satisfiable" in stdout_lower:
            logger.warning("API returned 416 Range Not Satisfiable - data not yet available. Skipping this run.")
            return
        if "end date must not be before start date" in stdout_lower:
            logger.warning("⚠️ Start date is in the future - no data to load yet. Skipping this run.")
            return
        if "date_end must be after date_start" in stdout_lower:
            logger.warning("⚠️ End date is before start date - likely requesting future data. Skipping this run.")
            return
        if "no timeseries records returned" in stdout_lower or "nothing to write" in stdout_lower:
            logger.warning("No data returned for this window - skipping without failure.")
            return

        # If none of the graceful cases matched, raise.
        raise subprocess.CalledProcessError(
            result.returncode, command, output=result.stdout, stderr=result.stderr
        )


# ==================== BRONZE TASKS ====================

# @task(name="bronze_facilities_loader", retries=1, retry_delay_seconds=30)
# def run_bronze_facilities_loader() -> None:
#     command = _build_shell_command(
#         BRONZE_SCRIPTS["facilities"],
#         ["--mode", "incremental"],
#     )
#     _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="bronze_timeseries_loader", retries=1, retry_delay_seconds=30)
def run_bronze_timeseries_loader() -> None:
    command = _build_shell_command(
        BRONZE_SCRIPTS["timeseries"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="bronze_weather_loader", retries=1, retry_delay_seconds=30)
def run_bronze_weather_loader() -> None:
    command = _build_shell_command(
        BRONZE_SCRIPTS["weather"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="bronze_air_quality_loader", retries=1, retry_delay_seconds=30)
def run_bronze_air_quality_loader() -> None:
    command = _build_shell_command(
        BRONZE_SCRIPTS["air_quality"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


# ==================== SILVER TASKS ====================

# @task(name="silver_facility_master_loader", retries=1, retry_delay_seconds=30)
# def run_silver_facility_master_loader() -> None:
#     command = _build_shell_command(
#         SILVER_CLI_SCRIPT,
#         ["facility_master", "--mode", "incremental", "--load-strategy", "merge"],
#     )
#     _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="silver_hourly_energy_loader", retries=1, retry_delay_seconds=30)
def run_silver_hourly_energy_loader() -> None:
    command = _build_shell_command(
        SILVER_CLI_SCRIPT,
        ["hourly_energy", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="silver_hourly_weather_loader", retries=1, retry_delay_seconds=30)
def run_silver_hourly_weather_loader() -> None:
    command = _build_shell_command(
        SILVER_CLI_SCRIPT,
        ["hourly_weather", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="silver_hourly_air_quality_loader", retries=1, retry_delay_seconds=30)
def run_silver_hourly_air_quality_loader() -> None:
    command = _build_shell_command(
        SILVER_CLI_SCRIPT,
        ["hourly_air_quality", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


# ==================== GOLD TASKS ====================

@task(name="gold_dim_date_loader", retries=1, retry_delay_seconds=30)
def run_gold_dim_date_loader() -> None:
    command = _build_shell_command(
        GOLD_CLI_SCRIPT,
        ["dim_date", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="gold_fact_solar_environmental_loader", retries=1, retry_delay_seconds=30)
def run_gold_fact_solar_environmental_loader() -> None:
    command = _build_shell_command(
        GOLD_CLI_SCRIPT,
        ["fact_solar_environmental", "--mode", "incremental", "--load-strategy", "merge"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


# ==================== FLOW ====================

@flow(name="bronze-silver-gold-hourly-incremental")
def bronze_silver_gold_flow() -> None:
    """
    Run the incremental Bronze → Silver → Gold pipeline in sequence.
    
    Bronze:
      - Facilities
      - Timeseries (energy)
      - Weather
      - Air quality
    
    Silver:
      - Facility master
      - Hourly energy
      - Hourly weather
      - Hourly air quality
    
    Gold:
      - dim_date
      - fact_solar_environmental
    """
    logger = get_run_logger()

    # Bronze
    logger.info("Starting Bronze layer incremental load...")
    # run_bronze_facilities_loader()
    run_bronze_timeseries_loader()
    run_bronze_weather_loader()
    run_bronze_air_quality_loader()
    logger.info("Bronze layer incremental load completed.")

    # Silver
    logger.info("Starting Silver layer incremental transformation...")
    # run_silver_facility_master_loader()
    run_silver_hourly_energy_loader()
    run_silver_hourly_weather_loader()
    run_silver_hourly_air_quality_loader()
    logger.info("Silver layer incremental transformation completed.")

    # Gold
    logger.info("Starting Gold layer incremental transformation...")
    run_gold_dim_date_loader()
    run_gold_fact_solar_environmental_loader()
    logger.info("Gold layer incremental transformation completed.")
    logger.info("Bronze → Silver → Gold hourly incremental pipeline finished successfully.")


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run or manage the Bronze-Silver-Gold hourly incremental Prefect flow."
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
    """Build and apply a Prefect deployment with cron scheduling (minute 1 past every hour, UTC)."""

    schedule = CronSchedule(cron="1 * * * *", timezone="UTC")

    deployment = Deployment.build_from_flow(
        flow=bronze_silver_gold_flow,
        name=deployment_name,
        work_pool_name=work_pool,
        schedule=schedule,
        description="Hourly incremental Bronze → Silver → Gold ETL pipeline (runs at :01 of every hour)",
        tags=["bronze", "silver", "gold", "incremental", "hourly", "etl"],
    )
    deployment.apply()

    print(f"Deployment '{deployment_name}' created successfully!")
    print(f"Schedule: Every hour at minute 1 (e.g., 21:01, 22:01, 23:01)")
    print(f"Cron Expression: 1 * * * * (UTC timezone)")
    print(f"Work Pool: {work_pool}")
    print("\nTo start the deployment, run:")
    print(f"  prefect deployment run 'bronze-silver-gold-hourly-incremental/{deployment_name}'")


if __name__ == "__main__":
    args = _parse_cli_args()

    if args.deploy:
        _build_and_apply_deployment(args.work_pool, args.deployment_name)

    if not args.skip_run:
        bronze_silver_gold_flow()
