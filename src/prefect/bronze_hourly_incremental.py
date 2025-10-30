"""Prefect flow to orchestrate hourly Bronze incremental loaders via spark-submit.

This module invokes the existing bronze ETL scripts inside the spark-master container
using docker compose. Each task runs in incremental mode and can be scheduled to run
hourly using Prefect deployments.
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

_DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = os.environ.get("PV_LAKEHOUSE_ROOT") or str(_DEFAULT_PROJECT_ROOT)
DOCKER_COMPOSE_FILE = os.environ.get("DOCKER_COMPOSE_FILE") or str(
    Path(PROJECT_ROOT) / "docker" / "docker-compose.yml"
)
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit")
DOCKER_COMPOSE_CMD = os.environ.get("DOCKER_COMPOSE_CMD", "docker compose")
DEFAULT_WORK_POOL = os.environ.get("PREFECT_WORK_POOL", "default-pool")
DEFAULT_DEPLOYMENT_NAME = "bronze-hourly"

# Script paths inside the spark-master container
BRONZE_SCRIPTS = {
    "facilities": f"{PROJECT_ROOT}/src/pv_lakehouse/etl/bronze/load_facilities.py",
    "air_quality": f"{PROJECT_ROOT}/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py",
    "timeseries": f"{PROJECT_ROOT}/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py",
    "weather": f"{PROJECT_ROOT}/src/pv_lakehouse/etl/bronze/load_facility_weather.py",
}


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
    if result.stdout:
        get_run_logger().info(result.stdout.rstrip())
    if result.stderr:
        get_run_logger().warning(result.stderr.rstrip())
    if result.returncode != 0:
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


@task(name="run_facilities_loader")
def run_facilities_loader() -> None:
    command = _build_shell_command(
        BRONZE_SCRIPTS["facilities"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="run_air_quality_loader")
def run_air_quality_loader() -> None:
    command = _build_shell_command(
        BRONZE_SCRIPTS["air_quality"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="run_timeseries_loader")
def run_timeseries_loader() -> None:
    command = _build_shell_command(
        BRONZE_SCRIPTS["timeseries"],
        ["--mode", "incremental", "--interval", "1h", "--metrics", "power,energy"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@task(name="run_weather_loader")
def run_weather_loader() -> None:
    command = _build_shell_command(
        BRONZE_SCRIPTS["weather"],
        ["--mode", "incremental"],
    )
    _run_subprocess(command, env_overrides=[("PYTHONPATH", "/opt/workdir/src")])


@flow(name="bronze-hourly-incremental")
def bronze_hourly_incremental_flow() -> None:
    """Run all Bronze incremental loaders sequentially."""

    run_facilities_loader()
    run_timeseries_loader()
    run_weather_loader()
    run_air_quality_loader()


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run or manage the Bronze hourly incremental Prefect flow."
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Build and apply a Prefect deployment targeting the configured work pool.",
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
    deployment = Deployment.build_from_flow(
        flow=bronze_hourly_incremental_flow,
        name=deployment_name,
        work_pool_name=work_pool,
    )
    deployment.apply()


if __name__ == "__main__":
    args = _parse_cli_args()

    if args.deploy:
        _build_and_apply_deployment(args.work_pool, args.deployment_name)

    if not args.skip_run:
        bronze_hourly_incremental_flow()
