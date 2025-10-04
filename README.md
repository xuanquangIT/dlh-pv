# PV Lakehouse (Starter)

A minimal, batteries‑included starter for building a small lakehouse ETL on your laptop or a single VM. It uses open components and clear conventions so you can evolve from raw → normalized → curated data with confidence.

Stack: MinIO (S3‑compatible object store), Apache Iceberg (REST catalog), Spark (batch), Trino (SQL), Prefect (orchestration), MLflow (tracking), Postgres (metastore/metadata)

Medallion: Bronze (raw) → Silver (normalized) → Gold (curated/KPI)

---

## Repository layout (important files)

- `docker/etl-runner/` — ETL runner Dockerfile and requirements
- `docker/minio/init/` — MinIO init scripts (bucket/policy helpers)
- `docker-compose.yml` — root Compose file used by examples
- `flows/` — sample Prefect flows (lightweight examples)
- `sql/trino/` — DDL for creating schemas
- `src/pv_lakehouse/etl/` — ETL modules and entrypoints
- `tests/` — basic pytest smoke tests
- `.env.example` — environment template (copy to `.env` and fill secrets)
- `requirements-dev.txt` — developer tooling (pre-commit, ruff, black, pytest)
- `Makefile` — convenience tasks: `init`, `lint`, `test`, `ci`, `smoke`
- `.pre-commit-config.yaml` — configured hooks
- `.githooks/pre-push` — local pre-push hook (runs lint + tests)
- `scripts/smoke.sh` — optional helper to bring up Compose and check services
- `.github/workflows/ci.yml` — GitHub Actions job (runs lint + tests)

---

## Quick start

Requirements: Python 3.11+ (for development), Docker & Docker Compose (for running services)

Clone and open the repo:

```bash
git clone https://github.com/xuanquangIT/dlh-pv.git
cd dlh-pv
```

Option A — run locally with Python (developer flow):

```bash
# create & activate venv (Linux/macOS shown)
python -m venv .venv
source .venv/bin/activate

# install runtime deps (for ETL runner)
pip install -r docker/etl-runner/requirements.txt

# run tests
pytest -q
```

Option B — use Docker Compose to bring up services (MinIO, optional others):

```bash
# copy env template and edit values
cp .env.example .env

# start services using root docker-compose.yml
docker compose up -d
# or explicitly: docker compose -f docker-compose.yml up -d
```

Initialize MinIO buckets (if you use the provided init helper):

```bash
./docker/minio/init/create-buckets.sh
# or: docker compose run --rm minio-init
```

---

## Configuration

Copy `.env.example` to `.env` and update secrets/URLs. Example variables included in `.env.example`:

- MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, S3_REGION, S3_PATH_STYLE
- ICEBERG_REST_URI, ICEBERG_WAREHOUSE, ICEBERG_CATALOG
- TRINO_HOST, TRINO_PORT, TRINO_USER
- SPARK_MASTER_URL, SPARK_APP_NAME
- PREFECT_API_URL, PREFECT_WORK_POOL
- MLFLOW_TRACKING_URI
- POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
- OPEN_NEM_* and OPENMETEO_* (optional external APIs)

Never commit real secrets. Use `.env.example` as the template only.

---

## Development workflow (recommended)

We provide local pre-commit hooks and a Makefile to keep checks fast and consistent.

1) Install developer dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

2) Enable pre-commit and local pre-push hook

```bash
pre-commit install
git config core.hooksPath .githooks
# On Unix: make hooks/scripts executable
chmod +x .githooks/pre-push scripts/smoke.sh
```

3) Quick checks

```bash
make ci     # runs lint (ruff/black) and tests
make smoke  # optional: start compose and validate services via scripts/smoke.sh
```

Makefile targets:
- `init` — install dev deps and install pre-commit hooks
- `lint` — run ruff lint + format check
- `test` — run pytest
- `ci` — run lint + tests
- `smoke` — run `scripts/smoke.sh` (docker-compose based)

Pre-push hook (`.githooks/pre-push`) runs `ruff` and `pytest` and will block pushes on failures.

Note: on Windows `chmod` isn't required; ensure Git preserves executable bits or run the scripts via PowerShell.

---

## CI / GitHub Actions

The repository includes `.github/workflows/ci.yml` which:

- checks out the code
- sets up Python 3.11
- installs developer requirements
- runs `pre-commit` and `make ci` (lint + tests)

Cost note:
- Public repos using GitHub-hosted runners: free for standard runners.
- Private repos: have a free quota depending on plan; excess minutes billed.
- Self-hosted runners: you provide compute and Actions minutes are not billed by GitHub.

If you want zero cost, rely on local pre-commit hooks or run workflows on self-hosted runners.

---

## Troubleshooting

- If linters fail locally, run `pre-commit run --all-files` to see/auto-fix issues.
- If the GitHub Actions job fails due to TOML parsing, ensure `pyproject.toml` has a single `[build-system]` block (we keep one in this repo).
- For S3A/MinIO issues, set `S3_PATH_STYLE=true` in `.env` and validate endpoint/credentials.

---

## Contributing

Send PRs for improvements. Run `make ci` before pushing; the pre-push hook will also run checks automatically.

---

## License

MIT (see `LICENSE`)

