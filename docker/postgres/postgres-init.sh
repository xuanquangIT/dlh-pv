#!/bin/bash
#set -euo pipefail

# Note: during container init the shell may run with different shells; avoid -u
set -eo pipefail

echo "Running custom postgres init script"

# This script runs inside the official Postgres image during initialization.
# It creates the application role and databases (idempotent).

# Use the configured POSTGRES_USER (the created superuser during init) to run
# database creation commands. This avoids assuming a 'postgres' role exists.
PSQL_OPTS="-v ON_ERROR_STOP=1 -U ${POSTGRES_USER} -d postgres"

## Original line included 'prefect' — commented out to disable Prefect DB creation
## for DB in iceberg mlflow prefect; do
for DB in iceberg mlflow; do
  echo "Ensuring database ${DB} exists and owned by ${POSTGRES_USER}"
  psql ${PSQL_OPTS} -c "CREATE DATABASE \"${DB}\" OWNER \"${POSTGRES_USER}\";" || true
  psql ${PSQL_OPTS} -c "GRANT ALL PRIVILEGES ON DATABASE \"${DB}\" TO \"${POSTGRES_USER}\";" || true
done

# Optional: list databases for debug
psql -U "${POSTGRES_USER}" -c "\l"

echo "Postgres init script finished"
