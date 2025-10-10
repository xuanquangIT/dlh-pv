#!/usr/bin/env bash
set -euo pipefail
services=(minio postgres iceberg-rest trino mlflow prefect)
fail=0
for s in "${services[@]}"; do
  st=$(docker compose ps --format json | jq -r ".[] | select(.Name==\"$s\") | .State")
  echo "- $s: $st"
  if [[ "$st" != "running" ]]; then fail=1; fi
done
exit $fail