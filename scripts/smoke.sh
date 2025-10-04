#!/usr/bin/env bash
set -euo pipefail
docker compose up -d
echo "Waiting services..."
sleep 8
docker compose ps
echo "SMOKE OK"
