#!/usr/bin/env bash
# Creates the Hive master tables (deploy/hive/master_tables.hql) through HiveServer2.
# Safe to re-run: every statement is CREATE ... IF NOT EXISTS.
set -euo pipefail
cd "$(dirname "$0")/.."

# beeline is chatty even when it succeeds; only show its output if it fails.
if ! out=$(docker compose -f deploy/docker-compose.yml exec -T hiveserver2 \
  beeline -u jdbc:hive2://localhost:10000/ --silent=true -f /opt/ddl/master_tables.hql 2>&1); then
  echo "${out}" >&2
  exit 1
fi
echo "Hive master tables are ready: hvfhv.trips_bulk, hvfhv.trips_stream, hvfhv.zones, view hvfhv.trips"
