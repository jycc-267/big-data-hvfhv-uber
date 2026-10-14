#!/usr/bin/env bash
# Downloads the NYC taxi zone lookup into data/raw/zones.
set -euo pipefail

URL="${ZONE_LOOKUP_URL:-https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv}"
OUT_DIR="${OUT_DIR:-data/raw/zones}"

mkdir -p "${OUT_DIR}"
curl --fail --silent --show-error --retry 5 --retry-delay 5 -o "${OUT_DIR}/taxi_zone_lookup.csv.tmp" "${URL}"
mv "${OUT_DIR}/taxi_zone_lookup.csv.tmp" "${OUT_DIR}/taxi_zone_lookup.csv"
echo "Zone lookup: $(($(wc -l < "${OUT_DIR}/taxi_zone_lookup.csv") - 1)) zones in ${OUT_DIR}"
