#!/usr/bin/env bash
# Downloads the historical part of the trip data: every trip requested in
# [HISTORICAL_FROM, SPLIT_DATETIME), as CSV pages, into data/raw/trips.
#
# SPLIT_DATETIME is where the producer starts streaming, so the batch and speed
# layers never see the same trip twice. Keep it at midnight: the batch watermark is a
# request date, and a historical load that ends mid-day would leave that day's
# earlier trips in neither layer.
#
# Usage: HISTORICAL_FROM=2022-01-01T18:00:00 SPLIT_DATETIME=2022-01-02T00:00:00 scripts/download_sample.sh
set -euo pipefail

HISTORICAL_FROM="${HISTORICAL_FROM:-2022-01-01T18:00:00}"
SPLIT_DATETIME="${SPLIT_DATETIME:-2022-01-02T00:00:00}"
OUT_DIR="${OUT_DIR:-data/raw/trips}"
PAGE_SIZE="${PAGE_SIZE:-50000}"
ENDPOINT="${SOCRATA_CSV_ENDPOINT:-https://data.cityofnewyork.us/resource/g6pj-fsah.csv}"

if [[ "${SPLIT_DATETIME}" != *T00:00:00 ]]; then
  echo "SPLIT_DATETIME must be at midnight (yyyy-MM-ddT00:00:00), got ${SPLIT_DATETIME}" >&2
  exit 1
fi

auth=()
if [[ -n "${SOCRATA_APP_TOKEN:-}" ]]; then
  auth=(-H "X-App-Token: ${SOCRATA_APP_TOKEN}")
fi

# Start clean so a re-run with a different window never mixes old pages in.
rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

offset=0
while :; do
  page="${OUT_DIR}/trips_$(printf '%09d' "${offset}").csv"
  curl --fail --silent --show-error --retry 5 --retry-delay 5 --max-time 600 -G "${ENDPOINT}" \
    "${auth[@]+"${auth[@]}"}" \
    --data-urlencode "\$where=request_datetime >= '${HISTORICAL_FROM}' AND request_datetime < '${SPLIT_DATETIME}'" \
    --data-urlencode "\$order=request_datetime,:id" \
    --data-urlencode "\$limit=${PAGE_SIZE}" \
    --data-urlencode "\$offset=${offset}" \
    -o "${page}"

  # awk also counts a last line without a trailing newline; an off-by-one here would
  # shift every following page by one trip.
  rows=$(($(awk 'END { print NR }' "${page}") - 1)) # minus the header
  if (( rows <= 0 )); then
    rm -f "${page}"
    break
  fi
  offset=$((offset + rows))
  echo "Downloaded ${offset} trips"
  if (( rows < PAGE_SIZE )); then
    break
  fi
done

echo "Historical sample: ${offset} trips requested in [${HISTORICAL_FROM}, ${SPLIT_DATETIME}) in ${OUT_DIR}"
