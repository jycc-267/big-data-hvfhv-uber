#!/usr/bin/env bash
# End-to-end consistency test on the local docker-compose stack.
#
#  1. historical load [HISTORICAL_FROM, SPLIT_DATETIME) -> Hive trips_bulk -> batch view #1
#  2. stream [SPLIT_DATETIME, STREAM_UNTIL) through Kafka into the speed layer, taking the
#     whole stack down (keeping volumes) mid-stream: everything must resume where it stopped
#  3. ReconcileCheck: batch view + speed view == full recompute from the master dataset
#  4. batch view #2 absorbs the landing zone into Hive trips_stream and moves the watermark;
#     HiveQL row counts must equal the downloaded rows and every Kafka message; reconcile
#  5. batch view #3 on unchanged data (idempotency + speed-row cleanup); reconcile again
#  6. query the web app for a real route
#
# Wipes the local pipeline state (every volume except the spark-ivy download cache; the
# downloads in ./data/raw are kept). Re-uses downloaded CSVs when the window has not changed. Usage: scripts/e2e.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export HISTORICAL_FROM="${HISTORICAL_FROM:-2022-01-01T18:00:00}"
export SPLIT_DATETIME="${SPLIT_DATETIME:-2022-01-02T00:00:00}"
export STREAM_UNTIL="${STREAM_UNTIL:-2022-01-03T02:00:00}"
export PAGE_SIZE="${STREAM_PAGE_SIZE:-25000}"
export POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-1}"
TOPIC=hvfhv-trips

compose() { docker compose -f deploy/docker-compose.yml --profile stream "$@"; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mE2E FAILED: %s\033[0m\n' "$*" >&2; exit 1; }

cql() { # rows of a CQL query, columns separated by "|", whitespace trimmed
  compose exec -T cassandra cqlsh -e "$1" \
    | awk '/^-+(\+-+)*$/ { body = 1; next } body && NF && !/^\([0-9]+ rows\)$/ { print }' \
    | sed -E 's/^ +//; s/ *\| */|/g; s/ +$//'
}

hive_count() { # SELECT count(*) through HiveServer2
  compose exec -T hiveserver2 beeline -u jdbc:hive2://localhost:10000/ --silent=true \
    --outputformat=tsv2 --showHeader=false -e "SELECT count(*) FROM $1" 2>/dev/null | grep -E '^[0-9]+$'
}

meta() { # the serving pointer, as "batch_table watermark"
  cql "SELECT batch_table, watermark FROM hvfhv.serving_pointer WHERE name = 'serving'" | tr '|' ' '
}

topic_end_offset() {
  compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "${TOPIC}" \
    | awk -F: '{ total += $3 } END { print total + 0 }'
}

checkpoint_offset() { # Kafka offset fully processed by a streaming query
  # The checkpoints live in the pipeline-data volume: print the offsets of the newest
  # committed micro-batch from a throwaway container, then sum them here.
  # shellcheck disable=SC2016 # expanded by the container's shell
  compose --profile jobs run --rm --no-deps -T --entrypoint bash spark -c '
    root=/data/checkpoints/$1
    last=$(ls "${root}/commits" 2>/dev/null | grep -E "^[0-9]+$" | sort -n | tail -1)
    [[ -z "${last}" ]] || sed -n 3p "${root}/offsets/${last}"' _ "$1" 2>/dev/null \
    | python3 -c '
import json, sys
line = sys.stdin.read().strip()
print(sum(int(v) for p in json.loads(line).values() for v in p.values()) if line else 0)'
}

wait_for_drain() {
  local end; end=$(topic_end_offset)
  for _ in $(seq 1 120); do
    local archived speed
    archived=$(checkpoint_offset archive); speed=$(checkpoint_offset speed-view)
    if (( archived == end && speed == end )); then
      echo "Speed layer caught up: ${end} messages archived and applied"
      return
    fi
    echo "Waiting for the speed layer: topic=${end} archive=${archived} speed-view=${speed}"
    sleep 5
  done
  fail "speed layer did not catch up"
}

step "Build jars and images"
./mvnw -B -q package -DskipTests
compose build -q hive-metastore webapp

step "Download data (cached per window)"
window_file=data/raw/trips/.window
if [[ "$(cat "${window_file}" 2>/dev/null)" != "${HISTORICAL_FROM} ${SPLIT_DATETIME}" ]]; then
  scripts/download_sample.sh
  echo "${HISTORICAL_FROM} ${SPLIT_DATETIME}" > "${window_file}"
fi
[[ -f data/raw/zones/taxi_zone_lookup.csv ]] || scripts/get_zone_lookup.sh

step "Reset local state"
compose down --remove-orphans >/dev/null 2>&1 || true
# Like `down -v`, but keeps spark-ivy: it caches the Hive client Spark downloads on first use.
docker volume rm -f hvfhv_pipeline-data hvfhv_kafka-data hvfhv_metastore-db hvfhv_cassandra-data >/dev/null

start_services() { compose up -d --wait kafka hive-metastore hiveserver2 cassandra webapp; }

step "Start Kafka, Hive, Cassandra and the web app"
start_services
scripts/hive-init.sh
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic "${TOPIC}" --partitions 1 --replication-factor 1

step "Batch layer: Hive master dataset, lookups, batch view #1"
scripts/spark-job.sh hvfhv.batch.IngestMasterDataset
scripts/spark-job.sh hvfhv.batch.LoadLookups
scripts/spark-job.sh hvfhv.batch.BuildBatchView
read -r table1 watermark1 <<<"$(meta)"
echo "Serving ${table1} with watermark ${watermark1}"
[[ "${watermark1}" == "${SPLIT_DATETIME:0:4}${SPLIT_DATETIME:5:2}${SPLIT_DATETIME:8:2}" ]] \
  || fail "watermark #1 should be the split date, got ${watermark1}"

step "Speed layer: stream ${SPLIT_DATETIME} .. ${STREAM_UNTIL}, with a full-stack restart mid-stream"
compose up -d speed producer
sleep 45
compose stop producer >/dev/null # freeze the topic so its size can be compared across down/up
before=$(topic_end_offset)
echo "Taking the whole stack down (volumes kept) after ${before} messages"
compose down
start_services
after=$(topic_end_offset)
[[ "${after}" == "${before}" ]] || fail "Kafka lost messages across down/up (${before} -> ${after})"
echo "Kafka kept all ${after} messages; producer resumes from its saved offset, speed job from its checkpoint"
compose up -d speed producer
compose wait producer >/dev/null || fail "producer exited with an error"
wait_for_drain

step "Reconcile #1: batch view #1 + speed view"
scripts/spark-job.sh hvfhv.batch.ReconcileCheck

step "Batch view #2 absorbs the streamed trips into Hive"
scripts/spark-job.sh hvfhv.batch.BuildBatchView
read -r table2 watermark2 <<<"$(meta)"
echo "Serving ${table2} with watermark ${watermark2}"
[[ "${watermark2}" > "${watermark1}" ]] || fail "watermark did not advance (${watermark1} -> ${watermark2})"

downloaded=$(awk 'FNR > 1' data/raw/trips/*.csv | wc -l | tr -d ' ')
streamed=$(topic_end_offset)
bulk=$(hive_count hvfhv.trips_bulk)
stream=$(hive_count hvfhv.trips_stream)
echo "HiveQL: trips_bulk=${bulk} (downloaded ${downloaded}), trips_stream=${stream} (Kafka messages ${streamed})"
[[ "${bulk}" == "${downloaded}" ]] || fail "Hive trips_bulk does not hold every downloaded trip"
[[ "${stream}" == "${streamed}" ]] || fail "Hive trips_stream does not hold every streamed trip exactly once"
scripts/spark-job.sh hvfhv.batch.ReconcileCheck

step "Batch view #3 on unchanged data: idempotent, retires view #1 and old speed rows"
scripts/spark-job.sh hvfhv.batch.BuildBatchView
read -r table3 watermark3 <<<"$(meta)"
[[ "${watermark3}" == "${watermark2}" ]] || fail "watermark changed without new data"
[[ "$(hive_count hvfhv.trips_stream)" == "${streamed}" ]] || fail "re-absorbing the landing zone changed trips_stream"
scripts/spark-job.sh hvfhv.batch.ReconcileCheck
if [[ -n "$(cql "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'hvfhv' AND table_name = '${table1}'")" ]]; then
  fail "${table1} should have been dropped"
fi

step "Web app answers a real route"
route=$(cql "SELECT carrier, pickup_zone, dropoff_zone, hour_in_day FROM hvfhv.${table3} LIMIT 1")
IFS='|' read -r carrier pickup dropoff hour <<<"${route}"
html=$(curl -fsS -G http://localhost:3012/trip-results.html \
  --data-urlencode "carrier=${carrier}" --data-urlencode "pickup_location=${pickup}" \
  --data-urlencode "dropoff_location=${dropoff}" --data-urlencode "hour=${hour}")
grep -q "trips:" <<<"${html}" || fail "web app returned no results for ${route}"
echo "${route}: $(grep -o '[0-9]* trips:[^<]*' <<<"${html}" | tr -s ' \n' ' ')"

compose stop speed >/dev/null
printf '\n\033[32mE2E PASSED\033[0m: batch and speed views reconcile with the Hive master dataset\n'
