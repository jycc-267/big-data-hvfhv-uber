#!/usr/bin/env bash
# Runs one Spark batch job against the local docker-compose stack.
#
# Usage: scripts/spark-job.sh <main class> [job args]
#   scripts/spark-job.sh hvfhv.batch.IngestMasterDataset
#   scripts/spark-job.sh hvfhv.batch.BuildBatchView --watermark 20220102
set -euo pipefail

if (( $# < 1 )); then
  echo "Usage: $0 <main class> [job args]" >&2
  exit 2
fi
class="$1"
shift

cd "$(dirname "$0")/.."
docker compose -f deploy/docker-compose.yml --profile jobs run --rm -T spark \
  /opt/spark/bin/spark-submit \
  --master 'local[4]' \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.ui.enabled=false \
  --class "${class}" \
  /jobs/hvfhv-batch.jar "$@"
