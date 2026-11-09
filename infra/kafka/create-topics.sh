#!/bin/bash
# Creates all project topics. Idempotent: safe to run on every `docker compose up`.
# Partition/retention reasoning lives in docs/design-decisions.md.
set -euo pipefail

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
KT=/opt/kafka/bin/kafka-topics.sh

HOUR=3600000
DAY=86400000

create_topic() {
  local name=$1 partitions=$2 retention_ms=$3
  echo ">> topic=${name} partitions=${partitions} retention.ms=${retention_ms}"
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms="$retention_ms"
}

#            name             partitions  retention
create_topic hello-world      3           $((1 * HOUR))
create_topic driver-location  6           $((1 * DAY))
create_topic order-events     3           $((7 * DAY))
create_topic alerts           3           $((3 * DAY))
create_topic dead-letter      1           $((14 * DAY))

echo ">> Final topic layout:"
"$KT" --bootstrap-server "$BOOTSTRAP" --describe
