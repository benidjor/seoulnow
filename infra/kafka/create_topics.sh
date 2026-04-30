#!/usr/bin/env bash
set -euo pipefail

KCMD="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

create_topic() {
  local name=$1
  local partitions=$2
  local retention_ms=$3
  if $KCMD --list | grep -Fxq -- "${name}"; then
    echo "topic ${name} already exists, skipping"
    return
  fi
  $KCMD --create \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention_ms}" \
    --config "compression.type=lz4"
  echo "created: ${name}"
}

# spec §4-3
create_topic "seoul.hotspot.congestion.v1" 3 604800000   # 7d
create_topic "seoul.transit.subway.v1"     6 259200000   # 3d  (트래픽 많음)
create_topic "place.master.cdc.v1"         1 2592000000  # 30d (Day 6 에서 사용)

# Phase 1B 에서 사용 — 스켈레톤만 미리 생성해 둔다
create_topic "user.events.v1"              3 604800000   # 7d

echo
echo "== topic list =="
$KCMD --list
