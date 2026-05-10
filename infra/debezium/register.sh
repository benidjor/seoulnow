#!/usr/bin/env bash
# Day 6 Task 6.1/6.2 — Debezium Postgres connector 등록 자동화.
#
# 멱등 동작: 이미 등록되어 있으면 DELETE 후 재등록.
# slot 가드: stale replication slot 이 남아 있으면 connector 재등록이
# "replication slot already exists" 로 실패하므로 명시적 drop.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG="$(dirname "$0")/connector-places.json"

echo "== existing connectors =="
curl -sf "${CONNECT_URL}/connectors" || true
echo

# stale slot 가드 — replication slot 이 이전 시도에서 남아있으면 connector
# 등록이 실패. DELETE 만으로는 PG slot 이 즉시 release 안 되는 케이스가 있어
# 명시 drop 옵션 제공.
if curl -sf "${CONNECT_URL}/connectors/scp-pg-places" >/dev/null 2>&1; then
  echo "deleting existing scp-pg-places ..."
  curl -X DELETE "${CONNECT_URL}/connectors/scp-pg-places"
  sleep 2
fi

# 안전망 — slot 강제 drop (이미 없으면 silent fail).
docker compose exec -T postgres psql -U scp -d scp \
  -c "SELECT pg_drop_replication_slot('scp_places_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='scp_places_slot');" \
  >/dev/null 2>&1 || true

echo "registering scp-pg-places ..."
curl -sf -X POST -H "Content-Type: application/json" \
  --data @"${CONFIG}" "${CONNECT_URL}/connectors" | head -c 500
echo

echo
echo "== status =="
sleep 3
curl -sf "${CONNECT_URL}/connectors/scp-pg-places/status"
