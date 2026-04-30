#!/usr/bin/env bash
set -euo pipefail

echo "== docker compose ps =="
docker compose ps

echo
echo "== Kafka =="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list || echo "FAIL"

echo
echo "== Postgres =="
docker compose exec -T postgres pg_isready -U scp -d scp || echo "FAIL"

echo
echo "== MinIO =="
curl -sf http://localhost:9000/minio/health/ready && echo OK || echo FAIL

echo
echo "== Lakekeeper =="
curl -sf http://localhost:8181/health && echo OK || echo FAIL
