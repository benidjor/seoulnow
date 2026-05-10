#!/usr/bin/env bash
# Day 1+ infra healthcheck. Exits with the number of failed sections.
set -uo pipefail

_fail=0

echo "== docker compose ps =="
docker compose ps || { echo "FAIL"; _fail=$((_fail+1)); }

echo
echo "== Kafka =="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list || { echo "FAIL"; _fail=$((_fail+1)); }

echo
echo "== Postgres =="
docker compose exec -T postgres pg_isready -U scp -d scp || { echo "FAIL"; _fail=$((_fail+1)); }

echo
echo "== MinIO =="
curl -sf http://localhost:9000/minio/health/ready && echo OK || { echo FAIL; _fail=$((_fail+1)); }

echo
echo "== Lakekeeper =="
curl -sf http://localhost:8181/health && echo OK || { echo FAIL; _fail=$((_fail+1)); }

echo
echo "== Airflow =="
curl -sf http://localhost:8080/health && echo OK || { echo FAIL; _fail=$((_fail+1)); }

echo
echo "== summary =="
echo "failed sections: ${_fail}"
exit "${_fail}"
