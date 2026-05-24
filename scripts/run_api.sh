#!/usr/bin/env bash
# 호스트(노트북)에서 FastAPI 실행. compose 네트워크 밖이라 minio docker hostname
# 대신 localhost 고정 — AWS C++ SDK(pyiceberg PyArrow S3)가 /etc/hosts alias 를
# 신뢰성 있게 안 쓰는 Issue 1 회피 (정찰 §2). 환경변수 > .env 우선순위로
# .env 의 MINIO_ENDPOINT=http://minio:9000 을 안전하게 override.
#
# 사용: bash scripts/run_api.sh            (기본 127.0.0.1:8000)
#       bash scripts/run_api.sh --reload   (추가 uvicorn 인자 전달)
set -euo pipefail

exec env \
  MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}" \
  LAKEKEEPER_URL="${LAKEKEEPER_URL:-http://localhost:8181}" \
  PYTHONPATH="${PYTHONPATH:-src}" \
  uvicorn api.main:app --host 127.0.0.1 --port 8000 "$@"
