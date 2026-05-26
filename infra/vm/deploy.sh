#!/usr/bin/env bash
# VM 갱신 배포 — git pull + 인프라 compose(MVP 서비스만) + 호스트 프로세스 restart.
# GitHub Actions(ssh) + 수동 양쪽에서 사용. 첫 부트스트랩은 runbook 참조.
set -euo pipefail

cd /home/ubuntu/seoulnow
git pull --ff-only

# 인프라 compose — MVP 서비스만 (airflow / kafka-connect 제외).
docker compose up -d kafka postgres minio minio-bootstrap lakekeeper-migrate lakekeeper

# 호스트 프로세스 재시작.
sudo systemctl restart \
  seoulnow-hotspot-producer \
  seoulnow-bronze-silver \
  seoulnow-silver-gold \
  seoulnow-api

echo "deploy done."
