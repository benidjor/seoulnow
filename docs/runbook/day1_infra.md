# Day 1 Infra Runbook

## 평소 기동
```bash
docker compose up -d
./scripts/healthcheck.sh
```

## 정지
```bash
docker compose down            # 컨테이너만 정지, volume 유지
docker compose down -v         # volume 까지 삭제 (데이터 전부 날아감)
```

## 로그 확인
```bash
docker compose logs -f kafka
docker compose logs -f lakekeeper
```

## 메모리 모니터
```bash
./scripts/memory_watch.sh                # 80% 기본
THRESHOLD=70 ./scripts/memory_watch.sh   # 70% 로 더 빡빡하게
```

## 자주 발생하는 문제

| 증상 | 원인 / 조치 | 상세 |
|---|---|---|
| `lakekeeper` healthcheck 가 계속 실패 | Postgres 마이그레이션 지연 — 60초 더 대기. 그래도 실패하면 `docker compose logs lakekeeper` 확인 후 spec §9-1 fallback (JdbcCatalog) 발동 | [Issue 1](../portfolio/troubleshooting/2026-05-01-lakekeeper-v05-setup.md#issue-1--schema-migration-이-자동-실행되지-않음) |
| `lakekeeper` 컨테이너가 영원히 `(health: starting)` | Distroless 이미지 — `curl` 없음. healthcheck 가 binary 직접 호출 형태인지 확인 | [Issue 2](../portfolio/troubleshooting/2026-05-01-lakekeeper-v05-setup.md#issue-2--distroless-이미지라-plan-의-curl-기반-healthcheck-불가) |
| Lakekeeper warehouse 등록 시 storage validation 실패 | `MINIO_ENDPOINT` 가 컨테이너 내부에서 잘못된 호스트네임 — `http://minio:9000` 가 docker network 안 정답 | [Issue 3](../portfolio/troubleshooting/2026-05-01-lakekeeper-v05-setup.md#issue-3--lakekeeper-container-가-docker-network-안에서-minio-에-접근) |
| `RuntimeError: no project found in Lakekeeper` | Fresh instance 의 server-level bootstrap 미호출 — `bootstrap.py` 의 `ensure_server_bootstrapped()` 실행 | [Issue 4](../portfolio/troubleshooting/2026-05-01-lakekeeper-v05-setup.md#issue-4--fresh-instance-는-server-level-bootstrap-필요) |
| `kafka-topics.sh` 가 connection refused | Kafka KRaft 컨트롤러가 아직 안 떴음. 30초 대기 후 재시도 | — |
| MinIO 콘솔 접속 안 됨 | 9001 포트 충돌. `lsof -i :9001` 로 확인 | — |
| 메모리 80% 초과 | Spark 가 떠 있는지 확인 (Day 9 외에는 안 떠 있어야 함). 또는 Flink TaskManager heap 축소 | — |
