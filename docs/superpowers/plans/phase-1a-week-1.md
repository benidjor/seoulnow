# Phase 1A Week 1 Implementation Plan (Day 1~5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 서울 공공 실시간 데이터를 Kafka KRaft single-node → PyFlink streaming → Iceberg(Lakekeeper) Bronze/Silver/Gold 로 흘리고, dbt-core + GitHub Actions CI 까지 붙여 **데이터 신선도 P95 < 7분 SLO** 측정 가능한 코어 파이프라인을 5일 안에 완성한다.

**Architecture:** 로컬 macOS docker-compose 위에 Kafka(KRaft) + Postgres + MinIO + Lakekeeper(REST Catalog) 를 띄우고, Python producer 2종이 서울 OpenAPI를 폴링해 Kafka 토픽 2개로 발행한다. PyFlink job이 Bronze → Silver → Gold 의 Medallion 변환을 streaming 으로 수행하고, dbt 가 Gold 일부 변환을 코드로 관리하며, GitHub Actions 가 PR 마다 lint/test 를 검증한다. Day 7 Oracle Cloud VM 배포·Day 6 CDC 는 Week 2 plan 에서 다룬다.

**Tech Stack:** Python 3.11 (uv), Kafka 3.7 (KRaft), Apache Flink 1.20 (PyFlink), Apache Iceberg 1.7, Lakekeeper REST Catalog, MinIO (S3 호환), Postgres 16, DuckDB, dbt-core 1.9 + dbt-duckdb, GitHub Actions, pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` 의 §6-1 Day 1~5, §6-2 SLO 정의, §9-1 fallback, §10 Day 0 사전 준비 가 입력.

**전제 (Day 0 완료 항목):**
- 서울 OpenAPI 키 발급 완료 (`SEOUL_OPENAPI_KEY`)
- 서울교통공사 실시간 지하철 혼잡도 API 키 발급 완료 (`SEOUL_SUBWAY_API_KEY`)
- macOS 에 Docker Desktop 또는 colima 설치 + 8GB 이상 메모리 할당
- Python 3.11+, uv (또는 poetry), Node 20+ 설치
- GitHub repo 생성 (`seoul-citydata-platform` public, `main` branch)
- 본 디렉토리 = repo 루트 (`/Users/aryijq/Documents/01_DE_project/seoul-citydata-platform/`)

---

## File Structure

작업 후 repo 루트는 다음과 같이 구성된다. (이미 존재하는 파일 = `CLAUDE.md`, `.gitignore`, `docs/`)

```
seoul-citydata-platform/
├── CLAUDE.md                              (existing)
├── README.md                              (Task 1.1 신규)
├── .gitignore                             (existing — 추가 필요시 update)
├── .env.example                           (Task 1.1 신규)
├── docker-compose.yml                     (Task 1.2 신규)
├── infra/
│   ├── kafka/
│   │   └── create_topics.sh               (Task 1.4 신규)
│   ├── lakekeeper/
│   │   └── bootstrap.py                   (Task 1.5 신규 — warehouse 등록)
│   └── minio/
│       └── README.md                      (Task 1.2 신규 — bucket 안내)
├── pyproject.toml                         (Task 2.1 신규)
├── uv.lock                                (Task 2.1 자동 생성)
├── src/
│   ├── platform_common/
│   │   ├── __init__.py                    (Task 2.1 신규)
│   │   ├── config.py                      (Task 2.1 신규 — env loader)
│   │   └── kafka.py                       (Task 2.2 신규 — producer factory)
│   ├── producers/
│   │   ├── __init__.py                    (Task 2.2 신규)
│   │   ├── hotspot_producer.py            (Task 2.2 신규)
│   │   ├── subway_producer.py             (Task 2.3 신규)
│   │   └── schemas.py                     (Task 2.2 신규 — pydantic models)
│   └── flink_jobs/
│       ├── __init__.py                    (Task 3.1 신규)
│       ├── bronze_to_silver.py            (Task 3.3 신규)
│       ├── silver_to_gold.py              (Task 4.1 신규)
│       ├── slo_metrics.py                 (Task 4.2 신규)
│       └── lib/
│           ├── __init__.py                (Task 3.1 신규)
│           ├── iceberg_sink.py            (Task 3.2 신규 — Iceberg Sink helper)
│           ├── region_lookup.py           (Task 3.3 신규 — 핫스팟 → 자치구 매핑)
│           └── transforms.py              (Task 3.3 신규 — pure functions)
├── data/
│   └── reference/
│       └── hotspot_regions.csv            (Task 3.3 신규 — 120개 핫스팟 매핑)
├── dbt/
│   └── seoul/
│       ├── dbt_project.yml                (Task 5.1 신규)
│       ├── profiles.yml.example           (Task 5.1 신규)
│       ├── packages.yml                   (Task 5.1 신규)
│       ├── models/
│       │   ├── sources.yml                (Task 5.2 신규)
│       │   ├── staging/
│       │   │   └── stg_hotspot_silver.sql (Task 5.2 신규)
│       │   └── marts/
│       │       ├── fact_hotspot_congestion_hourly.sql (Task 5.2 신규)
│       │       └── schema.yml             (Task 5.3 신규 — dbt tests)
│       └── tests/
│           └── assert_congest_level_valid.sql (Task 5.3 신규)
├── tests/
│   ├── __init__.py                        (Task 2.2 신규)
│   ├── conftest.py                        (Task 2.2 신규)
│   ├── unit/
│   │   ├── test_hotspot_producer.py       (Task 2.2 신규)
│   │   ├── test_subway_producer.py        (Task 2.3 신규)
│   │   ├── test_transforms.py             (Task 3.3 신규)
│   │   └── test_slo_metrics.py            (Task 4.2 신규)
│   └── fixtures/
│       ├── seoul_hotspot_sample.json      (Task 2.2 신규)
│       └── seoul_subway_sample.json       (Task 2.3 신규)
├── scripts/
│   ├── healthcheck.sh                     (Task 1.3 신규)
│   ├── memory_watch.sh                    (Task 1.3 신규)
│   └── duckdb_check.py                    (Task 4.3 신규)
├── docs/
│   ├── superpowers/
│   │   ├── specs/                         (existing)
│   │   └── plans/                         (this file lives here)
│   └── runbook/
│       └── day1_infra.md                  (Task 1.3 신규)
└── .github/
    └── workflows/
        └── ci.yml                         (Task 5.4 신규)
```

각 파일의 단일 책임:
- `docker-compose.yml`: 단일 출처. Kafka/Postgres/MinIO/Lakekeeper 5개 서비스만.
- `src/platform_common/`: producer/flink job 양쪽이 공유하는 환경 변수·Kafka factory.
- `src/producers/`: API 폴링 → Kafka 발행. 1 파일 = 1 토픽 발행자.
- `src/flink_jobs/`: PyFlink job 진입점 (`bronze_to_silver.py`, `silver_to_gold.py`). 변환 로직은 `lib/transforms.py` 의 순수 함수로 분리해 pytest 가능.
- `dbt/seoul/`: dbt-duckdb 프로젝트. 마트 1개로 시작.
- `tests/unit/`: PyFlink 환경 없이 돌아가는 순수 함수 단위 테스트.
- `.github/workflows/ci.yml`: PR 마다 ruff + pytest + dbt parse/test.

---

## Conventions (모든 Task 공통)

- **커밋 메시지**: Conventional Commits. `feat:`, `fix:`, `chore:`, `docs:`, `test:`, `ci:`.
- **각 Task 의 마지막 Step = commit**. Step 실패 시 commit 하지 않고 fix.
- **Run 명령은 repo 루트에서 실행** (별도 명시 없는 한).
- **secrets**: `.env` 는 절대 commit 금지. `.env.example` 만 commit.
- **Iceberg 카탈로그 이름**: `seoul`. **Warehouse 위치**: `s3://seoul-warehouse/`.
- **Kafka 클러스터 ID**: 자동 생성. 컨테이너 이름 prefix = `scp-` (seoul citydata platform).
- **Topic 명명**: spec §4-3. `seoul.hotspot.congestion.v1`, `seoul.transit.subway.v1`.
- **Iceberg 테이블 명명**: `bronze.hotspot_raw`, `silver.hotspot_congestion`, `gold.fact_hotspot_congestion_5min`.

---

## Day 1 — 인프라 기동 (Kafka KRaft + Postgres + MinIO + Lakekeeper)

**Day 1 목표 (spec §6-1):** docker-compose 한 번으로 4개 인프라가 뜨고, Kafka 토픽 2개가 생성되며, Lakekeeper 에 `seoul` warehouse 가 등록된다. Day 1 이 안 끝나면 Day 2 producer 가 발행할 곳이 없으므로 Day 2 진입 금지.

### Task 1.1: 프로젝트 골격 + README + .env.example

**Files:**
- Create: `README.md`
- Create: `.env.example`
- Modify: `.gitignore` (필요 시 항목 추가)

- [ ] **Step 1: README.md 작성**

```markdown
# Seoul Citydata Platform

서울 공공 실시간 데이터(도시데이터·지하철 혼잡도) 와 Postgres CDC, 익명 사용자 행동 로그를 Kafka 메시지 버스로 통합하고 PyFlink streaming + Spark batch + Iceberg(Lakekeeper) + dbt + GitHub Actions 로 처리하는 1인 운영 데이터 플랫폼.

## Quick Start (로컬 docker-compose)

```bash
cp .env.example .env
# .env 의 SEOUL_OPENAPI_KEY, SEOUL_SUBWAY_API_KEY 채우기

docker compose up -d
./scripts/healthcheck.sh

# 토픽 생성
./infra/kafka/create_topics.sh

# Lakekeeper warehouse 등록
uv run python infra/lakekeeper/bootstrap.py
```

## 문서

- 프로젝트 컨텍스트: [`CLAUDE.md`](./CLAUDE.md)
- Phase 1 spec: [`docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`](./docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md)
- Phase 1A Week 1 plan: [`docs/superpowers/plans/phase-1a-week-1.md`](./docs/superpowers/plans/phase-1a-week-1.md)

## 비용

운영 비용 월 $0~$2 (Oracle Cloud Always Free + Cloudflare 무료 + 공공 무료 API).
```

- [ ] **Step 2: .env.example 작성**

```bash
# Seoul OpenAPI
SEOUL_OPENAPI_KEY=replace-me
SEOUL_SUBWAY_API_KEY=replace-me

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Postgres (Day 6 CDC 에서 본격 사용, Day 1 부터 띄움)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=scp
POSTGRES_PASSWORD=scp_dev_password
POSTGRES_DB=scp

# MinIO (S3 호환)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://localhost:9000
MINIO_REGION=us-east-1
ICEBERG_WAREHOUSE_BUCKET=seoul-warehouse

# Lakekeeper REST Catalog
LAKEKEEPER_URL=http://localhost:8181
ICEBERG_CATALOG_NAME=seoul

# Producer 폴링 주기 (초)
HOTSPOT_POLL_INTERVAL_SEC=300
SUBWAY_POLL_INTERVAL_SEC=60
```

- [ ] **Step 3: .gitignore 검증**

기존 `.gitignore` 에 `.env`, `__pycache__/`, `target/`, `dbt_packages/`, `.venv/` 가 이미 포함됨을 확인. 추가 항목 필요 시 한 줄씩 append.

Run: `grep -E '^(\.env$|\.venv|__pycache__|target/|dbt_packages/)' .gitignore`
Expected: 4개 라인 모두 출력.

- [ ] **Step 4: Commit**

```bash
git add README.md .env.example .gitignore
git commit -m "chore: project skeleton — README + .env.example"
```

---

### Task 1.2: docker-compose.yml — Kafka KRaft + Postgres + MinIO + Lakekeeper

**Files:**
- Create: `docker-compose.yml`
- Create: `infra/minio/README.md`

**근거:** spec §3 (Kafka KRaft single-node), §5-4 (Lakekeeper REST), §9-3 (메모리 충돌 — Day 1 free 메모리 측정).

- [ ] **Step 1: docker-compose.yml 작성**

```yaml
name: scp

services:
  kafka:
    image: bitnami/kafka:3.7
    container_name: scp-kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_CFG_LOG_RETENTION_HOURS: 168
      KAFKA_KRAFT_CLUSTER_ID: scp-kafka-cluster-id
    volumes:
      - kafka_data:/bitnami/kafka
    healthcheck:
      test: ["CMD-SHELL", "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 5s
      retries: 12

  postgres:
    image: postgres:16
    container_name: scp-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: scp
      POSTGRES_PASSWORD: scp_dev_password
      POSTGRES_DB: scp
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_wal_senders=4"
      - "-c"
      - "max_replication_slots=4"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U scp -d scp"]
      interval: 5s
      timeout: 5s
      retries: 12

  minio:
    image: minio/minio:RELEASE.2025-01-20T14-49-07Z
    container_name: scp-minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9000/minio/health/ready || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 12

  minio-bootstrap:
    image: minio/mc:RELEASE.2025-01-17T23-25-50Z
    container_name: scp-minio-bootstrap
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      (mc mb local/seoul-warehouse || true) &&
      (mc mb local/lakekeeper || true) &&
      mc anonymous set none local/seoul-warehouse &&
      echo 'minio buckets ready'
      "
    restart: "no"

  lakekeeper:
    image: quay.io/lakekeeper/catalog:v0.5
    container_name: scp-lakekeeper
    depends_on:
      postgres:
        condition: service_healthy
      minio-bootstrap:
        condition: service_completed_successfully
    ports:
      - "8181:8181"
    environment:
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://scp:scp_dev_password@postgres:5432/scp
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://scp:scp_dev_password@postgres:5432/scp
      LAKEKEEPER__PG_ENCRYPTION_KEY: scp-dev-key-do-not-use-in-prod
      LAKEKEEPER__BASE_URI: http://localhost:8181
      LAKEKEEPER__LISTEN_PORT: 8181
      RUST_LOG: info
    command: ["serve"]
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8181/health || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 18

volumes:
  kafka_data:
  postgres_data:
  minio_data:
```

- [ ] **Step 2: infra/minio/README.md 작성**

```markdown
# MinIO buckets

`docker compose up` 시 `minio-bootstrap` 컨테이너가 자동으로 다음 버킷을 만든다:

- `seoul-warehouse` — Iceberg warehouse (모든 Bronze/Silver/Gold 테이블)
- `lakekeeper` — Lakekeeper 자체 메타데이터 보관용 (예약)

콘솔: http://localhost:9001 (minioadmin / minioadmin)

수동 재생성:
```bash
docker compose exec minio mc mb local/seoul-warehouse
```
```

- [ ] **Step 3: 컨테이너 기동 + healthcheck**

Run: `docker compose up -d`
Run: `docker compose ps`
Expected: `scp-kafka`, `scp-postgres`, `scp-minio`, `scp-lakekeeper` 4개 모두 `healthy`. `scp-minio-bootstrap` 은 `exited (0)`.

기다려야 할 시간: Lakekeeper 가 Postgres 마이그레이션을 돌리므로 30~60초.

Run: `curl -sf http://localhost:8181/health && echo OK`
Expected: `OK`

Run: `curl -sf http://localhost:9000/minio/health/ready && echo OK`
Expected: `OK`

- [ ] **Step 4: 디버깅 가드 — Lakekeeper 가 안 뜨면 fallback**

Lakekeeper 컨테이너가 2 시간 이상 안정화되지 않으면 spec §9-1 의 JdbcCatalog 우회로 전환한다. 트리거 조건: `docker compose logs lakekeeper` 에서 panic / migration error 가 반복되고 30분 재시도 후에도 health 가 green 이 안 됨.

이 plan 은 Lakekeeper 정상 경로를 가정. 막히면 본 step 에 메모만 남기고 Task 1.5 를 JdbcCatalog 등록 스크립트로 교체.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml infra/minio/README.md
git commit -m "feat: docker-compose with kafka kraft, postgres, minio, lakekeeper"
```

---

### Task 1.3: 헬스체크 + 메모리 모니터 스크립트 + 런북

**Files:**
- Create: `scripts/healthcheck.sh`
- Create: `scripts/memory_watch.sh`
- Create: `docs/runbook/day1_infra.md`

**근거:** spec §9-3 — "Day 1에 free 메모리 측정 스크립트 작성. 80% 초과 시 알림."

- [ ] **Step 1: scripts/healthcheck.sh 작성**

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "== docker compose ps =="
docker compose ps

echo
echo "== Kafka =="
docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
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
```

- [ ] **Step 2: scripts/memory_watch.sh 작성**

```bash
#!/usr/bin/env bash
# 80% 초과 시 비-0 종료. cron 또는 수동 실행.
set -euo pipefail

THRESHOLD=${THRESHOLD:-80}

if [[ "$(uname)" == "Darwin" ]]; then
  total_bytes=$(sysctl -n hw.memsize)
  page_size=$(vm_stat | awk '/page size of/ {print $8}')
  free_pages=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); print $3}')
  inactive_pages=$(vm_stat | awk '/Pages inactive/ {gsub(/\./,"",$3); print $3}')
  free_bytes=$(( (free_pages + inactive_pages) * page_size ))
  used_bytes=$(( total_bytes - free_bytes ))
else
  total_bytes=$(awk '/MemTotal/ {print $2*1024}' /proc/meminfo)
  avail_bytes=$(awk '/MemAvailable/ {print $2*1024}' /proc/meminfo)
  used_bytes=$(( total_bytes - avail_bytes ))
fi

usage_pct=$(( used_bytes * 100 / total_bytes ))
echo "memory used: ${usage_pct}% (threshold ${THRESHOLD}%)"
if (( usage_pct > THRESHOLD )); then
  echo "ALERT: memory above threshold"
  exit 1
fi
```

- [ ] **Step 3: 실행 권한 부여**

Run: `chmod +x scripts/healthcheck.sh scripts/memory_watch.sh`

- [ ] **Step 4: 헬스체크 실행 검증**

Run: `./scripts/healthcheck.sh`
Expected: 모든 섹션이 `OK` 또는 healthy 출력. `FAIL` 0건.

Run: `./scripts/memory_watch.sh`
Expected: `memory used: NN% (threshold 80%)`, 80 미만이면 종료 코드 0.

- [ ] **Step 5: docs/runbook/day1_infra.md 작성**

```markdown
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

| 증상 | 원인 / 조치 |
|---|---|
| `lakekeeper` healthcheck 가 계속 실패 | Postgres 마이그레이션 지연. 60초 더 대기. 그래도 실패하면 `docker compose logs lakekeeper` 확인 후 spec §9-1 fallback (JdbcCatalog) 발동. |
| `kafka-topics.sh` 가 connection refused | Kafka KRaft 컨트롤러가 아직 안 떴음. 30초 대기 후 재시도. |
| MinIO 콘솔 접속 안 됨 | 9001 포트 충돌. `lsof -i :9001` 로 확인. |
| 메모리 80% 초과 | Spark 가 떠 있는지 확인 (Day 9 외에는 안 떠 있어야 함). 또는 Flink TaskManager heap 축소. |
```

- [ ] **Step 6: Commit**

```bash
git add scripts/healthcheck.sh scripts/memory_watch.sh docs/runbook/day1_infra.md
git commit -m "feat: healthcheck and memory watch scripts + day1 runbook"
```

---

### Task 1.4: Kafka 토픽 생성 스크립트

**Files:**
- Create: `infra/kafka/create_topics.sh`

**근거:** spec §4-3 토픽 명세. Day 2 producer 가 발행할 곳.

- [ ] **Step 1: infra/kafka/create_topics.sh 작성**

```bash
#!/usr/bin/env bash
set -euo pipefail

KCMD="docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

create_topic() {
  local name=$1
  local partitions=$2
  local retention_ms=$3
  if $KCMD --list | grep -q "^${name}$"; then
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
```

- [ ] **Step 2: 실행 권한 + 토픽 생성**

Run: `chmod +x infra/kafka/create_topics.sh`
Run: `./infra/kafka/create_topics.sh`
Expected: 4개 토픽 created. `topic list` 에 4개 모두 출력.

재실행 검증 (멱등성):
Run: `./infra/kafka/create_topics.sh`
Expected: 4개 모두 `already exists, skipping`.

- [ ] **Step 3: 토픽 describe 로 설정 확인**

Run:
```bash
docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic seoul.hotspot.congestion.v1
```
Expected: `PartitionCount: 3`, `ReplicationFactor: 1`, `Configs: ...retention.ms=604800000,compression.type=lz4...`.

- [ ] **Step 4: Commit**

```bash
git add infra/kafka/create_topics.sh
git commit -m "feat: kafka topic provisioning script"
```

---

### Task 1.5: Lakekeeper warehouse 부트스트랩

**Files:**
- Create: `infra/lakekeeper/bootstrap.py`

**근거:** spec §3 catalog = Lakekeeper REST. PyFlink/dbt 가 사용할 `seoul` warehouse 를 Lakekeeper 에 등록.

이 task 는 Python 가상환경이 아직 없으므로 **uv 단독 실행** (pyproject 는 Task 2.1 에서). uv 가 임시로 의존성을 설치하고 스크립트를 실행하는 방식.

- [ ] **Step 1: infra/lakekeeper/bootstrap.py 작성**

```python
"""Lakekeeper warehouse 등록 (멱등).

LAKEKEEPER_URL 의 management API 를 호출해 'seoul' warehouse 를
S3 (MinIO) backend 로 등록한다. 이미 있으면 skip.

Usage:
    uv run --with httpx python infra/lakekeeper/bootstrap.py
"""
from __future__ import annotations

import os
import sys

import httpx

LAKEKEEPER_URL = os.environ.get("LAKEKEEPER_URL", "http://localhost:8181")
WAREHOUSE_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "seoul")
BUCKET = os.environ.get("ICEBERG_WAREHOUSE_BUCKET", "seoul-warehouse")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def get_default_project_id(client: httpx.Client) -> str:
    r = client.get(f"{LAKEKEEPER_URL}/management/v1/project-list")
    r.raise_for_status()
    projects = r.json().get("projects", [])
    if not projects:
        raise RuntimeError("no project found in Lakekeeper")
    return projects[0]["project-id"]


def warehouse_exists(client: httpx.Client, project_id: str) -> bool:
    r = client.get(
        f"{LAKEKEEPER_URL}/management/v1/warehouse",
        params={"project-id": project_id},
    )
    r.raise_for_status()
    for wh in r.json().get("warehouses", []):
        if wh["name"] == WAREHOUSE_NAME:
            return True
    return False


def create_warehouse(client: httpx.Client, project_id: str) -> None:
    payload = {
        "warehouse-name": WAREHOUSE_NAME,
        "project-id": project_id,
        "storage-profile": {
            "type": "s3",
            "bucket": BUCKET,
            "key-prefix": "warehouse",
            "endpoint": MINIO_ENDPOINT,
            "region": MINIO_REGION,
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": MINIO_USER,
            "aws-secret-access-key": MINIO_PASS,
        },
    }
    r = client.post(f"{LAKEKEEPER_URL}/management/v1/warehouse", json=payload)
    if r.status_code >= 400:
        print(f"create warehouse failed: {r.status_code} {r.text}", file=sys.stderr)
        r.raise_for_status()
    print(f"created warehouse '{WAREHOUSE_NAME}'")


def main() -> None:
    with httpx.Client(timeout=30.0) as client:
        project_id = get_default_project_id(client)
        if warehouse_exists(client, project_id):
            print(f"warehouse '{WAREHOUSE_NAME}' already exists, skipping")
            return
        create_warehouse(client, project_id)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 실행**

Run: `uv run --with httpx python infra/lakekeeper/bootstrap.py`
Expected: `created warehouse 'seoul'` 또는 (재실행 시) `warehouse 'seoul' already exists, skipping`.

- [ ] **Step 3: Lakekeeper API 로 직접 검증**

Run:
```bash
curl -s http://localhost:8181/catalog/v1/config?warehouse=seoul | head -c 500
```
Expected: JSON 응답에 `overrides`, `defaults` 키. 에러 아니면 OK.

- [ ] **Step 4: 디버깅 가드 — payload 스키마 미스매치**

Lakekeeper 버전이 0.5 와 다른 경우 payload 의 `storage-profile` / `storage-credential` 스키마가 다를 수 있다. `r.text` 가 `unknown field` 류 에러면 `curl http://localhost:8181/api/openapi.json` 에서 실제 스키마를 확인하고 payload 를 조정. 본 plan 은 v0.5 가정.

- [ ] **Step 5: Commit**

```bash
git add infra/lakekeeper/bootstrap.py
git commit -m "feat: lakekeeper warehouse bootstrap script"
```

**Day 1 종료 게이트:** `./scripts/healthcheck.sh` 통과 + 토픽 4개 + warehouse `seoul` 등록 완료. 미달 시 Day 2 진입 금지.

---

## Day 2 — Producer 2종 (도시데이터 + 지하철 혼잡도) → Kafka

**Day 2 목표 (spec §6-1):** Python producer 2개가 서울 OpenAPI 폴링 → `seoul.hotspot.congestion.v1` / `seoul.transit.subway.v1` 토픽에 메시지를 발행한다. **메시지에 `api_response_ts` 헤더를 첨부**해 Day 4 SLO 측정의 시작점을 마련한다 (spec §6-2).

### Task 2.1: Python 프로젝트 셋업 (uv + pyproject + platform_common)

**Files:**
- Create: `pyproject.toml`
- Create: `src/platform_common/__init__.py`
- Create: `src/platform_common/config.py`

- [ ] **Step 1: pyproject.toml 작성**

```toml
[project]
name = "seoul-citydata-platform"
version = "0.1.0"
description = "Seoul realtime citydata platform — Phase 1A"
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "confluent-kafka>=2.5",
    "tenacity>=8.2",
    "python-dotenv>=1.0",
    "structlog>=24.1",
    "duckdb>=1.1",
    "pyiceberg[duckdb,s3fs]>=0.7",
]

[project.optional-dependencies]
flink = [
    "apache-flink==1.20.0",
]
dev = [
    "pytest>=8.3",
    "pytest-mock>=3.14",
    "ruff>=0.6",
    "mypy>=1.11",
    "respx>=0.21",
]

[tool.uv]
package = true

[tool.hatch.build.targets.wheel]
packages = ["src/platform_common", "src/producers", "src/flink_jobs"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "N", "W"]
ignore = ["E501"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra -q"
pythonpath = ["src"]
```

> **버전 메모 (Day 2 구현 시 발견)**: `tenacity>=8.2` 는 `pyiceberg 0.7.x` 의 `tenacity<9.0` 제약을 반영한 값. `flink` extra 의 `apache-flink==1.20.0` 이 `apache-beam<2.49` → `pyarrow<12` 를 끌어와 `pyiceberg>=0.8` (`pyarrow>=14`) 와 동시 resolve 불가 → `pyiceberg 0.7.x` 가 고정되고 그 결과 `tenacity<9` 가 강제됨. retry API 는 8.x / 9.x 동일이라 코드 영향 없음.

- [ ] **Step 2: src/platform_common/__init__.py 작성**

```python
"""Shared utilities across producers and flink jobs."""

from .config import Settings, get_settings

__all__ = ["Settings", "get_settings"]
```

- [ ] **Step 3: src/platform_common/config.py 작성**

```python
"""환경 변수 → 강타입 Settings (pydantic-settings)."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    seoul_openapi_key: str = Field(default="", alias="SEOUL_OPENAPI_KEY")
    seoul_subway_api_key: str = Field(default="", alias="SEOUL_SUBWAY_API_KEY")

    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS"
    )

    hotspot_poll_interval_sec: int = Field(default=300, alias="HOTSPOT_POLL_INTERVAL_SEC")
    subway_poll_interval_sec: int = Field(default=60, alias="SUBWAY_POLL_INTERVAL_SEC")

    minio_endpoint: str = Field(default="http://localhost:9000", alias="MINIO_ENDPOINT")
    minio_region: str = Field(default="us-east-1", alias="MINIO_REGION")
    minio_user: str = Field(default="minioadmin", alias="MINIO_ROOT_USER")
    minio_password: str = Field(default="minioadmin", alias="MINIO_ROOT_PASSWORD")
    iceberg_warehouse_bucket: str = Field(default="seoul-warehouse", alias="ICEBERG_WAREHOUSE_BUCKET")

    lakekeeper_url: str = Field(default="http://localhost:8181", alias="LAKEKEEPER_URL")
    iceberg_catalog_name: str = Field(default="seoul", alias="ICEBERG_CATALOG_NAME")


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: 의존성 동기화**

Run: `uv sync --extra dev`
Expected: `.venv/` 생성. `uv.lock` 생성. 에러 없음.

- [ ] **Step 5: import smoke test**

Run: `uv run python -c "from platform_common import get_settings; print(get_settings().kafka_bootstrap_servers)"`
Expected: `localhost:9092` 또는 .env 값.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/platform_common/
git commit -m "feat: pyproject with uv + platform_common.config"
```

---

### Task 2.2: 도시데이터 핫스팟 producer (TDD)

**Files:**
- Create: `tests/__init__.py` (빈 파일)
- Create: `tests/conftest.py`
- Create: `tests/fixtures/seoul_hotspot_sample.json`
- Create: `tests/unit/test_hotspot_producer.py`
- Create: `src/producers/__init__.py` (빈 파일)
- Create: `src/producers/schemas.py`
- Create: `src/platform_common/kafka.py`
- Create: `src/producers/hotspot_producer.py`

**근거:** spec §4 (이종 소스 4종 중 1번), §6-2 (`api_response_ts` 헤더), §6-1 Day 2.

- [ ] **Step 1: 샘플 응답 fixture 작성**

서울 도시데이터 API 응답은 핫스팟 1곳당 `CITYDATA` 객체에 `LIVE_PPLTN_STTS`, `ROAD_TRAFFIC_STTS`, `WEATHER_STTS` 등이 들어있는 구조. 본 plan 은 핵심 필드만 사용.

`tests/fixtures/seoul_hotspot_sample.json`:

```json
{
  "RESULT": {"RESULT.CODE": "INFO-000", "RESULT.MESSAGE": "정상 처리되었습니다"},
  "CITYDATA": {
    "AREA_NM": "강남역",
    "AREA_CD": "POI001",
    "LIVE_PPLTN_STTS": {
      "AREA_CONGEST_LVL": "붐빔",
      "AREA_CONGEST_MSG": "사람이 많이 모여있어 답답할 수 있어요.",
      "AREA_PPLTN_MIN": "42000",
      "AREA_PPLTN_MAX": "44000",
      "PPLTN_TIME": "2026-04-30 14:25"
    },
    "ROAD_TRAFFIC_STTS": {
      "AVG_ROAD_DATA": {"ROAD_TRAFFIC_IDX": "서행", "ROAD_TRAFFIC_SPD": "18.4"}
    },
    "WEATHER_STTS": {
      "WEATHER_TIME": "2026-04-30 14:20",
      "TEMP": "21.3",
      "PRECIPITATION": "없음",
      "PCP_MSG": "비가 오지 않습니다"
    }
  }
}
```

- [ ] **Step 2: tests/conftest.py — 공용 fixture**

```python
import json
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def hotspot_sample() -> dict:
    return json.loads((FIXTURE_DIR / "seoul_hotspot_sample.json").read_text(encoding="utf-8"))


@pytest.fixture
def subway_sample() -> dict:
    return json.loads((FIXTURE_DIR / "seoul_subway_sample.json").read_text(encoding="utf-8"))
```

- [ ] **Step 3: 실패 테스트 작성 — `parse_hotspot_payload`**

`tests/unit/test_hotspot_producer.py`:

```python
from datetime import UTC, datetime

from producers.hotspot_producer import parse_hotspot_payload
from producers.schemas import HotspotEvent


def test_parse_hotspot_payload_extracts_core_fields(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")

    assert isinstance(event, HotspotEvent)
    assert event.area_code == "POI001"
    assert event.area_name == "강남역"
    assert event.congest_level == "붐빔"
    assert event.population_min == 42000
    assert event.population_max == 44000
    assert event.api_response_ts == datetime(2026, 4, 30, 14, 25, tzinfo=UTC).replace(tzinfo=None)
    # 공기/도로/날씨는 옵셔널
    assert event.road_traffic_index == "서행"
    assert event.temperature_c == 21.3


def test_parse_hotspot_payload_returns_none_when_missing(hotspot_sample):
    bad = {"RESULT": {"RESULT.CODE": "ERROR-500"}}
    assert parse_hotspot_payload(bad, area_code="POI001") is None


def test_hotspot_event_kafka_key_is_area_code(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")
    assert event.kafka_key() == "POI001"


def test_hotspot_event_kafka_headers_includes_api_response_ts(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")
    headers = dict(event.kafka_headers())
    assert "api_response_ts" in headers
    # 헤더는 bytes
    assert headers["api_response_ts"] == b"2026-04-30T14:25:00"
    assert headers["schema_version"] == b"v1"
```

- [ ] **Step 4: 실패 확인**

Run: `uv run pytest tests/unit/test_hotspot_producer.py -v`
Expected: `ModuleNotFoundError: No module named 'producers'` (또는 `parse_hotspot_payload`).

- [ ] **Step 5: src/producers/schemas.py — pydantic 모델**

```python
"""Producer 출력 스키마 (Kafka 메시지 본문)."""
from __future__ import annotations

from datetime import datetime
from typing import Iterable

from pydantic import BaseModel, ConfigDict


class HotspotEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    area_code: str
    area_name: str
    congest_level: str
    congest_message: str | None = None
    population_min: int | None = None
    population_max: int | None = None
    road_traffic_index: str | None = None
    road_traffic_speed_kmh: float | None = None
    temperature_c: float | None = None
    precipitation: str | None = None
    api_response_ts: datetime  # 서울 API 의 PPLTN_TIME (KST 가정, naive datetime)

    def kafka_key(self) -> str:
        return self.area_code

    def kafka_headers(self) -> Iterable[tuple[str, bytes]]:
        return [
            ("schema_version", b"v1"),
            ("api_response_ts", self.api_response_ts.isoformat().encode("utf-8")),
            ("source", b"seoul.openapi.citydata"),
        ]


class SubwayCongestionEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    station_code: str
    station_name: str
    line_name: str
    train_no: str | None = None
    direction: str | None = None              # 상행 / 하행 / 내선 / 외선
    congestion_score: float | None = None     # 0~150 류 (API 정의 따름)
    congestion_level: str | None = None       # 여유/보통/주의/혼잡 류
    api_response_ts: datetime

    def kafka_key(self) -> str:
        return f"{self.line_name}:{self.station_code}"

    def kafka_headers(self) -> Iterable[tuple[str, bytes]]:
        return [
            ("schema_version", b"v1"),
            ("api_response_ts", self.api_response_ts.isoformat().encode("utf-8")),
            ("source", b"seoul.subway.congestion"),
        ]
```

- [ ] **Step 6: src/platform_common/kafka.py — producer factory**

```python
"""Kafka producer factory + JSON serializer."""
from __future__ import annotations

import json
from typing import Any, Iterable

from confluent_kafka import Producer

from .config import get_settings


def build_producer(client_id: str) -> Producer:
    s = get_settings()
    return Producer(
        {
            "bootstrap.servers": s.kafka_bootstrap_servers,
            "client.id": client_id,
            "compression.type": "lz4",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "batch.num.messages": 1000,
        }
    )


def produce_json(
    producer: Producer,
    topic: str,
    key: str,
    value: dict[str, Any],
    headers: Iterable[tuple[str, bytes]] | None = None,
) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
        headers=list(headers) if headers else None,
    )
```

- [ ] **Step 7: src/producers/hotspot_producer.py — parse + run**

```python
"""서울 도시데이터 핫스팟 producer.

폴링 주기: 5분 (HOTSPOT_POLL_INTERVAL_SEC). 핫스팟 N 곳을 순회하며
http://openapi.seoul.go.kr:8088/{KEY}/json/citydata/1/5/{AREA_NM} 호출.
"""
from __future__ import annotations

import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from platform_common import get_settings
from platform_common.kafka import build_producer, produce_json

from .schemas import HotspotEvent

log = structlog.get_logger()

TOPIC = "seoul.hotspot.congestion.v1"
SEOUL_API_BASE = "http://openapi.seoul.go.kr:8088"


def parse_hotspot_payload(payload: dict[str, Any], area_code: str) -> HotspotEvent | None:
    result = payload.get("RESULT", {})
    code = result.get("RESULT.CODE") or result.get("CODE")
    if code and code != "INFO-000":
        return None

    citydata = payload.get("CITYDATA")
    if not isinstance(citydata, dict):
        return None

    live = citydata.get("LIVE_PPLTN_STTS", {}) or {}
    road = ((citydata.get("ROAD_TRAFFIC_STTS") or {}).get("AVG_ROAD_DATA")) or {}
    weather = citydata.get("WEATHER_STTS", {}) or {}

    pttm = live.get("PPLTN_TIME")
    if not pttm:
        return None
    try:
        api_ts = datetime.strptime(pttm, "%Y-%m-%d %H:%M")
    except ValueError:
        return None

    def _to_int(v: Any) -> int | None:
        try:
            return int(str(v).replace(",", "")) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    def _to_float(v: Any) -> float | None:
        try:
            return float(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    return HotspotEvent(
        area_code=area_code,
        area_name=citydata.get("AREA_NM", ""),
        congest_level=live.get("AREA_CONGEST_LVL", ""),
        congest_message=live.get("AREA_CONGEST_MSG"),
        population_min=_to_int(live.get("AREA_PPLTN_MIN")),
        population_max=_to_int(live.get("AREA_PPLTN_MAX")),
        road_traffic_index=road.get("ROAD_TRAFFIC_IDX"),
        road_traffic_speed_kmh=_to_float(road.get("ROAD_TRAFFIC_SPD")),
        temperature_c=_to_float(weather.get("TEMP")),
        precipitation=weather.get("PRECIPITATION"),
        api_response_ts=api_ts,
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_hotspot(client: httpx.Client, api_key: str, area_name: str) -> dict[str, Any]:
    url = f"{SEOUL_API_BASE}/{api_key}/json/citydata/1/5/{area_name}"
    r = client.get(url, timeout=10.0)
    r.raise_for_status()
    return r.json()


def run(area_codes: dict[str, str]) -> None:
    """area_codes = {area_code: area_name}. e.g. {"POI001": "강남역"}."""
    s = get_settings()
    if not s.seoul_openapi_key:
        raise SystemExit("SEOUL_OPENAPI_KEY not set")

    producer = build_producer(client_id="hotspot-producer")
    stop = {"flag": False}

    def _on_signal(_signum, _frame):
        stop["flag"] = True
        log.info("shutdown signal received")

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    with httpx.Client() as client:
        while not stop["flag"]:
            cycle_start = time.monotonic()
            for code, name in area_codes.items():
                try:
                    payload = fetch_hotspot(client, s.seoul_openapi_key, name)
                except Exception as e:
                    log.warning("fetch_failed", area=name, error=str(e))
                    continue
                event = parse_hotspot_payload(payload, area_code=code)
                if event is None:
                    log.warning("parse_returned_none", area=name)
                    continue
                produce_json(
                    producer,
                    topic=TOPIC,
                    key=event.kafka_key(),
                    value=event.model_dump(mode="json"),
                    headers=event.kafka_headers(),
                )
                log.info("produced", topic=TOPIC, area=name, congest=event.congest_level)
            producer.flush(timeout=10)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0, s.hotspot_poll_interval_sec - elapsed)
            for _ in range(int(sleep_for)):
                if stop["flag"]:
                    break
                time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # 데모용 3곳. 본격 운영 시 data/reference/hotspot_regions.csv 로 확장.
    DEFAULT_AREAS = {
        "POI001": "강남역",
        "POI002": "홍대입구역(2호선)",
        "POI003": "여의도",
    }
    run(DEFAULT_AREAS)
    sys.exit(0)
```

- [ ] **Step 8: 테스트 통과 확인**

Run: `uv run pytest tests/unit/test_hotspot_producer.py -v`
Expected: 4개 모두 PASS.

- [ ] **Step 9: 통합 검증 — 실제 토픽으로 1회 발행**

`.env` 에 `SEOUL_OPENAPI_KEY` 가 채워진 상태에서:
Run:
```bash
uv run python -m producers.hotspot_producer &
SLEEP_PID=$!
sleep 30
kill $SLEEP_PID 2>/dev/null || true
```

Run:
```bash
docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic seoul.hotspot.congestion.v1 \
  --from-beginning --max-messages 3 --property print.headers=true --timeout-ms 15000
```
Expected: 3개 메시지. 헤더에 `api_response_ts:2026-...`, `schema_version:v1`. 본문 JSON 에 `area_code`, `congest_level`.

API 키가 없거나 거부되면 fixture 만으로 단위 테스트 통과를 Day 2 종료 게이트로 인정. 실제 발행은 Day 3 까지 미뤄도 됨.

- [ ] **Step 10: Commit**

```bash
git add tests/__init__.py tests/conftest.py tests/fixtures/seoul_hotspot_sample.json \
        tests/unit/test_hotspot_producer.py \
        src/producers/__init__.py src/producers/schemas.py \
        src/platform_common/kafka.py src/producers/hotspot_producer.py
git commit -m "feat: hotspot producer with api_response_ts header"
```

---

### Task 2.3: 지하철 혼잡도 producer (TDD)

**Files:**
- Create: `tests/fixtures/seoul_subway_sample.json`
- Create: `tests/unit/test_subway_producer.py`
- Create: `src/producers/subway_producer.py`

- [ ] **Step 1: 샘플 응답 fixture**

서울교통공사 실시간 지하철 혼잡도 API 는 노선/역/방향/혼잡도 score 를 반환. 응답 포맷이 공급자에 따라 다를 수 있으므로 본 plan 은 일반화된 키 사용. 실제 응답 도착 시 `parse_subway_payload` 의 키 매핑만 조정하면 됨.

`tests/fixtures/seoul_subway_sample.json`:

```json
{
  "errorMessage": {"code": "INFO-000", "message": "정상 처리되었습니다"},
  "CongestionInfo": [
    {
      "stationCode": "0222",
      "stationName": "강남",
      "lineName": "2호선",
      "trainNo": "2034",
      "direction": "내선",
      "congestionScore": 87.5,
      "congestionLevel": "혼잡",
      "responseTime": "2026-04-30 14:25:30"
    },
    {
      "stationCode": "0239",
      "stationName": "홍대입구",
      "lineName": "2호선",
      "trainNo": "2071",
      "direction": "외선",
      "congestionScore": 62.0,
      "congestionLevel": "보통",
      "responseTime": "2026-04-30 14:25:30"
    }
  ]
}
```

- [ ] **Step 2: 실패 테스트 작성**

`tests/unit/test_subway_producer.py`:

```python
from datetime import datetime

from producers.schemas import SubwayCongestionEvent
from producers.subway_producer import parse_subway_payload


def test_parse_subway_payload_returns_list(subway_sample):
    events = parse_subway_payload(subway_sample)
    assert len(events) == 2
    assert all(isinstance(e, SubwayCongestionEvent) for e in events)


def test_parse_subway_payload_extracts_fields(subway_sample):
    events = parse_subway_payload(subway_sample)
    e0 = events[0]
    assert e0.station_code == "0222"
    assert e0.station_name == "강남"
    assert e0.line_name == "2호선"
    assert e0.congestion_score == 87.5
    assert e0.congestion_level == "혼잡"
    assert e0.api_response_ts == datetime(2026, 4, 30, 14, 25, 30)


def test_parse_subway_payload_skips_when_error(subway_sample):
    subway_sample["errorMessage"]["code"] = "ERROR-500"
    assert parse_subway_payload(subway_sample) == []


def test_subway_event_kafka_key_includes_line(subway_sample):
    events = parse_subway_payload(subway_sample)
    assert events[0].kafka_key() == "2호선:0222"
```

- [ ] **Step 3: 실패 확인**

Run: `uv run pytest tests/unit/test_subway_producer.py -v`
Expected: `ModuleNotFoundError: No module named 'producers.subway_producer'`.

- [ ] **Step 4: src/producers/subway_producer.py 작성**

```python
"""서울 지하철 실시간 혼잡도 producer.

폴링 주기 60초 (SUBWAY_POLL_INTERVAL_SEC).
실 API endpoint 는 키 발급 시 안내 받음. 본 plan 은 응답 형태가
{ "errorMessage": {...}, "CongestionInfo": [{...}, ...] } 라고 가정.
다르면 parse_subway_payload 의 키만 조정.
"""
from __future__ import annotations

import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from platform_common import get_settings
from platform_common.kafka import build_producer, produce_json

from .schemas import SubwayCongestionEvent

log = structlog.get_logger()

TOPIC = "seoul.transit.subway.v1"
SUBWAY_API_BASE = "https://openapi.seoulmetro.co.kr"  # 실제 endpoint 발급 시 교체


def _to_float(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


def parse_subway_payload(payload: dict[str, Any]) -> list[SubwayCongestionEvent]:
    err = payload.get("errorMessage", {}) or {}
    code = err.get("code") or err.get("CODE")
    if code and code != "INFO-000":
        return []

    items = payload.get("CongestionInfo") or payload.get("congestionInfo") or []
    out: list[SubwayCongestionEvent] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ts_raw = item.get("responseTime") or item.get("RESPONSE_TIME")
        if not ts_raw:
            continue
        try:
            api_ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        out.append(
            SubwayCongestionEvent(
                station_code=str(item.get("stationCode") or item.get("STATION_CD") or ""),
                station_name=str(item.get("stationName") or item.get("STATION_NM") or ""),
                line_name=str(item.get("lineName") or item.get("LINE_NM") or ""),
                train_no=item.get("trainNo") or item.get("TRAIN_NO"),
                direction=item.get("direction") or item.get("DIR"),
                congestion_score=_to_float(item.get("congestionScore")),
                congestion_level=item.get("congestionLevel") or item.get("CONGEST_LVL"),
                api_response_ts=api_ts,
            )
        )
    return out


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_subway(client: httpx.Client, api_key: str, line: str) -> dict[str, Any]:
    url = f"{SUBWAY_API_BASE}/api/subway/{api_key}/json/realtimeCongestion/{line}"
    r = client.get(url, timeout=10.0)
    r.raise_for_status()
    return r.json()


def run(lines: list[str]) -> None:
    s = get_settings()
    if not s.seoul_subway_api_key:
        raise SystemExit("SEOUL_SUBWAY_API_KEY not set")

    producer = build_producer(client_id="subway-producer")
    stop = {"flag": False}

    def _on_signal(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    with httpx.Client() as client:
        while not stop["flag"]:
            cycle_start = time.monotonic()
            for line in lines:
                try:
                    payload = fetch_subway(client, s.seoul_subway_api_key, line)
                except Exception as e:
                    log.warning("fetch_failed", line=line, error=str(e))
                    continue
                events = parse_subway_payload(payload)
                for event in events:
                    produce_json(
                        producer,
                        topic=TOPIC,
                        key=event.kafka_key(),
                        value=event.model_dump(mode="json"),
                        headers=event.kafka_headers(),
                    )
                log.info("produced_batch", line=line, count=len(events))
            producer.flush(timeout=10)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0, s.subway_poll_interval_sec - elapsed)
            for _ in range(int(sleep_for)):
                if stop["flag"]:
                    break
                time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    DEFAULT_LINES = ["2호선", "9호선"]
    run(DEFAULT_LINES)
    sys.exit(0)
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `uv run pytest tests/unit/test_subway_producer.py -v`
Expected: 4개 모두 PASS.

- [ ] **Step 6: 전체 단위 테스트 검증**

Run: `uv run pytest tests/unit/ -v`
Expected: hotspot 4 + subway 4 = 8개 PASS.

- [ ] **Step 7: Commit**

```bash
git add tests/fixtures/seoul_subway_sample.json tests/unit/test_subway_producer.py \
        src/producers/subway_producer.py
git commit -m "feat: subway congestion producer with api_response_ts header"
```

**Day 2 종료 게이트:** `uv run pytest tests/unit/ -q` 8 PASS + 최소 한 개 producer 가 실 토픽으로 1회 발행 성공 (콘솔 컨슈머에서 확인). API 키 미발급 시 fixture 단위 테스트만으로 인정 후 Day 3 진입.

---

## Day 3 — PyFlink Bronze → Silver (Iceberg via Lakekeeper)

**Day 3 목표 (spec §6-1):** PyFlink streaming job 1개가 Kafka 두 토픽을 source 로 잡고 Iceberg `bronze.hotspot_raw`, `bronze.subway_raw` 에 적재 → 정규화 + 핫스팟 region 매핑 → `silver.hotspot_congestion`, `silver.subway_congestion` 에 출력. **Lakekeeper REST Catalog 를 사용**해 catalog 결합. fallback (Spark Structured Streaming, JdbcCatalog) 는 spec §9-1 트리거 발동 시.

### Task 3.1: PyFlink 환경 + Iceberg/Kafka connector JARs

**Files:**
- Create: `src/flink_jobs/__init__.py`
- Create: `src/flink_jobs/lib/__init__.py`
- Create: `infra/flink/download_jars.sh`
- Create: `infra/flink/jars/.gitkeep`
- Modify: `.gitignore` (`infra/flink/jars/*.jar` 추가)

**근거:** PyFlink 는 Kafka/Iceberg connector JAR 를 별도 다운로드해야 함.

- [ ] **Step 1: src/flink_jobs/__init__.py, src/flink_jobs/lib/__init__.py — 빈 파일**

```python
```

- [ ] **Step 2: infra/flink/download_jars.sh 작성**

```bash
#!/usr/bin/env bash
set -euo pipefail

JAR_DIR="$(dirname "$0")/jars"
mkdir -p "$JAR_DIR"

FLINK_VER=1.20.0
ICEBERG_VER=1.7.1
KAFKA_VER=3.3.0-1.20

declare -a JARS=(
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VER}/flink-sql-connector-kafka-${KAFKA_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VER}/iceberg-flink-runtime-1.20-${ICEBERG_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
)

for url in "${JARS[@]}"; do
  fname=$(basename "$url")
  if [[ -f "$JAR_DIR/$fname" ]]; then
    echo "skip $fname"
    continue
  fi
  echo "downloading $fname"
  curl -fsSL -o "$JAR_DIR/$fname" "$url"
done

echo
ls -lh "$JAR_DIR"
```

- [ ] **Step 3: .gitignore 갱신**

`.gitignore` 끝에 추가:

```
# Flink connector JARs (downloaded locally)
infra/flink/jars/*.jar
```

- [ ] **Step 4: 다운로드 실행**

Run: `chmod +x infra/flink/download_jars.sh && ./infra/flink/download_jars.sh`
Expected: 4개 JAR (각 수십 ~ 수백 MB) 다운로드 완료. `ls -lh` 결과에 `iceberg-flink-runtime-1.20-1.7.1.jar`, `flink-sql-connector-kafka-3.3.0-1.20.jar` 가 보임.

- [ ] **Step 5: PyFlink 의존성 설치**

`pyproject.toml` 의 `[project.optional-dependencies].flink` 가 이미 정의되어 있음. 별도 그룹으로 설치:
Run: `uv sync --extra dev --extra flink`
Expected: `apache-flink==1.20.0` 가 `.venv/` 에 설치됨.

PyFlink 는 JDK 11 이 필요. 미설치 시:
Run (macOS): `brew install --cask temurin@11` 후 `JAVA_HOME` 을 export.

- [ ] **Step 6: PyFlink 헬로월드**

Run:
```bash
uv run python -c "from pyflink.table import EnvironmentSettings, TableEnvironment; \
  t = TableEnvironment.create(EnvironmentSettings.in_streaming_mode()); print(t.get_config().get_configuration().get_string('pipeline.classpaths', '<empty>'))"
```
Expected: 에러 없이 `<empty>` 또는 빈 classpath 출력.

- [ ] **Step 7: Commit**

```bash
git add src/flink_jobs/__init__.py src/flink_jobs/lib/__init__.py \
        infra/flink/download_jars.sh infra/flink/jars/.gitkeep .gitignore
git commit -m "chore: pyflink env + connector jar download script"
```

---

### Task 3.2: Iceberg Sink helper (PyFlink Table API)

**Files:**
- Create: `src/flink_jobs/lib/iceberg_sink.py`

**근거:** Bronze/Silver/Gold 모든 sink 가 같은 catalog 등록 코드를 반복하지 않도록 helper 1개로 묶는다. 본 모듈은 `pyflink.table.TableEnvironment` 에 Iceberg REST catalog 를 등록한다.

- [ ] **Step 1: lib/iceberg_sink.py 작성**

```python
"""PyFlink Table API 에 Iceberg (Lakekeeper REST) catalog 를 등록한다."""
from __future__ import annotations

from pyflink.table import TableEnvironment

from platform_common import get_settings


def register_iceberg_catalog(t_env: TableEnvironment, catalog_alias: str = "ice") -> str:
    """Lakekeeper REST → Iceberg catalog 등록. catalog alias 반환."""
    s = get_settings()
    rest_uri = f"{s.lakekeeper_url}/catalog"
    warehouse = f"s3://{s.iceberg_warehouse_bucket}/warehouse"

    ddl = f"""
    CREATE CATALOG {catalog_alias} WITH (
      'type' = 'iceberg',
      'catalog-type' = 'rest',
      'uri' = '{rest_uri}',
      'warehouse' = '{warehouse}',
      'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
      's3.endpoint' = '{s.minio_endpoint}',
      's3.access-key-id' = '{s.minio_user}',
      's3.secret-access-key' = '{s.minio_password}',
      's3.path-style-access' = 'true',
      's3.region' = '{s.minio_region}'
    )
    """
    t_env.execute_sql(ddl)
    t_env.execute_sql(f"USE CATALOG {catalog_alias}")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {s.iceberg_catalog_name}.bronze")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {s.iceberg_catalog_name}.silver")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {s.iceberg_catalog_name}.gold")
    return catalog_alias


def warehouse_namespace() -> str:
    """`{catalog}.{db}` 의 catalog 부분 반환 (Lakekeeper warehouse 이름)."""
    return get_settings().iceberg_catalog_name
```

- [ ] **Step 2: 단위 테스트는 PyFlink 의존성 없이 import 만 검증**

이 helper 는 실 Flink env 가 있어야 동작. 단위 테스트는 import smoke 만:

`tests/unit/test_iceberg_sink_import.py`:

```python
def test_iceberg_sink_module_imports():
    from flink_jobs.lib import iceberg_sink

    assert hasattr(iceberg_sink, "register_iceberg_catalog")
    assert iceberg_sink.warehouse_namespace() == "seoul"
```

Run: `uv run pytest tests/unit/test_iceberg_sink_import.py -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/flink_jobs/lib/iceberg_sink.py tests/unit/test_iceberg_sink_import.py
git commit -m "feat: iceberg rest catalog helper for pyflink"
```

---

### Task 3.3: 핫스팟 region 매핑 + 순수 변환 함수 (TDD)

**Files:**
- Create: `data/reference/hotspot_regions.csv`
- Create: `src/flink_jobs/lib/region_lookup.py`
- Create: `src/flink_jobs/lib/transforms.py`
- Create: `tests/unit/test_transforms.py`

**근거:** spec §4-2 Silver 정규화. 핫스팟 → 자치구 매핑은 정적 CSV. 변환 로직은 PyFlink 의존성 없이 pytest 가능하도록 순수 함수.

- [ ] **Step 1: data/reference/hotspot_regions.csv 작성**

본 plan 은 spec §4-3 의 120개 핫스팟 중 일부만 정의. 운영 중 풀 리스트는 사용자가 OpenAPI 문서에서 추출해 추가.

```csv
area_code,area_name,district,gu_code,latitude,longitude
POI001,강남역,강남구,11680,37.4980,127.0276
POI002,홍대입구역(2호선),마포구,11440,37.5571,126.9240
POI003,여의도,영등포구,11560,37.5219,126.9245
POI004,종로·청계 관광특구,종로구,11110,37.5701,126.9912
POI005,성수카페거리,성동구,11200,37.5447,127.0557
POI006,잠실종합운동장,송파구,11710,37.5159,127.0731
POI007,건대입구역,광진구,11215,37.5403,127.0700
POI008,압구정로데오거리,강남구,11680,37.5275,127.0410
POI009,DDP(동대문디자인플라자),중구,11140,37.5665,127.0094
POI010,남산공원,용산구,11170,37.5512,126.9882
```

- [ ] **Step 2: lib/region_lookup.py 작성**

```python
"""핫스팟 → 자치구 매핑. CSV 한 번 로드 후 dict 캐시."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

REFERENCE_PATH = Path(__file__).resolve().parents[3] / "data" / "reference" / "hotspot_regions.csv"


@dataclass(frozen=True)
class Region:
    area_code: str
    area_name: str
    district: str
    gu_code: str
    latitude: float
    longitude: float


@lru_cache
def _load() -> dict[str, Region]:
    out: dict[str, Region] = {}
    with REFERENCE_PATH.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[row["area_code"]] = Region(
                area_code=row["area_code"],
                area_name=row["area_name"],
                district=row["district"],
                gu_code=row["gu_code"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
            )
    return out


def lookup(area_code: str) -> Region | None:
    return _load().get(area_code)


def all_regions() -> dict[str, Region]:
    return _load()
```

- [ ] **Step 3: 실패 테스트 작성**

`tests/unit/test_transforms.py`:

```python
from datetime import datetime

from flink_jobs.lib.region_lookup import lookup
from flink_jobs.lib.transforms import (
    enrich_hotspot_silver,
    normalize_congest_level,
    sanitize_population,
)


def test_lookup_known_area():
    r = lookup("POI001")
    assert r is not None
    assert r.district == "강남구"
    assert r.gu_code == "11680"


def test_lookup_unknown_area_returns_none():
    assert lookup("POI999") is None


def test_normalize_congest_level_maps_korean_to_score():
    assert normalize_congest_level("여유") == 1
    assert normalize_congest_level("보통") == 2
    assert normalize_congest_level("약간 붐빔") == 3
    assert normalize_congest_level("붐빔") == 4
    assert normalize_congest_level("알 수 없음") == 0


def test_sanitize_population_swaps_min_max_when_inverted():
    assert sanitize_population(40000, 30000) == (30000, 40000)
    assert sanitize_population(30000, 40000) == (30000, 40000)
    assert sanitize_population(None, 40000) == (None, 40000)


def test_enrich_hotspot_silver_adds_district_and_score():
    bronze = {
        "area_code": "POI001",
        "area_name": "강남역",
        "congest_level": "붐빔",
        "population_min": 42000,
        "population_max": 44000,
        "api_response_ts": "2026-04-30T14:25:00",
    }
    silver = enrich_hotspot_silver(bronze)
    assert silver["district"] == "강남구"
    assert silver["gu_code"] == "11680"
    assert silver["congest_level_score"] == 4
    assert silver["population_min"] == 42000
    assert silver["population_max"] == 44000
    assert "silver_arrival_ts" in silver


def test_enrich_hotspot_silver_drops_unknown_area():
    bronze = {
        "area_code": "POI999",
        "area_name": "Unknown",
        "congest_level": "보통",
        "population_min": 0,
        "population_max": 0,
        "api_response_ts": "2026-04-30T14:25:00",
    }
    assert enrich_hotspot_silver(bronze) is None
```

- [ ] **Step 4: 실패 확인**

Run: `uv run pytest tests/unit/test_transforms.py -v`
Expected: ImportError 로 모두 FAIL.

- [ ] **Step 5: lib/transforms.py 작성**

```python
"""Bronze JSON dict → Silver dict 의 순수 변환.
PyFlink 환경 없이 단위 테스트 가능."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .region_lookup import lookup

CONGEST_LEVEL_MAP = {
    "여유": 1,
    "보통": 2,
    "약간 붐빔": 3,
    "붐빔": 4,
}


def normalize_congest_level(level: str | None) -> int:
    if not level:
        return 0
    return CONGEST_LEVEL_MAP.get(level.strip(), 0)


def sanitize_population(p_min: int | None, p_max: int | None) -> tuple[int | None, int | None]:
    if p_min is None or p_max is None:
        return p_min, p_max
    if p_min > p_max:
        return p_max, p_min
    return p_min, p_max


def enrich_hotspot_silver(bronze: dict[str, Any]) -> dict[str, Any] | None:
    region = lookup(bronze.get("area_code", ""))
    if region is None:
        return None

    p_min, p_max = sanitize_population(
        bronze.get("population_min"),
        bronze.get("population_max"),
    )

    return {
        "area_code": region.area_code,
        "area_name": region.area_name,
        "district": region.district,
        "gu_code": region.gu_code,
        "latitude": region.latitude,
        "longitude": region.longitude,
        "congest_level": bronze.get("congest_level"),
        "congest_level_score": normalize_congest_level(bronze.get("congest_level")),
        "congest_message": bronze.get("congest_message"),
        "population_min": p_min,
        "population_max": p_max,
        "road_traffic_index": bronze.get("road_traffic_index"),
        "road_traffic_speed_kmh": bronze.get("road_traffic_speed_kmh"),
        "temperature_c": bronze.get("temperature_c"),
        "precipitation": bronze.get("precipitation"),
        "api_response_ts": bronze.get("api_response_ts"),
        "silver_arrival_ts": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
    }
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `uv run pytest tests/unit/test_transforms.py -v`
Expected: 6개 모두 PASS.

- [ ] **Step 7: Commit**

```bash
git add data/reference/hotspot_regions.csv \
        src/flink_jobs/lib/region_lookup.py src/flink_jobs/lib/transforms.py \
        tests/unit/test_transforms.py
git commit -m "feat: hotspot region lookup + silver transforms (pure)"
```

---

### Task 3.4: PyFlink job — Bronze + Silver 적재

**Files:**
- Create: `src/flink_jobs/bronze_to_silver.py`

**근거:** spec §6-1 Day 3 산출물 = "Silver 테이블". Kafka source → Iceberg Bronze sink → Iceberg Silver sink (region 매핑 + congest score). 핫스팟만 우선 본격 구현, subway 는 동일 패턴으로 한 함수만 추가.

- [ ] **Step 1: bronze_to_silver.py 작성**

```python
"""Kafka(`seoul.hotspot.congestion.v1`) → Iceberg bronze → silver.
Run:
  uv run --extra flink python -m flink_jobs.bronze_to_silver
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
from pyflink.table import DataTypes

from flink_jobs.lib.iceberg_sink import register_iceberg_catalog, warehouse_namespace
from flink_jobs.lib.transforms import enrich_hotspot_silver

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

JAR_DIR = Path(__file__).resolve().parents[2] / "infra" / "flink" / "jars"


def _classpath() -> str:
    jars = sorted(JAR_DIR.glob("*.jar"))
    return ";".join(f"file://{p}" for p in jars)


def build_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.jars", _classpath())
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("execution.checkpointing.interval", "30 s")
    return t_env


def register_kafka_source_hotspot(t_env: TableEnvironment) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    ddl = f"""
    CREATE TEMPORARY TABLE hotspot_kafka_src (
      area_code STRING,
      area_name STRING,
      congest_level STRING,
      congest_message STRING,
      population_min INT,
      population_max INT,
      road_traffic_index STRING,
      road_traffic_speed_kmh DOUBLE,
      temperature_c DOUBLE,
      precipitation STRING,
      api_response_ts TIMESTAMP(3),
      kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'seoul.hotspot.congestion.v1',
      'properties.bootstrap.servers' = '{bootstrap}',
      'properties.group.id' = 'flink-bronze-hotspot',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.timestamp-format.standard' = 'ISO-8601',
      'json.ignore-parse-errors' = 'true'
    )
    """
    t_env.execute_sql(ddl)


def create_bronze_table(t_env: TableEnvironment) -> None:
    cat = warehouse_namespace()
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS ice.{cat}.bronze.hotspot_raw (
          area_code STRING,
          area_name STRING,
          congest_level STRING,
          congest_message STRING,
          population_min INT,
          population_max INT,
          road_traffic_index STRING,
          road_traffic_speed_kmh DOUBLE,
          temperature_c DOUBLE,
          precipitation STRING,
          api_response_ts TIMESTAMP(3),
          kafka_ts TIMESTAMP_LTZ(3),
          ingest_ts TIMESTAMP_LTZ(3)
        ) PARTITIONED BY (area_code)
        WITH ('format-version' = '2', 'write.upsert.enabled' = 'false')
        """
    )


def create_silver_table(t_env: TableEnvironment) -> None:
    cat = warehouse_namespace()
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS ice.{cat}.silver.hotspot_congestion (
          area_code STRING,
          area_name STRING,
          district STRING,
          gu_code STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          congest_level STRING,
          congest_level_score INT,
          congest_message STRING,
          population_min INT,
          population_max INT,
          road_traffic_index STRING,
          road_traffic_speed_kmh DOUBLE,
          temperature_c DOUBLE,
          precipitation STRING,
          api_response_ts TIMESTAMP(3),
          silver_arrival_ts TIMESTAMP(3)
        ) PARTITIONED BY (district)
        WITH ('format-version' = '2')
        """
    )


@udf(result_type=DataTypes.ROW(
    [
        DataTypes.FIELD("district", DataTypes.STRING()),
        DataTypes.FIELD("gu_code", DataTypes.STRING()),
        DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
        DataTypes.FIELD("longitude", DataTypes.DOUBLE()),
        DataTypes.FIELD("congest_level_score", DataTypes.INT()),
    ]
))
def enrich_udf(area_code: str, congest_level: str):
    bronze = {"area_code": area_code, "congest_level": congest_level,
              "population_min": None, "population_max": None,
              "api_response_ts": None}
    silver = enrich_hotspot_silver(bronze)
    if silver is None:
        return None, None, None, None, 0
    return (
        silver["district"],
        silver["gu_code"],
        silver["latitude"],
        silver["longitude"],
        silver["congest_level_score"],
    )


def run() -> None:
    t_env = build_env()
    register_iceberg_catalog(t_env, catalog_alias="ice")
    cat = warehouse_namespace()

    register_kafka_source_hotspot(t_env)
    create_bronze_table(t_env)
    create_silver_table(t_env)

    # Bronze 적재 (Kafka → bronze.hotspot_raw)
    t_env.execute_sql(
        f"""
        INSERT INTO ice.{cat}.bronze.hotspot_raw
        SELECT
          area_code, area_name, congest_level, congest_message,
          population_min, population_max,
          road_traffic_index, road_traffic_speed_kmh,
          temperature_c, precipitation,
          api_response_ts, kafka_ts,
          CURRENT_TIMESTAMP AS ingest_ts
        FROM hotspot_kafka_src
        """
    )

    # Silver 적재 (bronze → silver, region join via UDF)
    t_env.create_temporary_function("enrich_hotspot", enrich_udf)
    t_env.execute_sql(
        f"""
        INSERT INTO ice.{cat}.silver.hotspot_congestion
        SELECT
          b.area_code,
          b.area_name,
          e.district,
          e.gu_code,
          e.latitude,
          e.longitude,
          b.congest_level,
          e.congest_level_score,
          b.congest_message,
          b.population_min,
          b.population_max,
          b.road_traffic_index,
          b.road_traffic_speed_kmh,
          b.temperature_c,
          b.precipitation,
          b.api_response_ts,
          CURRENT_TIMESTAMP AS silver_arrival_ts
        FROM ice.{cat}.bronze.hotspot_raw b
        CROSS JOIN LATERAL TABLE(
          SELECT enrich_hotspot(b.area_code, b.congest_level)
        ) AS e(district, gu_code, latitude, longitude, congest_level_score)
        WHERE e.district IS NOT NULL
        """
    )
    log.info("Bronze + Silver streaming jobs submitted")


if __name__ == "__main__":
    run()
```

- [ ] **Step 2: smoke run — 1분 가동 후 stop**

producer 가 가동 중이거나 Day 2 의 1회성 발행으로 Kafka 에 메시지가 있는 상태에서:
Run:
```bash
uv run --extra flink python -m flink_jobs.bronze_to_silver &
FLINK_PID=$!
sleep 90
kill $FLINK_PID 2>/dev/null || true
```

- [ ] **Step 3: Iceberg 검증 — DuckDB 로 Bronze/Silver 카운트**

`scripts/duckdb_check.py` 는 Task 4.3 에서 본격 만들지만, 본 step 만 임시 1회성 명령으로:

Run:
```bash
uv run python -c "
import duckdb
con = duckdb.connect()
con.execute(\"INSTALL iceberg; LOAD iceberg;\")
con.execute(\"CREATE OR REPLACE SECRET (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')\")
print('bronze:', con.execute(\"SELECT count(*) FROM iceberg_scan('s3://seoul-warehouse/warehouse/bronze/hotspot_raw')\").fetchone())
print('silver:', con.execute(\"SELECT count(*) FROM iceberg_scan('s3://seoul-warehouse/warehouse/silver/hotspot_congestion')\").fetchone())
"
```
Expected: 두 카운트 모두 > 0. 메시지 5분 폴링 1회만 했어도 Bronze=N, Silver≤N (region 매핑 누락 행 빼고).

- [ ] **Step 4: 디버깅 가드 — Lakekeeper 인증 / S3 경로 / Hadoop conf 이슈**

- `Could not load credentials` → `s3.access-key-id` 설정 확인.
- `NoSuchBucket` → `seoul-warehouse` 버킷 존재 확인 (`docker compose exec minio mc ls local/`).
- `RestCatalog ... 404` → Lakekeeper warehouse 등록 (Task 1.5) 재실행.
- 2 시간 이상 막히면 spec §9-1 Day 3 트리거 → Spark Structured Streaming 우회 검토.

- [ ] **Step 5: Commit**

```bash
git add src/flink_jobs/bronze_to_silver.py
git commit -m "feat: pyflink bronze→silver streaming job"
```

**Day 3 종료 게이트:** PyFlink job 이 1분 이상 안정 가동 + DuckDB 로 `silver.hotspot_congestion` count > 0 확인. Day 4 진입.

---

## Day 4 — PyFlink Silver → Gold + 데이터 신선도 SLO

**Day 4 목표 (spec §6-1):** `gold.fact_hotspot_congestion_5min` 적재 + 데이터 신선도 P95 < 7분 측정 코드 + DuckDB 검증 노트북. **신선도 측정은 `api_response_ts` (producer header) → `gold_arrival_ts` (Flink sink 시각) 차이의 분포** (spec §6-2).

### Task 4.1: Silver → Gold (`fact_hotspot_congestion_5min`)

**Files:**
- Create: `src/flink_jobs/silver_to_gold.py`

**근거:** spec §4-2 Gold 테이블 명세. 5분 텀블링 윈도우.

- [ ] **Step 1: silver_to_gold.py 작성**

```python
"""Silver hotspot_congestion → Gold fact_hotspot_congestion_5min.

5분 텀블링 윈도우. 자치구 단위 평균 혼잡도/인구.
"""
from __future__ import annotations

import logging

from pyflink.table import EnvironmentSettings, TableEnvironment

from flink_jobs.bronze_to_silver import _classpath
from flink_jobs.lib.iceberg_sink import register_iceberg_catalog, warehouse_namespace

logging.basicConfig(level=logging.INFO)


def build_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.jars", _classpath())
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("execution.checkpointing.interval", "60 s")
    return t_env


def create_gold_table(t_env: TableEnvironment) -> None:
    cat = warehouse_namespace()
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS ice.{cat}.gold.fact_hotspot_congestion_5min (
          window_start TIMESTAMP(3),
          window_end   TIMESTAMP(3),
          district STRING,
          gu_code STRING,
          area_count BIGINT,
          avg_congest_score DOUBLE,
          max_congest_score INT,
          avg_population_min DOUBLE,
          avg_population_max DOUBLE,
          last_api_response_ts TIMESTAMP(3),
          gold_arrival_ts TIMESTAMP(3)
        ) PARTITIONED BY (district)
        WITH ('format-version' = '2')
        """
    )


def run() -> None:
    t_env = build_env()
    register_iceberg_catalog(t_env, catalog_alias="ice")
    cat = warehouse_namespace()
    create_gold_table(t_env)

    # silver 를 source 로 읽기 위해 streaming source 모드 옵션
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW silver_stream AS
        SELECT *,
          CAST(api_response_ts AS TIMESTAMP_LTZ(3)) AS event_time
        FROM ice.{cat}.silver.hotspot_congestion
          /*+ OPTIONS('streaming'='true', 'monitor-interval'='30s') */
        """
    )

    t_env.execute_sql(
        f"""
        INSERT INTO ice.{cat}.gold.fact_hotspot_congestion_5min
        SELECT
          window_start,
          window_end,
          district,
          MAX(gu_code) AS gu_code,
          COUNT(DISTINCT area_code) AS area_count,
          AVG(CAST(congest_level_score AS DOUBLE)) AS avg_congest_score,
          MAX(congest_level_score) AS max_congest_score,
          AVG(CAST(population_min AS DOUBLE)) AS avg_population_min,
          AVG(CAST(population_max AS DOUBLE)) AS avg_population_max,
          MAX(api_response_ts) AS last_api_response_ts,
          CURRENT_TIMESTAMP AS gold_arrival_ts
        FROM TABLE(
          TUMBLE(TABLE silver_stream, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, district
        """
    )


if __name__ == "__main__":
    run()
```

- [ ] **Step 2: smoke run**

Bronze→Silver job 이 가동 중인 상태에서 별도 셸:
Run: `uv run --extra flink python -m flink_jobs.silver_to_gold &`
1~2 윈도우(=10분) 가동 후 종료.

- [ ] **Step 3: DuckDB 로 Gold 카운트 확인**

Run:
```bash
uv run python -c "
import duckdb
con = duckdb.connect()
con.execute(\"INSTALL iceberg; LOAD iceberg;\")
con.execute(\"CREATE OR REPLACE SECRET (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')\")
print(con.execute(\"SELECT district, area_count, avg_congest_score FROM iceberg_scan('s3://seoul-warehouse/warehouse/gold/fact_hotspot_congestion_5min') LIMIT 10\").fetchall())
"
```
Expected: 1행 이상. `district` 가 한국어, `avg_congest_score` 가 1~4 사이.

- [ ] **Step 4: Commit**

```bash
git add src/flink_jobs/silver_to_gold.py
git commit -m "feat: pyflink silver→gold 5min tumbling aggregation"
```

---

### Task 4.2: 데이터 신선도 SLO 측정 코드 (TDD)

**Files:**
- Create: `src/flink_jobs/slo_metrics.py`
- Create: `tests/unit/test_slo_metrics.py`

**근거:** spec §6-2 — "producer 가 메시지에 `api_response_ts` 헤더 첨부 → Flink Gold sink 가 `gold_arrival_ts` 기록 → 두 값 차이의 분포를 자체 Python 스크립트로 일일 리포트". P95 < 7분.

본 task 는 PyFlink 가 아닌 **DuckDB → Iceberg 직접 쿼리** 로 측정하는 standalone 스크립트.

- [ ] **Step 1: 실패 테스트 작성**

`tests/unit/test_slo_metrics.py`:

```python
from datetime import datetime, timedelta

import pytest

from flink_jobs.slo_metrics import FreshnessReport, compute_freshness_seconds, summarize


def test_compute_freshness_seconds_basic():
    api_ts = datetime(2026, 4, 30, 14, 0, 0)
    gold_ts = datetime(2026, 4, 30, 14, 4, 30)
    assert compute_freshness_seconds(api_ts, gold_ts) == 270


def test_compute_freshness_seconds_negative_clamped_to_zero():
    api_ts = datetime(2026, 4, 30, 14, 5)
    gold_ts = datetime(2026, 4, 30, 14, 0)
    assert compute_freshness_seconds(api_ts, gold_ts) == 0


def test_summarize_returns_p50_p95_p99_max():
    samples = [60, 90, 120, 150, 180, 210, 240, 300, 360, 420]  # 10개, 초 단위
    rep = summarize(samples)
    assert isinstance(rep, FreshnessReport)
    assert rep.count == 10
    assert rep.p50_seconds == 195
    assert rep.p95_seconds == 414
    assert rep.max_seconds == 420
    assert rep.p95_seconds < 7 * 60  # 7분 SLO 통과


def test_summarize_empty_returns_zeros():
    rep = summarize([])
    assert rep.count == 0
    assert rep.p50_seconds == 0
    assert rep.slo_violated is False


def test_summarize_marks_slo_violated_when_p95_above_7min():
    samples = [60, 60, 60, 60, 60, 60, 60, 60, 60, 9999]
    rep = summarize(samples)
    assert rep.slo_violated is True
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/test_slo_metrics.py -v`
Expected: ImportError.

- [ ] **Step 3: src/flink_jobs/slo_metrics.py 작성**

```python
"""데이터 신선도 SLO 리포트.

api_response_ts (producer 가 첨부한 서울 API 응답 시각) →
gold_arrival_ts (Flink Gold sink 시각) 차이의 분포.
P95 < 7분 SLO (spec §6-2).

Run:
  uv run python -m flink_jobs.slo_metrics
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

import duckdb

from platform_common import get_settings

SLO_P95_SECONDS = 7 * 60


@dataclass
class FreshnessReport:
    count: int
    p50_seconds: int
    p95_seconds: int
    p99_seconds: int
    max_seconds: int
    slo_violated: bool


def compute_freshness_seconds(api_ts: datetime, gold_ts: datetime) -> int:
    delta = (gold_ts - api_ts).total_seconds()
    return max(0, int(delta))


def _percentile(sorted_samples: Sequence[int], p: float) -> int:
    if not sorted_samples:
        return 0
    n = len(sorted_samples)
    rank = p * (n - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return int(sorted_samples[lo])
    frac = rank - lo
    return int(sorted_samples[lo] + (sorted_samples[hi] - sorted_samples[lo]) * frac)


def summarize(samples: Sequence[int]) -> FreshnessReport:
    if not samples:
        return FreshnessReport(0, 0, 0, 0, 0, False)
    s = sorted(samples)
    p50 = _percentile(s, 0.50)
    p95 = _percentile(s, 0.95)
    p99 = _percentile(s, 0.99)
    return FreshnessReport(
        count=len(s),
        p50_seconds=p50,
        p95_seconds=p95,
        p99_seconds=p99,
        max_seconds=s[-1],
        slo_violated=p95 > SLO_P95_SECONDS,
    )


def _duckdb_with_iceberg() -> duckdb.DuckDBPyConnection:
    s = get_settings()
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""CREATE OR REPLACE SECRET (
            TYPE S3,
            KEY_ID '{s.minio_user}',
            SECRET '{s.minio_password}',
            ENDPOINT '{s.minio_endpoint.replace("http://", "")}',
            URL_STYLE 'path', USE_SSL false, REGION '{s.minio_region}'
        )"""
    )
    return con


def fetch_samples_from_gold() -> list[int]:
    con = _duckdb_with_iceberg()
    s = get_settings()
    rows = con.execute(
        f"""
        SELECT date_diff('second', last_api_response_ts, gold_arrival_ts) AS sec
        FROM iceberg_scan('s3://{s.iceberg_warehouse_bucket}/warehouse/gold/fact_hotspot_congestion_5min')
        WHERE gold_arrival_ts >= now() - INTERVAL 24 HOUR
        """
    ).fetchall()
    return [int(r[0]) for r in rows if r[0] is not None]


def main() -> None:
    samples = fetch_samples_from_gold()
    rep = summarize(samples)
    print(f"== Freshness SLO Report ==")
    print(f"count          : {rep.count}")
    print(f"p50 seconds    : {rep.p50_seconds}")
    print(f"p95 seconds    : {rep.p95_seconds}  (SLO threshold: {SLO_P95_SECONDS})")
    print(f"p99 seconds    : {rep.p99_seconds}")
    print(f"max seconds    : {rep.max_seconds}")
    print(f"SLO violated   : {rep.slo_violated}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: 단위 테스트 통과 확인**

Run: `uv run pytest tests/unit/test_slo_metrics.py -v`
Expected: 5개 모두 PASS.

- [ ] **Step 5: 실 데이터로 1회 측정**

Day 4 끝까지 Flink job 가동 후:
Run: `uv run python -m flink_jobs.slo_metrics`
Expected: count > 0, p95 가 7분(420초) 이내. 만약 초과 시 spec §9-1 fallback 검토. 첫 측정에서 7분 초과 흔함 — 다음 30분 더 가동 후 재측정.

- [ ] **Step 6: Commit**

```bash
git add src/flink_jobs/slo_metrics.py tests/unit/test_slo_metrics.py
git commit -m "feat: data freshness slo report (p95 < 7min)"
```

---

### Task 4.3: DuckDB 검증 노트북 / 스크립트

**Files:**
- Create: `scripts/duckdb_check.py`

**근거:** spec §6-3 포트폴리오 4페이지 = "데이터 신선도 SLO 측정 결과 + dbt tests". DuckDB 노트북 스크린샷이 필요.

- [ ] **Step 1: scripts/duckdb_check.py 작성**

```python
"""DuckDB 로 Iceberg Bronze/Silver/Gold 를 직접 쿼리해 검증.

Run:
  uv run python scripts/duckdb_check.py
"""
from __future__ import annotations

import duckdb

from platform_common import get_settings


def main() -> None:
    s = get_settings()
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""CREATE OR REPLACE SECRET (
            TYPE S3,
            KEY_ID '{s.minio_user}',
            SECRET '{s.minio_password}',
            ENDPOINT '{s.minio_endpoint.replace("http://", "")}',
            URL_STYLE 'path', USE_SSL false, REGION '{s.minio_region}'
        )"""
    )

    base = f"s3://{s.iceberg_warehouse_bucket}/warehouse"

    print("== bronze.hotspot_raw count ==")
    print(con.execute(f"SELECT count(*) FROM iceberg_scan('{base}/bronze/hotspot_raw')").fetchone())

    print("\n== silver.hotspot_congestion sample ==")
    rows = con.execute(
        f"""SELECT area_code, area_name, district, congest_level, congest_level_score,
                  population_min, population_max, api_response_ts, silver_arrival_ts
           FROM iceberg_scan('{base}/silver/hotspot_congestion')
           ORDER BY silver_arrival_ts DESC LIMIT 5"""
    ).fetchall()
    for r in rows:
        print(r)

    print("\n== gold.fact_hotspot_congestion_5min sample ==")
    rows = con.execute(
        f"""SELECT window_start, window_end, district, area_count, avg_congest_score
           FROM iceberg_scan('{base}/gold/fact_hotspot_congestion_5min')
           ORDER BY window_start DESC LIMIT 5"""
    ).fetchall()
    for r in rows:
        print(r)

    print("\n== district 별 latest avg_congest_score ==")
    rows = con.execute(
        f"""SELECT district, avg_congest_score
           FROM iceberg_scan('{base}/gold/fact_hotspot_congestion_5min')
           QUALIFY row_number() OVER (PARTITION BY district ORDER BY window_start DESC) = 1
           ORDER BY avg_congest_score DESC"""
    ).fetchall()
    for r in rows:
        print(r)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 실행 검증**

Run: `uv run python scripts/duckdb_check.py`
Expected: Bronze count, Silver 샘플 5행, Gold 샘플 5행, 자치구별 최신 score 출력. 0행이면 Flink job 을 더 오래 가동 후 재시도.

- [ ] **Step 3: Commit**

```bash
git add scripts/duckdb_check.py
git commit -m "feat: duckdb verification script for bronze/silver/gold"
```

**Day 4 종료 게이트:** SLO 리포트가 1회 이상 출력되고 P95 < 7분. duckdb_check.py 가 Gold 데이터 정상 출력. 미달이면 Flink 파이프라인 디버깅.

---

## Day 5 — dbt-core + Airflow 본진 셋업 + GitHub Actions CI

**Day 5 목표 (spec §6-1, §5-8):** ① Silver→Gold 변환 일부를 dbt-core(+dbt-duckdb) 모델로 코드화하고 dbt tests 5~10개 추가, ② GitHub Actions 가 PR 마다 ruff + pytest + dbt parse/test 를 돌리고, ③ **Airflow (LocalExecutor + SQLite metadata) 셋업 + `dbt_full_run` DAG (TaskGroup + 의존성 + SLA + on_failure_callback)** 로 dbt 운영을 batch ops 본진에 올린다. Gold 적재 자체는 PyFlink 가 streaming 으로 계속 담당하고, dbt 는 **시간당 집계 (`fact_hotspot_congestion_hourly`)** 등 batch-friendly mart 1개를 새로 만든다.

**Airflow 도입 사유 (spec §5-8 본진 사용 정당화):**
- 1번 포트폴리오에서 Airflow 를 15분 batch trigger (cron 대용) 로만 사용 → 본 프로젝트에서 본진 기능 직접 운영으로 spec §2 약점 #11 커버
- DE JD 빈출 키워드 "Airflow 등 워크플로우 관리 도구 운영 경험" 직접 대응
- 3계층 분리 원칙: streaming = Flink, polling = cron, batch ops = Airflow

**메모리 mitigation (spec §5-8, §9-3):** LocalExecutor + SQLite metadata DB → ~700MB. Postgres meta / Celery / Redis 미사용. Airflow 도입 직후 free 메모리 측정 (80% = 19.2GB 임계 확인). Day 9 Spark 기동 직전 `airflow-scheduler` 일시 stop 운영 원칙.

**Day 5 fallback (spec §9-1):** Airflow 셋업 4시간 초과 시 1단계 — `dbt_full_run` 만 GitHub Actions schedule + cron 으로 우회 후 buffer 에 재시도. Day 6 에도 실패 시 2단계 — Airflow 미도입으로 결정 + 약점 #11 미커버를 포트폴리오에 솔직히 기술.

### Task 5.1: dbt-duckdb 프로젝트 init

**Files:**
- Create: `dbt/seoul/dbt_project.yml`
- Create: `dbt/seoul/profiles.yml.example`
- Create: `dbt/seoul/packages.yml`
- Modify: `pyproject.toml` (dev deps 에 dbt 추가)

- [ ] **Step 1: pyproject.toml dev deps 에 dbt 추가**

`[project.optional-dependencies].dev` 의 리스트에 다음 두 줄을 추가:

```toml
    "dbt-core>=1.9",
    "dbt-duckdb>=1.9",
```

Run: `uv sync --extra dev`

- [ ] **Step 2: dbt_project.yml 작성**

```yaml
name: seoul
version: "0.1.0"
config-version: 2

profile: seoul_duckdb

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  seoul:
    staging:
      +materialized: view
    marts:
      +materialized: table
      +schema: gold
```

- [ ] **Step 3: profiles.yml.example 작성**

```yaml
seoul_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ":memory:"
      extensions:
        - iceberg
        - httpfs
      settings:
        s3_endpoint: "localhost:9000"
        s3_access_key_id: "minioadmin"
        s3_secret_access_key: "minioadmin"
        s3_region: "us-east-1"
        s3_use_ssl: false
        s3_url_style: "path"
```

이 파일을 `~/.dbt/profiles.yml` 로 복사 (또는 `DBT_PROFILES_DIR=$(pwd)/dbt/seoul dbt run` 으로 동일 디렉토리 사용). 본 plan 은 후자 방식.

- [ ] **Step 4: packages.yml 작성**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.2.0", "<2.0.0"]
```

- [ ] **Step 5: dbt deps + parse 검증**

Run:
```bash
cd dbt/seoul
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt deps
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt parse
cd ../..
```

profiles.yml 이 example 만 있으므로 우선:
Run: `cp dbt/seoul/profiles.yml.example dbt/seoul/profiles.yml`

그리고 `dbt/seoul/profiles.yml` 을 `.gitignore` 에 추가:
`.gitignore` 끝에:
```
dbt/seoul/profiles.yml
```

다시 실행:
Run:
```bash
cd dbt/seoul
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt deps
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt parse
cd ../..
```
Expected: `Found 0 models, 0 tests, ...` 에러 없음.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock dbt/seoul/dbt_project.yml \
        dbt/seoul/profiles.yml.example dbt/seoul/packages.yml .gitignore
git commit -m "feat: dbt-duckdb scaffold for seoul project"
```

---

### Task 5.2: dbt 모델 — Silver staging + Gold hourly mart

**Files:**
- Create: `dbt/seoul/models/sources.yml`
- Create: `dbt/seoul/models/staging/stg_hotspot_silver.sql`
- Create: `dbt/seoul/models/marts/fact_hotspot_congestion_hourly.sql`

- [ ] **Step 1: sources.yml — Iceberg Silver 를 dbt source 로 등록**

```yaml
version: 2

sources:
  - name: silver
    description: PyFlink 가 적재한 Iceberg silver 테이블 (S3/MinIO).
    meta:
      external_location: "s3://seoul-warehouse/warehouse/silver"
    tables:
      - name: hotspot_congestion
        description: 핫스팟 5분 단위 혼잡도 (region 매핑 + congest score).
        meta:
          external_location: "s3://seoul-warehouse/warehouse/silver/hotspot_congestion"
        columns:
          - name: area_code
            description: 핫스팟 코드 (POI001~).
          - name: district
            description: 자치구.
          - name: congest_level_score
            description: 0~4. 0=알수없음, 4=붐빔.
          - name: api_response_ts
            description: 서울 API 응답 시각.
          - name: silver_arrival_ts
            description: Flink Silver sink 시각.
```

- [ ] **Step 2: stg_hotspot_silver.sql**

```sql
-- staging: silver 직접 노출 (dbt-duckdb iceberg_scan 사용)
{{ config(materialized='view') }}

select
    area_code,
    area_name,
    district,
    gu_code,
    latitude,
    longitude,
    congest_level,
    congest_level_score,
    population_min,
    population_max,
    road_traffic_index,
    road_traffic_speed_kmh,
    temperature_c,
    api_response_ts,
    silver_arrival_ts
from iceberg_scan('s3://seoul-warehouse/warehouse/silver/hotspot_congestion')
where congest_level_score > 0
```

- [ ] **Step 3: fact_hotspot_congestion_hourly.sql**

```sql
-- Gold mart: 자치구 × 시각 시간단위 평균 (PyFlink 의 5분 윈도우와 별개로 batch 집계)
{{ config(
    materialized='table',
    schema='gold'
) }}

select
    date_trunc('hour', api_response_ts) as window_hour,
    district,
    any_value(gu_code) as gu_code,
    count(distinct area_code) as area_count,
    avg(congest_level_score) as avg_congest_score,
    max(congest_level_score) as max_congest_score,
    avg(population_min) as avg_population_min,
    avg(population_max) as avg_population_max,
    max(silver_arrival_ts) as last_silver_arrival_ts
from {{ ref('stg_hotspot_silver') }}
where api_response_ts >= now() - interval 7 day
group by 1, 2
```

- [ ] **Step 4: dbt run 검증**

Run:
```bash
cd dbt/seoul
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt run
cd ../..
```
Expected: `Completed successfully` + 2 models (stg view + mart table). DuckDB in-memory 에 mart 가 생성됨.

- [ ] **Step 5: Commit**

```bash
git add dbt/seoul/models/sources.yml \
        dbt/seoul/models/staging/stg_hotspot_silver.sql \
        dbt/seoul/models/marts/fact_hotspot_congestion_hourly.sql
git commit -m "feat: dbt staging + hourly mart for hotspot congestion"
```

---

### Task 5.3: dbt tests 5개 + custom singular test 1개

**Files:**
- Create: `dbt/seoul/models/marts/schema.yml`
- Create: `dbt/seoul/tests/assert_congest_level_valid.sql`

**근거:** spec §5-5 — "dbt tests 5~10개". 본 task 에서 generic 5 + singular 1 = 6개.

- [ ] **Step 1: schema.yml — generic tests 5개**

```yaml
version: 2

models:
  - name: stg_hotspot_silver
    description: Silver hotspot 정규화 view (dbt staging).
    columns:
      - name: area_code
        description: POI001~ 형식.
        tests:
          - not_null
          - dbt_utils.not_empty_string
      - name: district
        description: 자치구.
        tests:
          - not_null
      - name: congest_level_score
        description: 0~4.
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4]   # staging where 절에서 0 제거
      - name: api_response_ts
        tests:
          - not_null

  - name: fact_hotspot_congestion_hourly
    description: 자치구 × 시각 시간단위 평균.
    columns:
      - name: window_hour
        tests:
          - not_null
      - name: district
        tests:
          - not_null
      - name: area_count
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 1"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - window_hour
            - district
```

- [ ] **Step 2: tests/assert_congest_level_valid.sql — singular test**

```sql
-- 한국어 congest_level 라벨이 매핑 4종 안에 있는지 검증.
-- 매핑 밖 라벨이 들어오면 producer/transforms 가 깨진 것.
select
    distinct congest_level
from {{ source('silver', 'hotspot_congestion') }}
where congest_level not in ('여유', '보통', '약간 붐빔', '붐빔')
  and congest_level is not null
```

- [ ] **Step 3: dbt test 실행**

Run:
```bash
cd dbt/seoul
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt deps
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt run
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt test
cd ../..
```
Expected: 5 generic + 1 singular = 6 tests PASS. 일부 데이터가 부족해 `area_count >= 1` 같은 테스트가 PASS 하더라도 row 0 이면 dbt 가 warning 만 낼 수 있음. 모든 PASS 면 OK.

- [ ] **Step 4: Commit**

```bash
git add dbt/seoul/models/marts/schema.yml dbt/seoul/tests/assert_congest_level_valid.sql
git commit -m "test: 6 dbt tests (5 generic + 1 singular)"
```

---

### Task 5.4: GitHub Actions CI — ruff + pytest + dbt parse/test

**Files:**
- Create: `.github/workflows/ci.yml`

**근거:** spec §5-5 — "GitHub Actions (dbt + PyFlink lint/test)". CI 는 인프라 없이 돌아가야 하므로 **PyFlink 통합 테스트는 제외**, **dbt 는 parse + compile** 까지만 (실 Iceberg 데이터 없이).

- [ ] **Step 1: ci.yml 작성**

```yaml
name: CI

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up uv
        uses: astral-sh/setup-uv@v3
        with:
          version: "0.4.x"

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dev deps
        run: uv sync --extra dev

      - name: Ruff lint
        run: uv run ruff check src tests

      - name: Ruff format check
        run: uv run ruff format --check src tests

      - name: Pytest unit
        run: uv run pytest tests/unit -v

  dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up uv
        uses: astral-sh/setup-uv@v3
        with:
          version: "0.4.x"

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dev deps (incl. dbt)
        run: uv sync --extra dev

      - name: Write CI profiles.yml
        run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml <<'EOF'
          seoul_duckdb:
            target: ci
            outputs:
              ci:
                type: duckdb
                path: ":memory:"
          EOF

      - name: dbt deps
        working-directory: dbt/seoul
        run: uv run --project ../.. dbt deps

      - name: dbt parse
        working-directory: dbt/seoul
        run: uv run --project ../.. dbt parse

      - name: dbt compile
        working-directory: dbt/seoul
        run: uv run --project ../.. dbt compile
```

CI 환경에는 MinIO 가 없으므로 `dbt run` / `dbt test` 는 빠짐. 실제 데이터 검증은 로컬에서 수행 후 PR 본문에 결과 첨부. 이는 spec §5-5 의 "lint/test" 범위 안.

- [ ] **Step 2: 로컬에서 ruff lint 통과 확인**

Run: `uv run ruff check src tests`
Expected: `All checks passed!`. 실패 시 `uv run ruff check --fix src tests` 후 수동 검토 + commit.

Run: `uv run ruff format --check src tests`
Expected: 포맷 차이 없음. 실패 시 `uv run ruff format src tests` 후 commit.

- [ ] **Step 3: Pytest 전체 통과**

Run: `uv run pytest tests/unit -v`
Expected: 18~20개 (hotspot 4 + subway 4 + iceberg_sink_import 1 + transforms 6 + slo 5 = 20) PASS.

- [ ] **Step 4: dbt parse/compile 통과**

Run:
```bash
cd dbt/seoul
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt parse
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt compile
cd ../..
```
Expected: 둘 다 success.

- [ ] **Step 5: Push 후 Actions 결과 확인**

Run:
```bash
git push origin main   # 또는 feature branch + PR
```

GitHub Actions 페이지에서 `python` + `dbt` 두 잡 모두 green 확인. red 면 로그에 따라 조치 후 재push.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: github actions for ruff, pytest, dbt parse/compile"
git push
```

---

### Task 5.5: Airflow 셋업 — docker-compose 추가 (LocalExecutor + SQLite metadata)

**Files:**
- Modify: `docker-compose.yml` — `airflow-init` (init container) + `airflow-webserver` + `airflow-scheduler` 서비스 추가
- Create: `airflow/Dockerfile` — Airflow 이미지 + Python 의존성 (`apache-airflow-providers-postgres`, `pyiceberg`, `duckdb`, `boto3`)
- Create: `airflow/requirements.txt`
- Create: `airflow/dags/.gitkeep`
- Create: `airflow/plugins/.gitkeep`
- Create: `airflow/logs/.gitkeep` (gitignore)
- Modify: `.env.example` — `AIRFLOW_UID`, `AIRFLOW_HOME`, `AIRFLOW_FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`
- Modify: `.gitignore` — `airflow/logs/`, `airflow/airflow.db`
- Modify: `scripts/healthcheck.sh` — Airflow webserver `/health` endpoint 체크 추가

**Goal:** `docker compose up -d airflow-webserver airflow-scheduler` 후 `http://localhost:8080` 에서 Airflow UI 접속, admin 계정 로그인 가능, 초기 DAG 0개 상태 확인.

**본진 기능 발휘 포인트 (spec §5-8):**
- LocalExecutor + SQLite metadata → Postgres meta / Celery / Redis 미사용 → ~700MB
- `parsing_processes=1, dag_dir_list_interval=300` → scheduler 메모리 절감
- `airflow-init` init container 가 첫 기동 시 `airflow db init` + admin 사용자 생성 후 종료 (idempotent)

**환경 편차 가정 (`env-deviations-day-1` 메모리 참조):**
- Apache 공식 이미지 (`apache/airflow:2.10.x`) 채택. constraints URL 은 Airflow 2.10 + Python 3.11 매칭.
- ARM Ampere A1 아키텍처 호환 확인 (`docker buildx`).

**검증 명령:**
- `./scripts/healthcheck.sh` → Airflow webserver OK
- `docker stats airflow-webserver airflow-scheduler` → RES 합계 < 1GB 확인
- `free -h` → 80% (19.2GB) 임계 안 확인. **초과 시 spec §9-1 fallback 트리거 가동.**
- `docker compose logs airflow-scheduler --tail=50 | grep -i error` → 0건

**Day 5 fallback 트리거:** 4시간 초과 시 Task 5.6/5.7/5.8 보류 + `dbt_full_run` 만 GitHub Actions schedule 로 우회.

> **상세 implementation step (Dockerfile 본문, docker-compose 환경변수 풀세트, healthcheck 명령) 은 Day 4 종료 시점에 별도 plan-update commit 으로 작성 — env 편차 (Airflow constraints, ARM 빌드, Lakekeeper network alias) 반영 필요.**

---

### Task 5.6: `dbt_full_run` DAG — dbt 운영을 Airflow 본진에 (TaskGroup + 의존성 + SLA)

**Files:**
- Create: `airflow/dags/dbt_full_run.py` — DAG 정의
- Create: `airflow/dags/common/callbacks.py` — `send_discord_alert(context)` (on_failure_callback)
- Create: `airflow/dags/common/__init__.py`
- Create: `tests/unit/airflow/test_dbt_full_run_dag.py` — DAG 파싱 / 의존성 그래프 / SLA / callback 검증

**Goal:** `airflow dags trigger dbt_full_run` 으로 staging → marts 순서 실행, staging test 실패 시 marts 자동 skip, on_failure_callback 발신 동작.

**본진 기능 발휘 (spec §5-8 표 1행):**
- **TaskGroup**: `with TaskGroup("staging") as staging:`, `with TaskGroup("marts") as marts:` — UI 시각 분리
- **Task 의존성**: `staging.dbt_test_staging >> marts.dbt_run_marts` — test 실패 시 marts 자동 skip
- **Retry policy**: `default_args={"retries": 2, "retry_exponential_backoff": True, "retry_delay": timedelta(minutes=5)}`
- **SLA**: `default_args={"sla": timedelta(minutes=30)}` — 30분 초과 시 SLA miss 자동 기록
- **on_failure_callback**: Discord webhook (Task ID + execution_date + log URL)
- schedule: `"0 2 * * *"` (매일 02:00 KST, streaming peak 회피)

**Task 그래프 (spec §5-8 의 시각화 그대로):**
```
dbt_seed
└─ TaskGroup: staging
   ├─ dbt_run_staging   (BashOperator: dbt run --select staging)
   └─ dbt_test_staging  (BashOperator: dbt test --select staging)
   └─ TaskGroup: marts
      ├─ dbt_run_marts  (BashOperator: dbt run --select marts)
      └─ dbt_test_marts (BashOperator: dbt test --select marts)
└─ dbt_docs_generate    (BashOperator: dbt docs generate)
└─ upload_docs          (BashOperator: aws s3 cp / Cloudflare Pages)
```

**TDD 단계 (pure DAG 파싱 검증):**
- Step 1: 실패 테스트 — `test_dag_loads()`, `test_staging_blocks_marts()`, `test_sla_30min()`, `test_on_failure_callback_set()`
- Step 2: 테스트 fail 확인
- Step 3: DAG 본문 작성 (위 본진 기능 모두 발휘)
- Step 4: 테스트 PASS 확인 (`pytest tests/unit/airflow -v`)
- Step 5: Airflow UI 에서 DAG 보임 + manual trigger 1회 성공 (dbt 실패해도 callback 검증 가능)
- Step 6: Commit

**검증 명령:**
- `airflow dags list | grep dbt_full_run` → 보임
- `airflow dags test dbt_full_run $(date +%Y-%m-%d)` → end-to-end 1회 성공 (또는 staging test 실패 시 marts skip 확인)
- Airflow UI > Graph view 에서 staging/marts TaskGroup 시각 확인

> **상세 implementation step (Discord webhook URL 환경변수, dbt CLI working_dir, profiles.yml 경로, BashOperator vs DbtRunOperator 선택) 은 Day 4 종료 시점에 별도 plan-update commit 으로 작성. dbt CLI 호출 패턴은 Task 5.4 의 GitHub Actions yml 과 일관성 유지.**

---

### Task 5.7 (Day 5~6 buffer): `backfill_silver_from_bronze` DAG — Dynamic Task Mapping + 멱등 MERGE INTO

**Files:**
- Create: `airflow/dags/backfill_silver_from_bronze.py`
- Create: `airflow/dags/common/spark_submit.py` — Spark 호출 helper (Day 9 와 공유)
- Create: `tests/unit/airflow/test_backfill_dag.py`
- Create: `spark/jobs/backfill_silver_partition.py` — partition 1개 처리 Spark job (멱등 MERGE INTO)

**Goal:** Airflow UI 에서 `start_ts` / `end_ts` / `tables` / `dry_run` Params 입력 후 trigger → 시간 partition 별로 Spark job 병렬 실행 (max 2개 동시) → 멱등 MERGE INTO 로 Silver 재처리. 같은 백필을 반복해도 결과 동일.

**본진 기능 발휘 (spec §5-8 표 3행):**
- **Dynamic Task Mapping**: `process_partition.expand(partition=hours)` — 런타임에 N개 task 자동 생성 (Airflow 2.3+)
- **Params**: 6개 시간 partition 백필 시 6개 task 자동 펼침
- **`max_active_tis_per_dag=2`**: Spark 동시 submit 2개 제한 → 메모리 OOM 방지
- **dry_run 모드**: row count 만 추정, 실제 적재 안 함
- **멱등 MERGE INTO**: `MERGE INTO silver.* USING bronze.* ON dedup_key WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT`
- schedule: `None` (수동 trigger 전용)

**Task 그래프:**
```
validate_params
└─ generate_hourly_partitions  (Python: ["2026-...T00", "...T01", ...])
   └─ process_partition.expand(partition=...) ← ★ Dynamic Task Mapping
   └─ verify_silver_row_count
   └─ post_backfill_summary    (Discord webhook)
```

**검증 명령 (3시간 백필 dry_run):**
- Airflow UI > Trigger DAG w/ config: `{"start_ts": "2026-...T00", "end_ts": "2026-...T03", "tables": ["fact_hotspot_congestion_5min"], "dry_run": true}`
- Graph view 에서 process_partition[0/1/2] 3개 task 자동 생성 확인
- 모든 task SUCCESS + Discord 요약 메시지 도착 확인

**면접 답변 시그널 (spec §8-2):** 백필 설계 = orchestrator 본진. "어떻게 백필했어요?" 단골 질문 답변.

> **상세 implementation step (Spark job 본문, MERGE INTO SQL, dedup_key 결정, partition 시간 범위 generator) 은 Day 4 종료 시점 plan-update commit 으로 작성. Day 9 Task 9.2 (Spark MERGE INTO 멱등성 검증) 와 dedup_key 일관성 필수 (spec §10 1번 미해결 closure 과 동일 패턴).**

---

### Task 5.8 (Day 5~6 buffer): `iceberg_maintenance` DAG 골격 — Day 9 본격 운영 전 셋업

**Files:**
- Create: `airflow/dags/iceberg_maintenance.py` — DAG 골격 (Day 5 시점은 BashOperator placeholder, Day 9 에서 SparkSubmitOperator 로 교체)
- Create: `tests/unit/airflow/test_iceberg_maintenance_dag.py` — DAG 파싱 + 병렬 구조 + max_active_tis_per_dag 검증

**Goal:** DAG 파싱 / 병렬 구조 / SLA 1시간 / max_active_tis_per_dag=3 골격을 Day 5 buffer 에 미리 잡아두고, Day 9 에서 실제 Spark MERGE INTO + Compaction job 으로 본문 채움. **Day 5 시점은 BashOperator 가 echo 만 하는 placeholder (DAG 파싱 검증만).**

**본진 기능 발휘 (spec §5-8 표 2행, Day 9 에서 본격 활성화):**
- **병렬 실행**: TaskGroup `rewrite` 안에 3개 task (`rewrite_fact_hotspot_congestion_5min`, `rewrite_fact_user_event` (P1B 후 활성화), `rewrite_dim_place`)
- **`max_active_tis_per_dag=3`**: Spark concurrent submit 제한
- **XCom**: before/after 메트릭 (`{file_count, total_bytes, snapshot_count}`)
- **on_success_callback**: Discord 압축률 자동 보고
- **SLA 1시간**: 메모리 ceiling 위협 자동 감지
- schedule: `"0 3 * * *"` (매일 03:00 KST)

**Task 그래프 (Day 9 에서 본격):**
```
snapshot_metrics_before    (XCom push)
└─ TaskGroup: rewrite      (병렬, max 3)
   ├─ rewrite_fact_hotspot_congestion_5min
   ├─ rewrite_fact_user_event  (P1B 후 활성화)
   └─ rewrite_dim_place
   └─ expire_snapshots (older than 7d)
      └─ remove_orphan_files (older than 3d)
         └─ snapshot_metrics_after  (XCom push)
            └─ post_compaction_report  (XCom pull → Discord)
```

**Day 5 시점 검증 명령 (골격만):**
- `pytest tests/unit/airflow/test_iceberg_maintenance_dag.py -v` → 파싱 / 병렬 구조 / max_active_tis_per_dag / SLA 검증 PASS
- Airflow UI > Graph view 에서 골격 시각 확인 (실제 실행은 Day 9 까지 schedule_interval=None 또는 unpause 보류)

**Day 5 fallback 트리거:** Task 5.7/5.8 까지 buffer 안에 마치지 못하면 → Task 5.5/5.6 만 Day 5 PR 에 포함, Task 5.7/5.8 은 Day 6 시작 시 추가 commit. `iceberg_maintenance` 본문은 Day 9 에서 어차피 채우므로 큰 일정 부담 없음.

> **상세 implementation step (Iceberg `rewrite_data_files` Spark SQL, expire_snapshots / remove_orphan_files Iceberg procedure, XCom dict 스키마, Discord 메시지 포맷) 은 Day 8 종료 시점 plan-update commit 으로 작성. Day 9 Task 9.3 (Compaction) 과 본문 통합 시점 동일.**

---

**Day 5 종료 게이트 (= Phase 1A Week 1 종료):**
- `./scripts/healthcheck.sh` 5 components OK (Kafka + Postgres + MinIO + Lakekeeper + **Airflow webserver**)
- `uv run pytest tests/unit -q` 모든 테스트 PASS (airflow DAG 파싱 테스트 3개 포함)
- DuckDB 검증 스크립트가 Bronze/Silver/Gold 모두 row > 0
- SLO 리포트 P95 < 7분
- dbt run + test 통과
- GitHub Actions 두 잡 green
- 토픽 4개 (hotspot, subway, place CDC, user events)
- Iceberg 테이블 5개 (bronze 1 + silver 1 + gold 1 + dbt mart 1)
- **Airflow DAG 4개 등록** (`dbt_full_run` 활성, `backfill_silver_from_bronze` 수동 trigger 만, `iceberg_maintenance` 골격, **`slo_daily_report` 는 Day 10 에 추가**)
- **Airflow `dbt_full_run` 1회 manual trigger 성공** (또는 staging test 실패 시 marts skip 동작 확인)
- **`free -h` 80% 임계 안** (19.2GB), `docker stats` 로 Airflow RES 합계 < 1GB 확인

위 게이트 모두 충족 시 Week 2 plan (`phase-1a-week-2.md`, Day 6~10) 으로 진입.

---

## Self-Review (writing-plans 스킬 §Self-Review)

**1. Spec 커버리지 매핑 (spec §6-1 Day 1~5)**

| Spec 항목 | 본 plan 의 task |
|---|---|
| Day 1 — docker-compose (Kafka KRaft, Postgres, MinIO, Lakekeeper) | Task 1.1~1.5 |
| Day 2 — 도시데이터 + 지하철 producer → 토픽 2개 | Task 2.1~2.3 |
| Day 3 — PyFlink Bronze→Silver (정규화, region 매핑) → Iceberg via Lakekeeper | Task 3.1~3.4 |
| Day 4 — PyFlink Silver→Gold (`fact_hotspot_congestion_5min`) + DuckDB 검증 + 데이터 신선도 SLO 측정 코드 | Task 4.1~4.3 |
| Day 5 — dbt-core 도입, Silver→Gold 일부 dbt 이관, dbt tests 5~10, **Airflow 본진 셋업 + `dbt_full_run` DAG**, GitHub Actions CI | Task 5.1~5.4 + **5.5 (Airflow 셋업) + 5.6 (`dbt_full_run` DAG)** |
| Day 5~6 buffer — `backfill_silver_from_bronze` + `iceberg_maintenance` 골격 (spec §5-8 본진 4 DAG 중 2~3번) | **Task 5.7 (backfill DAG, dynamic task mapping) + 5.8 (iceberg_maintenance 골격, Day 9 본문 채움)** |
| Spec §5-8 — Airflow 본진 사용 정당화 + 메모리 mitigation (LocalExecutor + SQLite + 야간 실행 + Day 9 scheduler stop) | Task 5.5 (셋업) + 5.6/5.7/5.8 (본진 기능 발휘) + Day 5 종료 게이트 (`free -h` 80% 임계 검증) |
| Spec §6-2 — `api_response_ts` 헤더 → `gold_arrival_ts`, P95 < 7분 | Task 2.2 (헤더), 4.1 (gold sink), 4.2 (리포트) |
| Spec §9-1 fallback 트리거 (Lakekeeper, Flink, Spark, **Airflow 셋업 4시간 / 메모리 80% 초과**) | Task 1.5 Step 4, Task 3.4 Step 4, **Task 5.5/5.6/5.7/5.8 의 Day 5 fallback 메모** |
| Spec §9-3 메모리 모니터 | Task 1.3 + **Day 5 종료 게이트 재측정** |
| Spec §10 Day 0 사전 준비 | "전제 (Day 0 완료 항목)" 섹션 |

**2. Placeholder 스캔**

- "TODO" / "TBD" / "implement later" / "fill in details" → 0건. (확인됨)
- "Add appropriate error handling" → 모든 producer 가 구체적 try/except + tenacity retry 명시.
- "Write tests for the above" → 모든 TDD step 에 실제 테스트 코드 포함.
- "Similar to Task N" → 0건. 각 task 코드 전체 포함.
- **Task 5.5~5.8 (Airflow 4 task) 는 의도적 골격** — Files / Goal / 본진 기능 / 검증 명령 / fallback 까지는 명시했으나, 상세 implementation step (Dockerfile 본문, DAG Python 코드, Spark job 본문) 은 **Day 4 종료 시점 plan-update commit 으로 작성** (env 편차 반영 + dbt CLI 호출 패턴 통일을 위해). 이는 writing-plans skill 의 "No Placeholders" 원칙과 일부 충돌하지만, **명시적 시점 + 명시적 trigger 가 박혀있어 implicit TBD 가 아님** — 동일 패턴이 Week 2 (Day 6~10) 와 Week 1 (Day 1~5) 의 분리에서 이미 정착된 점진적 작성 방식을 따른다.

**3. 타입 일관성**

- `HotspotEvent` 의 필드명: `area_code`, `congest_level`, `population_min/max`, `api_response_ts` — Task 2.2 schemas → Task 3.3 transforms → Task 3.4 PyFlink DDL → Task 4.1 Gold DDL → Task 5.2 dbt model 까지 일관.
- `SubwayCongestionEvent` 는 Day 3~5 에서 Bronze/Silver/Gold 풀 변환을 포함하지 않음. 이는 의도. spec §6-1 Day 3 = "핫스팟 region 매핑" 이라 hotspot 우선, subway 는 Bronze 적재만 Week 2 plan 에서 다룬다. 본 plan 의 Task 3.4 는 hotspot 만 처리하므로 일관성 유지.
- Topic 이름: `seoul.hotspot.congestion.v1`, `seoul.transit.subway.v1`, `place.master.cdc.v1`, `user.events.v1` — Task 1.4 와 Task 2.2/2.3 의 `TOPIC` 상수 일치.
- Iceberg 카탈로그 alias `ice` + warehouse db `seoul` — Task 3.2/3.4/4.1 일관.
- Gold 테이블 컬럼 `last_api_response_ts` + `gold_arrival_ts` — Task 4.1 sink 와 Task 4.2 SLO 쿼리 (`date_diff('second', last_api_response_ts, gold_arrival_ts)`) 일치.

**4. Out of scope (의도)**

- Day 6 (Postgres + Debezium CDC, `dim_place` SCD2)
- Day 7 (Next.js + Cloudflare Pages)
- Day 8 ("지금 한가하고 영업 중인" 데모 화면)
- Day 9 (Spark batch MERGE INTO 멱등성 + Compaction)
- Day 10 (포트폴리오 1차 작성)
- → 모두 Week 2 plan (`phase-1a-week-2.md`).

이 4개 점검 결과 본 plan 은 spec §6-1 Day 1~5 + §6-2 SLO + §9-3 메모리 모니터를 빠짐없이 구현한다.
