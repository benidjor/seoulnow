# Seoul Citydata Platform

서울 공공 실시간 데이터(도시데이터·지하철 혼잡도) 와 Postgres CDC, 익명 사용자 행동 로그를 Kafka 메시지 버스로 통합하고 PyFlink streaming + Spark batch + Iceberg(Lakekeeper) + dbt + GitHub Actions 로 처리하는 1인 운영 데이터 플랫폼.

## 프로젝트 목적과 이유

### 왜 이 프로젝트를 기획했는가

서울에서 활동할 때 — **데이트 / 친구 모임 / 카페에서 공부** 등 — 지역별 혼잡도와 영업 시간 정보가 동시에 필요한데, 한 곳에서 실시간으로 받을 수 있는 서비스를 찾기 어려웠다. 본인이 실제로 사람들을 만날 때 같은 어려움을 겪었던 점이 본 프로젝트의 직접 동기.

서울시 공공 실시간 데이터 (도시데이터 + 지하철 도착정보) + 가게 영업 시간 정보를 결합하면, 사람들의 선택에 도움을 줄 수 있는 정보를 충분히 만들 수 있다는 가설을 직접 검증.

### 사용자에게 제공하고자 한 것 (3 단계)

| 단계 | 기능 | 데이터 출처 |
|---|---|---|
| 1차 — Phase 1A 메인 지도 | **관심 지역별 실시간 혼잡도** | 서울 도시데이터 API (5분 갱신) + 지하철 도착정보 API (30 ~ 60초 갱신) |
| 2차 — Phase 1A `chill_open_now` mart | **특정 시간 기준** 해당 지역에서 **영업하는 카페 / 식당 / 술집** | 공공 인허가 (영업 시간) + 1차의 실시간 혼잡도 |
| 3차 — Phase 2 확장 | **지역 + 시간대 혼잡도 기반 카페 / 식당 / 술집 추천** | + 외부 가게 정보 (영업 시간 + 별점) |

### 무엇을 풀고 싶었는가 (운영 / 기술 측면)

| 기술 / 운영 동기 | 본 프로젝트의 답 |
|---|---|
| 실시간 streaming 의 동작을 직접 검증 (선행 프로젝트의 micro-batch 한계 인지 후) | PyFlink → Iceberg streaming 의 exactly-once + 5min tumbling window 직접 구현 |
| 단일 노드 운영 (월 $0 ~ $0.83) 의 trade-off 경험 | Oracle Cloud Free VM + Kafka KRaft single-node + LocalExecutor Airflow + Lakekeeper REST |
| 본진 워크플로우 도구 직접 운영 | Airflow 본진 4 DAG (cron 대용 사용에서 본진 운영으로 도구 사용 깊이의 변화) |
| 데이터 품질 자동화 + CI/CD | dbt tests 6+ + GitHub Actions ruff/pytest/dbt 매 PR |

### 운영 목표

- **월 운영 비용 $0 ~ $0.83** — Oracle Cloud Free + Cloudflare 무료 + 공공 무료 API + Iceberg Compaction 의 S3 PutObject 만
- **데이터 신선도 SLO 자동 측정** — `slo_daily_report` DAG 매일 09:00 KST 자동 리포트 (두 가지 SLO: Data Freshness P95 < 45분, Platform Latency P95 < 7분)
- **재현 가능한 환경** — docker-compose 한 명령으로 전체 인프라 기동

## 핵심 결과 (Phase 1A, Day 10 시점)

| 항목 | 결과 |
|---|---|
| 공개 도메인 | https://seoul-citydata.pages.dev |
| Data Freshness SLO | **P95 < 45분** (서울 OpenAPI source lag 31분+ 포함) |
| Platform Latency SLO | **P95 < 7분** (silver→gold 우리 통제 구간) |
| 운영 비용 | **월 $0 ~ $0.83** |
| 이종 데이터 소스 | 3종 (도시데이터 + 지하철 + Postgres CDC) |
| Kafka 토픽 | 3개 (P1A) → 4개 (P1B 예정) |
| Iceberg 테이블 | 7개 (Bronze 3 + Silver 3 + Gold 4 mart) |
| dbt 모델 / 테스트 | 3 모델 / 6+ 테스트 |
| Airflow 본진 4 DAG | `dbt_full_run` / `iceberg_maintenance` / `backfill_silver_from_bronze` / `slo_daily_report` |
| GitHub Actions | ruff + pytest (25+) + dbt parse/compile, PR 마다 자동 |

**3계층 분리 원칙**: streaming = PyFlink / polling = cron / batch ops = Airflow.

## Phase 1A 산출물

- 종합 리포트 v1 (7 page, Airflow 본진 4 DAG + 트러블슈팅 포함): [`docs/portfolio/phase1a_v1.md`](./docs/portfolio/phase1a_v1.md)
- 시스템 아키텍처 다이어그램: [`docs/architecture/data-flow.md`](./docs/architecture/data-flow.md)
- 데이터 lineage + 두 가지 SLO 측정 경로: [`docs/architecture/data_lineage.md`](./docs/architecture/data_lineage.md)
- Day 9 archive (14건 트러블슈팅 학습 자산): [`docs/portfolio/troubleshooting/2026-05-12-day-9-archive.md`](./docs/portfolio/troubleshooting/2026-05-12-day-9-archive.md)
- Day 8 archive: [`docs/portfolio/troubleshooting/2026-05-11-day-8-archive.md`](./docs/portfolio/troubleshooting/2026-05-11-day-8-archive.md)

## Quick Start (로컬 docker-compose)

```bash
# 0) 사전 준비 (한 번)
# host /etc/hosts 에 docker hostname alias 추가 (sudo 1회).
# Lakekeeper REST 의 vend URI 가 docker hostname 기준이라 host 측 client 도
# 같은 hostname 으로 resolve 되어야 함. host port mapping (8181:8181, 9000:9000)
# 덕분에 alias 1줄로 host / container 양쪽 통과.
sudo sh -c 'echo "127.0.0.1 lakekeeper minio" >> /etc/hosts'
cp .env.example .env
# .env 의 SEOUL_OPENAPI_KEY, SEOUL_SUBWAY_API_KEY 채우기

# 1) 인프라
docker compose up -d
./scripts/healthcheck.sh                       # 4개 service healthy 확인
./infra/kafka/create_topics.sh                 # Kafka 토픽 4개 생성
uv run --with httpx python infra/lakekeeper/bootstrap.py    # warehouse 등록

# 2) Postgres seed + Debezium
docker compose exec -T postgres psql -U scp -d scp < infra/postgres/seed_places.sql
./infra/debezium/register.sh                   # CDC connector 등록

# 3) Producer (별도 셸)
uv sync --extra dev --extra flink
uv run python -m producers.hotspot_producer    # 5분 polling
uv run python -m producers.subway_producer     # 60초 polling

# 4) Flink streaming jobs (별도 셸)
uv run --extra flink python -m flink_jobs.bronze_to_silver
uv run --extra flink python -m flink_jobs.silver_to_gold
uv run --extra flink python -m flink_jobs.cdc_to_dim_place

# 5) FastAPI
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000

# 6) 정적 1회 적재 + dbt
uv run python scripts/load_static_places.py
cd dbt/seoul && DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt run && dbt test && cd ../..

# 7) Day 9 Spark batch (일시 기동) — airflow-scheduler 일시 stop 후 진행
docker compose stop airflow-scheduler   # 700MB 회수 (24GB 한계 + Day 9 OOM 방지)
docker compose --profile spark up -d spark
docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/jobs/merge_dim_place.py
docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/jobs/compaction_silver.py
docker compose --profile spark down
docker compose start airflow-scheduler  # 작업 종료 후 재기동
# 또는 Airflow iceberg_maintenance DAG manual trigger 로 위 두 spark-submit 자동화
# (Day 9 종료 시점부터 매일 03:00 KST 자동 실행)

# 8) Airflow 본진 4 DAG (Day 5 시작, Day 10 라인업 완성)
docker compose up -d airflow-webserver airflow-scheduler   # http://localhost:8080
```

## Airflow 본진 4 DAG

| DAG | 도입 | schedule | 본진 기능 |
|---|---|---|---|
| `dbt_full_run` | Day 5 | manual / PR 검토 | TaskGroup + 의존성 + SLA 30분 + `on_failure_callback` |
| `iceberg_maintenance` | Day 5 골격 → Day 9 본격 | `0 3 * * *` | 병렬 Spark + XCom + `on_success_callback` + `max_active_tis_per_dag=3` |
| `backfill_silver_from_bronze` | Day 5-6 buffer | manual | Dynamic Task Mapping + Params + 멱등 MERGE INTO |
| `slo_daily_report` | Day 10 PR α | `0 9 * * *` | BranchPythonOperator + XCom + 두 가지 SLO 분기 alert |

상세 = [`docs/portfolio/phase1a_v1.md` p6 참조](./docs/portfolio/phase1a_v1.md).

## SLO 측정

- **Data Freshness** = `gold_arrival_ts - api_response_ts(tm)` P95 < 45분 (서울 OpenAPI source lag 31분+ 포함)
- **Platform Latency** = `gold_arrival_ts - silver_arrival_ts` P95 < 7분 (Path B, silver→gold 우리 통제 구간)

측정 도구:

- `uv run --extra flink python -m flink_jobs.slo_metrics` — 24h 윈도우 분포 (`SLOReport` dataclass)
- Airflow `slo_daily_report` DAG — 매일 09:00 KST 자동 실행 + 위반 시 Discord webhook
- 측정 경로 시각화: [`docs/architecture/data_lineage.md`](./docs/architecture/data_lineage.md) §2

## 디렉토리 구조

```
seoul-citydata-platform/
├── airflow/dags/              # Airflow 본진 4 DAG + common helpers
├── docs/
│   ├── architecture/          # data-flow.md (mermaid) + data_lineage.md
│   ├── portfolio/             # 종합 리포트 v1 + Day archives
│   ├── runbook/               # Day 별 운영 매뉴얼
│   └── superpowers/           # spec + plan SoT
├── dbt/seoul/                 # dbt-core (staging + marts)
├── infra/                     # docker-compose + Kafka + Lakekeeper + Postgres + Debezium
├── scripts/                   # healthcheck / load_static_places / migrate / cost_report
├── src/
│   ├── api/                   # FastAPI (DuckDB → /api/*)
│   ├── flink_jobs/            # PyFlink streaming (bronze/silver/gold)
│   ├── producers/             # Python poller (hotspot + subway)
│   └── platform_common/       # 공통 settings
└── tests/unit/                # pytest (25+ case)
```

## CI / 테스트

- `uv run ruff check` — 전체 repo lint 통과
- `uv run pytest tests/unit/ -v` — 25+ case PASS
- `cd dbt/seoul && dbt parse && dbt compile` — dbt 모델 컴파일 확인
- GitHub Actions `.github/workflows/ci.yml` — 매 PR 자동 실행

## Phase 1A 종료 게이트 (Day 10 시점)

본 프로젝트의 **체크포인트 1** = 14일 일정의 첫 milestone. 다음 12+ 항목 모두 통과 = Phase 1A 종료 게이트 통과.

### 인프라 / 코드 검증

```bash
# 1) Docker compose service 모두 healthy
./scripts/healthcheck.sh                       # failed 0

# 2) pytest 단위 테스트 25+ PASS
uv run pytest tests/unit/ -v

# 3) ruff lint 전체 통과
uv run ruff check

# 4) DuckDB 의 Iceberg 직접 쿼리 — bronze + silver + gold 정상 row
uv run python scripts/duckdb_check.py
```

### SLO + dbt 검증

```bash
# 5) 두 가지 SLO 측정 (data freshness + platform latency)
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
  uv run --extra flink python -m flink_jobs.slo_metrics

# 6) dbt 3 모델 + 6 테스트 PASS
cd dbt/seoul && DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt run && dbt test && cd ../..
```

### Day 9 Spark + Iceberg Compaction 결과 (Day 9 archive SoT)

- **Spark MERGE INTO 멱등성** — `rows=5 hash=b72679e91078` (1회차 = 2회차 동일 hash)
- **Iceberg Compaction reduction** — `silver.hotspot_congestion` files 475 → 3 (-99.4%, 23x 쿼리 가속)

### Airflow 본진 4 DAG 라인업

```bash
# 7) DAG 4개 모두 존재 + healthy
docker exec scp-airflow-scheduler airflow dags list | \
    grep -E "dbt_full_run|iceberg_maintenance|backfill_silver_from_bronze|slo_daily_report"

# 8) slo_daily_report DAG manual trigger 1회 + branch 분기 시각 확인
docker exec scp-airflow-scheduler airflow dags test slo_daily_report $(date +%Y-%m-%d)
# Airflow UI > slo_daily_report > Graph view 에서 한쪽 branch 만 색칠 확인 (Day 10 PR α SoT)

# 9) 메모리 임계 — used < 19.2GB (80% 임계, 24GB 전체)
free -h 2>/dev/null || vm_stat | head -8
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}"
```

### 외부 노출 / 운영

```bash
# 10) phase-1a-v1 git tag 푸시 (PR γ 머지 후 별도 단계)
git tag -a phase-1a-v1 -m "Phase 1A 종료 — Day 1-10 완료"
git push origin phase-1a-v1

# 11) GitHub Actions 의 ci.yml green 확인
gh run list --limit 5

# 12) Cloudflare Pages 도메인 + /chill 접속 가능
curl -sI https://seoul-citydata.pages.dev/chill | head -3
```

상세 = [`docs/portfolio/phase1a_v1.md`](./docs/portfolio/phase1a_v1.md) 의 p4 SLO + p5 트러블슈팅 + p6 Airflow.

## 운영 비용

| 항목 | 비용 |
|---|---|
| Oracle Cloud Always Free VM (ARM Ampere A1, 4 vCPU / 24GB RAM) | $0 |
| Oracle Object Storage 10GB Free (또는 로컬 MinIO) | $0 |
| Cloudflare Pages + Tunnel + D1 (P1B) | $0 |
| 공공 API 호출 (도시데이터 + 지하철 + 인허가) | $0 |
| Iceberg Compaction 의 S3 PutObject (Day 9 본격) | $0.83 |
| **합계** | **$0 ~ $0.83/월** |

## Phase 1B / 2 로드맵 (요약)

- **Phase 1B (Day 11-14)**: `user.events.v1` Kafka 토픽 + Cloudflare Edge API (REST Proxy) + D1 익명 북마크 + Workers Cron + Web Push
- **Phase 2 (8 주)**: 회원가입 + UGC 별점 + 외부 가게 정보 (영업 시간 + 별점) 보강 + Trino + Superset + Great Expectations + Terraform + Grafana

상세 = [`docs/portfolio/phase1a_v1.md` p7 참조](./docs/portfolio/phase1a_v1.md).

## 문서 / 레퍼런스

- 프로젝트 컨텍스트: [`CLAUDE.md`](./CLAUDE.md)
- Phase 1 spec (단일 출처): [`docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`](./docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md)
- Phase 1A Week 1 plan: [`docs/superpowers/plans/phase-1a-week-1.md`](./docs/superpowers/plans/phase-1a-week-1.md)
- Phase 1A Week 2 plan: [`docs/superpowers/plans/phase-1a-week-2.md`](./docs/superpowers/plans/phase-1a-week-2.md)
- CONTRIBUTING (PR / commit / branch 컨벤션): [`CONTRIBUTING.md`](./CONTRIBUTING.md)

## 라이선스

본 프로젝트는 공공 실시간 API + 공공 인허가 (공공누리, 출처 표시) + Postgres CDC + OSS (Apache 2.0 등) 위에서 동작.
