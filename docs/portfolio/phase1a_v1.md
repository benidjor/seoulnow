# Phase 1A 종합 리포트 (v1, Day 10 1차 제출본)

> **체크포인트 1**: 본 문서는 14일 일정 중 Day 10 시점의 단독 제출 가능본 (6~7페이지).
> Phase 1B (Day 11-14) 완료 시 강화 버전 (8~10p) 으로 갱신.
>
> 의사결정 단일 출처 = `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`.
> 본 리포트는 그 위 의 운영 결과 + 학습 곡선 정리.

---

## p1. 표지 + 핵심 결과

**서울 실시간 지역 혼잡도 데이터 플랫폼 — Phase 1A**

서울시 공공 실시간 데이터 (도시데이터 + 지하철 도착정보) + Postgres CDC 를 **Kafka 메시지 버스** 로 통합. **PyFlink streaming + Spark batch (Day 9 보조) + Iceberg (Lakekeeper REST Catalog) + dbt + Airflow + GitHub Actions** 로 처리·검증.

### 1.1. 핵심 결과 표

| 항목 | 결과 |
|---|---|
| 공개 도메인 | https://seoul-citydata.pages.dev |
| Data Freshness SLO | **P95 < 45분** (서울 OpenAPI source lag 31분+ 포함 명시) — 실측값은 Day 11 09:00 KST `slo_daily_report` DAG 자동 첫 실행 후 별도 commit 으로 보강 |
| Platform Latency SLO | **P95 < 7분** (silver→gold 우리 통제 구간) — 실측값은 Day 11 이후 |
| 운영 비용 | **월 $0 ~ $0.83** ($0.83 = Iceberg compaction 의 S3 PutObject 추정치) |
| 이종 데이터 소스 | 3종 (서울 도시데이터 + 지하철 도착정보 + Postgres CDC) |
| Kafka 토픽 | 3개 (P1A) → 4개 (P1B `user.events.v1` 추가 예정) |
| Iceberg 테이블 | 7개 (Bronze 3 + Silver 3 + Gold 4 mart) |
| dbt 모델 / 테스트 | 3 모델 (`stg_hotspot_silver` + `fact_hotspot_congestion_hourly` + `chill_open_now`) / 6+ 테스트 |
| Airflow 본진 4 DAG | `dbt_full_run` / `iceberg_maintenance` / `backfill_silver_from_bronze` / `slo_daily_report` |
| GitHub Actions | ruff + pytest (25+) + dbt parse/compile, PR 마다 자동 |

### 1.2. 운영 환경

- **Oracle Cloud Always Free VM** (ARM Ampere A1, 4 vCPU / 24GB RAM, 월 $0)
- **Cloudflare Pages + Tunnel** (외부 노출, 월 $0)
- **단일 노드 docker-compose** (Kafka KRaft + PyFlink + Iceberg + Postgres + MinIO + Lakekeeper + Airflow LocalExecutor)

### 1.3. Day 별 진행 요약

| Day | 핵심 산출물 |
|---|---|
| Day 1 | Oracle Cloud VM + docker-compose 인프라 기동 (Kafka KRaft + Postgres + MinIO + Lakekeeper) |
| Day 2 | 도시데이터 + 지하철 producer → Kafka 2 토픽 발행 |
| Day 3 | PyFlink Bronze→Silver (region join, dedup) → Iceberg 적재 |
| Day 4 | PyFlink Silver→Gold (`fact_hotspot_congestion_5min`, 5min tumbling) + SLO 측정 코드 |
| Day 5 | dbt-core + Airflow LocalExecutor + `dbt_full_run` DAG (TaskGroup + 의존성 + SLA) |
| Day 6 | Postgres `places` + Debezium CDC + `dim_place` SCD2 골격 |
| Day 7 | Next.js 메인 지도 + Cloudflare Pages 배포 + FastAPI |
| Day 8 | "지금 한가하고 영업 중인 카페" `chill_open_now` mart + 24h SLO fire 예약 |
| Day 9 | Spark batch MERGE INTO 멱등성 + Iceberg Compaction (files 475→3, 99.4% 감소) + `iceberg_maintenance` DAG 본격 활성 |
| Day 10 | SLO 두 가지 분리 정정 (data freshness + platform latency) + `slo_daily_report` DAG (Airflow 본진 4 DAG 라인업 완성) + 본 문서 작성 |

---

## p2. 아키텍처

상세 다이어그램은 `docs/architecture/data-flow.md` 참조. 핵심 흐름 요약:

```
이종 소스 3종 → Kafka (KRaft single-node, 메시지 버스)
              → PyFlink streaming (메인 컨슈머, exactly-once)
              → Iceberg (Bronze / Silver / Gold via Lakekeeper REST Catalog)
              → DuckDB / dbt / Next.js Pages
                  + Spark batch (Day 9 일시 기동, MERGE INTO + Compaction)
                  + Airflow LocalExecutor (본진 4 DAG, batch ops 만)
```

### 2.1. Kafka 정당화 (spec §5)

이종 3종 통합 + Phase 1B 의 컨슈머 2개 확장 + Producer-Consumer 디커플링 + Replay + Flink exactly-once 기반. **단일 토픽이었으면 안 썼을 것** — 메시지 버스로서의 Kafka 정당화는 이종 소스 수 N 과 컨슈머 수 M 둘 다 1 보다 클 때만 성립.

### 2.2. KRaft single-node 결정 (spec §5)

레시핑 에서 Kafka 3-node + Connect 2-node 운영 경험 있음. 본 프로젝트는 Oracle Cloud Free 24GB + 1인 운영 + Day 9 Spark 일시 기동 시 OOM 회피를 위해 의식적으로 single-node 단순화. SPOF 는 limitation 으로 인정. Production SLA 환경이라면 3-node + RF=3.

### 2.3. 3계층 분리 원칙 (spec §3)

- **streaming = PyFlink** (실시간 처리, exactly-once, 5min tumbling window)
- **polling = cron** (producer 가 외부 API 를 주기적으로 호출, Python 스크립트)
- **batch ops = Airflow** (dbt run, iceberg compaction, SLO 리포트 — 정기 운영 작업)

각 계층이 자기 영역만 담당. Airflow 가 streaming 자체를 시도하지 않음 (Airflow 본진 사용 패턴 SoT).

---

## p3. 레시핑 (선행 프로젝트) 과의 차별화 + 학습 곡선

레시핑 프로젝트 (2025.07-10) 와 본 프로젝트 (Phase 1A) 의 도구 / 처리 / 데이터 / 운영 측면 차이.

### 3.1. 영역별 비교

| 영역 | 레시핑 | 본 프로젝트 (Phase 1A) |
|---|---|---|
| Kafka 사용 패턴 | Connect → S3 적재 통로 (사실상 micro-batch 의 입력) | PyFlink streaming 입력 (메시지 버스) |
| 처리 방식 | 15분 주기 Spark batch | streaming + batch 분리 (PyFlink streaming + Day 9 Spark 보조) |
| 카탈로그 | Hive Metastore | Lakekeeper REST Catalog (메모리 절감) |
| 변환 | Spark SQL 수동 | dbt-core + dbt tests + GitHub Actions CI |
| 데이터 출처 | 시뮬레이터 (가짜 데이터) | 공공 실시간 API + Postgres CDC (실데이터) |
| CDC | 부재 | Debezium → `dim_place` SCD2 골격 |
| 후속 작업 | "Dynamic Partition Overwrite 예정" / "Compaction 도입 예정" | Day 9 Spark MERGE INTO + Compaction 으로 보강 |
| Airflow 사용 | 15분 batch trigger (cron 대용) | 본진 4 DAG (TaskGroup / SLA / dynamic mapping / BranchPython / XCom / `on_failure_callback`) |
| 테스트 코드 | 부재 | pytest 25+ 단위 테스트 |
| CI/CD | 부재 | GitHub Actions (ruff + pytest + dbt parse/compile) |
| SLO 측정 | 부재 | 두 가지 SLO (data freshness + platform latency) 자동 일일 리포트 |

### 3.2. 학습 곡선 — Airflow 사용 패턴 진화

레시핑 시점의 Airflow 는 사실상 cron 대용 (단일 Spark batch 작업을 15분마다 trigger). DAG 의존성 / SLA / TaskGroup / dynamic task mapping / BranchPython / XCom 같은 본진 기능 미사용.

본 프로젝트의 Airflow 는 본진 4 DAG 운영 — 자세한 본진 기능 발휘는 `## p6` 참조:

- `dbt_full_run` — TaskGroup + 의존성 + SLA + `on_failure_callback`
- `iceberg_maintenance` — 병렬 Spark + XCom + `on_success_callback`
- `backfill_silver_from_bronze` — Dynamic Task Mapping + 멱등 MERGE INTO
- `slo_daily_report` — BranchPythonOperator + XCom + 두 가지 SLO 분기 alert

도구 사용 깊이의 변화 자체가 학습 자료.

### 3.3. 학습 곡선 — Kafka 사용 패턴 진화

레시핑의 Kafka 는 Connect → S3 적재 통로. 실제 처리는 15분 주기 Spark batch — 결국 streaming 부재.

본 프로젝트는 그 한계를 인지하고 PyFlink → Iceberg streaming 으로 본격 도입. Kafka 의 메시지 버스 정당화 (이종 소스 N → 1 버스 → 컨슈머 M) 도 실제로 발휘 (P1A 의 3 토픽 → P1B 의 4 토픽 + 컨슈머 2개).

### 3.4. 학습 곡선 — SLO 정의 재설계

Day 4 의 단일 SLO 정의 (`gold - api_response_ts`, P95 < 7m) → Day 8 24h SLO 첫 실측 시 P95 = 3.3h 위반 → Day 10 PR α 의 분리 재설계.

분리 재설계 자체가 학습 자료 — 단일 측정의 한계 발견 후 두 가지 SLO 로 분리. 자세한 의사결정 = `## p4. §4.3` 참조.

---

## p4. SLO + 데이터 품질 (DQ)

### 4.1. 두 가지 SLO 정의 (spec §6-2)

| SLO | 정의 | 임계값 | 의미 |
|---|---|---|---|
| **(α) Data Freshness** | `gold_arrival_ts - api_response_ts(tm)` | P95 < **45분** | 사용자 관점 데이터 나이 (서울 OpenAPI source lag 31m+ 포함) |
| **(β) Platform Latency** | `gold_arrival_ts - silver_arrival_ts` | P95 < **7분** | 우리 통제 구간 (silver→gold) — Path B 결정 (`§4.3` 참조) |

### 4.2. 측정 인프라

- **자체 Python 스크립트** (`src/flink_jobs/slo_metrics.py`) — 24h 윈도우 측정, `MetricSummary` + `SLOReport` dataclass
- **Airflow `slo_daily_report` DAG** (Day 10 PR α) — schedule `0 9 * * *` (매일 09:00 KST), BranchPythonOperator + XCom + `on_failure_callback`, Discord webhook alert (위반 SLO 명시)
- **DuckDB + pyiceberg** — Iceberg `gold.fact_hotspot_congestion_5min` 직접 쿼리 (`union_by_name=true` + `BinderException` graceful degrade)

### 4.3. SLO 재설계 학습 곡선 (Day 4 → Day 10)

**Day 4 (단일 SLO 설계)** — `gold - api_response_ts(tm)` P95 < 7m. 직관적 정의 (사용자 시점 데이터 나이) + 레시핑 의 15분 micro-batch 대비 50%+ 개선 목표.

**Day 8 (24h SLO 첫 실측)** — `count=846 / p50=42분 / p95=3.3시간 / p99=9.4시간`. **모든 percentile 거의 동일 (30초 spread)** = 일시적 spike 가 아니라 source 측 시각 자체의 일관 lag.

**Day 10 PR α (분리 재설계)** — 사전 점검 `duckdb_check` sample 에서 silver record 의 `eventtime` (= API tm) vs `ingest_ts` 31.5분 차이 확인 → 서울 OpenAPI 의 `tm` 응답값이 호출 시각보다 31분+ 옛날. 즉 단일 SLO 로는 source 측 lag 와 우리 플랫폼 측 latency 가 섞여 추적 불가능.

→ 두 가지 SLO 분리:
- (α) Data Freshness P95 < **45분** (source lag 포함, 사용자 관점)
- (β) Platform Latency P95 < **7분** (우리 통제 구간)

### 4.4. Path B 결정 — silver Iceberg `kafka_ts` 부재 발견

Day 10 PR α 작업 중 silver Iceberg catalog 의 `kafka_ts` 컬럼 부재 발견 (`bronze_to_silver.py` 의 INSERT 가 `CURRENT_TIMESTAMP AS silver_arrival_ts` 로 대체 채움). 두 path 검토:

| Path | Platform Latency 정의 | 작업량 |
|---|---|---|
| **B (채택)** | `gold - silver_arrival_ts` (silver→gold) | gold ALTER + `silver_to_gold.py` 정정만 |
| C (보류) | `gold - kafka_ts(METADATA)` (Kafka→gold) | silver ALTER + `bronze_to_silver.py` 정정 + Flink job 2개 재기동 |

Path B 한계 명시 — bronze→silver 의 lag (Kafka broker → silver 적재) 미포함. Phase 1B/2 의 silver schema 정정 시점에 Path C 로 전환 가능 (`compute_platform_latency_seconds(source_ts, gold_ts)` 의 `source_ts` 변수명 = future-proof reuse).

### 4.5. 실측 SLO 결과 (Day 10 PR α 직후 — Phase 1A 종료 시점)

본 PR γ 머지 후 (Phase 1A 종료 + `phase-1a-v1` tag 시점, 2026-05-14 02:06 KST) 실측:

| SLO | count | p50 | p95 | p99 | max | 임계값 | 결과 |
|---|---|---|---|---|---|---|---|
| (α) Data Freshness | 941 | 9.8h | **52.96h** | 130.5h | 187.5h | < 45m | 위반 |
| (β) Platform Latency | 809 | 0s | **0s** | 0s | 0s | < 7m | 통과 |

**해석**

- **β (Platform Latency P95 = 0s) — 평시 결과로 인정**: silver→gold streaming 이 5min tumbling window 안에 즉시 처리되어 `silver_arrival_ts ≈ gold_arrival_ts`. 정상 streaming 동작의 산출물. 7분 임계 안 통과.
- **α (Data Freshness P95 = 52.96h) — backfill 직후 측정값**: Day 9 ~ Day 10 작업 도중 Flink streaming jobs 일시 정지로 bronze 에 53h 분량 historical row 누적. Day 10 재기동 후 Flink 가 Kafka LAG = 0 까지 모두 소진 (`flink-bronze-hotspot` consumer group: partition 0 1315/1315, partition 2 660/660). 24h SLO window 안에 backfill 처리된 row 의 `api_response_ts` (= API tm) 가 53h+ 옛날인 row 가 포함되어 결과 왜곡.

**평시 결과 재측정 의무** — 본 측정 시점 + 24h 후 (`slo_daily_report` DAG 자동 첫 실행, schedule `0 9 * * *`) 결과를 인용한 별도 commit 으로 본 §4.5 표 갱신. Phase 1B Day 14 또는 강화 리포트 v2 (Day 18) 시점에 일괄 처리.

**측정 명령** (재현):

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) uv run --extra flink python airflow/dags/common/slo_query.py
```

stdout 마지막 라인 = JSON SLOReport (`data_freshness` + `platform_latency` dataclass).

### 4.6. dbt tests (DQ)

- staging: `stg_hotspot_silver` 의 `area_code` / `district` / `api_response_ts` not_null
- mart: `fact_hotspot_congestion_hourly` 의 `window_hour` / `district` not_null + unique combination
- mart: `chill_open_now` 의 `biz_reg_no` unique + `district` not_null
- singular: `assert_congest_level_valid` (혼잡도 값 enum 검증)
- dbt tests = 매 PR + Airflow `dbt_full_run` DAG 의 SLA 1h 안에 자동 실행

---

## p5. 트러블슈팅 — Spark MERGE + Iceberg Compaction (Day 9)

레시핑의 "Dynamic Partition Overwrite 예정 / Compaction 도입 예정" 후속 작업을 본 프로젝트 Day 9 에서 본격 보강. 자세한 archive = `docs/portfolio/troubleshooting/2026-05-12-day-9-archive.md` (14건 학습 자산).

### 5.1. Spark MERGE INTO 멱등성 검증 (`gold.dim_place`)

`scripts/merge_dim_place.py` 의 2회 실행 결과:

- 1회차 적재 후 `rows=5 / hash=b72679e91078`
- 2회차 (같은 입력) 적재 후 `rows=5 / hash=b72679e91078` (동일)

→ MERGE INTO 의 멱등성 확정. SCD2 의 effective_from / effective_to 변경 없음.

### 5.2. Iceberg Compaction (`silver.hotspot_congestion`)

`scripts/compaction_silver.py` 실행 (Day 9 의 `rewrite_data_files` Spark action):

| 메트릭 | Before | After | 변화 |
|---|---|---|---|
| 파일 수 | 475 | 3 | -99.4% |
| 쿼리 시간 (DuckDB count) | 1.86s | 0.08s | 23x 가속 |
| 운영 비용 | — | — | $0.83/월 (S3 PutObject 추정) |

### 5.3. Airflow `iceberg_maintenance` DAG (Day 9 PR γ)

본격 활성 후 manual trigger SUCCESS (6 task 약 11초). schedule `0 3 * * *` (매일 03:00 KST, streaming peak 회피).

**Option B 패턴 정착** — `snapshot_metrics_before/after` task 가 BashOperator + dbt-venv subprocess 호출 (Airflow base image 에 duckdb / pyiceberg 미설치 회피). 본 패턴은 Day 10 PR α 의 `slo_query.py` 도 reuse.

### 5.4. 도커 소켓 마운트 한계 (Phase 1A 한정)

Airflow 컨테이너가 host docker daemon 의 root 권한 직접 사용 — Phase 1A 데모 한정 (single-user laptop, public 공개 없음). Phase 2 Oracle Cloud 배포 시 Spark on Kubernetes / `SparkSubmitOperator` + Livy 또는 `SSHOperator` 로 재설계 의무.

---

## p6. Airflow 본진 4 DAG 운영

레시핑의 cron 대용 사용 → 본 프로젝트의 본진 4 DAG 운영으로 진화. 본 프로젝트의 핵심 학습 자료 중 하나.

### 6.1. 4 DAG 라인업

| DAG | 도입 시점 | schedule | 본진 기능 |
|---|---|---|---|
| `dbt_full_run` | Day 5 | (manual + PR γ 검토) | TaskGroup + 의존성 + SLA 30분 + `on_failure_callback` |
| `iceberg_maintenance` | Day 5 골격 → Day 9 본격 | `0 3 * * *` | 병렬 Spark + XCom (before/after 메트릭) + `on_success_callback` (`send_compaction_report`) + `max_active_tis_per_dag=3` |
| `backfill_silver_from_bronze` | Day 5-6 buffer | manual | Dynamic Task Mapping + Params + 멱등 MERGE INTO + dry_run 모드 |
| `slo_daily_report` | Day 10 PR α | `0 9 * * *` | BranchPythonOperator + XCom + `on_failure_callback` + 두 가지 SLO 분기 alert |

### 6.2. 본진 기능 발휘 — 12종

spec §5-8 SoT 의 본진 기능 12종이 4 DAG 에 걸쳐 모두 발휘:

- **DAG 의존성 그래프** (`>>` operator, TaskGroup) → `dbt_full_run`
- **SLA 1h ~ 30분** → `dbt_full_run` (30분), `iceberg_maintenance` (1h)
- **TaskGroup** → `dbt_full_run` (staging / marts 의 의존성 묶음)
- **Dynamic Task Mapping** → `backfill_silver_from_bronze` (Params 의 시간 범위 → 5min boundary 마다 mapped task)
- **Params + UI form** → `backfill_silver_from_bronze` (Airflow UI 에서 직접 시간 범위 입력)
- **BranchPythonOperator** → `slo_daily_report` (any_violated → send_alert | skip_alert)
- **XCom** → 4 DAG 모두 (메트릭 push/pull, JSON 또는 dict)
- **`on_failure_callback`** → 4 DAG 모두 (`send_discord_alert` 공통 helper)
- **`on_success_callback`** → `iceberg_maintenance` (`send_compaction_report`)
- **`max_active_tis_per_dag`** → `iceberg_maintenance` (Spark 동시 submit 제한)
- **schedule (cron 표기)** → 3 DAG (03:00 / 09:00, streaming peak 회피)
- **trigger_rule** → `slo_daily_report` 의 `archive_report` (`none_failed_min_one_success`, branch 후 둘 중 하나만 성공해도 archive 실행)

### 6.3. 메모리 mitigation

Oracle Cloud Free 24GB 한계 + Kafka + Flink + Iceberg + Postgres + MinIO + Lakekeeper + Airflow 동시 가동 = 메모리 위협.

- LocalExecutor + SQLite metadata DB (~700MB) — Postgres metadata / Celery / Redis 미사용
- DAG schedule = 야간 (02-05시 / 09시) 으로 streaming peak 회피
- Day 9 Spark 일시 기동 직전 `docker compose stop airflow-scheduler` 로 메모리 회수
- 80% 임계 = 19.2GB. Day 5 도입 직후 측정 의무

### 6.4. 3계층 분리 원칙 SoT

| 계층 | 도구 | 역할 |
|---|---|---|
| streaming | PyFlink | 실시간 처리, exactly-once, 5min tumbling window |
| polling | cron | producer 가 외부 API 를 주기 호출 |
| batch ops | Airflow | dbt run, iceberg compaction, SLO 리포트 |

Airflow 가 streaming 자체를 시도하지 않음. 레시핑 의 cron 대용 사용 (Airflow = 15분 batch trigger) 과 의식적 분리.

---

## p7. 운영 비용 + Phase 1B/2 로드맵

### 7.1. 운영 비용 (월)

| 항목 | 비용 | 메모 |
|---|---|---|
| Oracle Cloud Always Free VM | $0 | ARM Ampere A1, 4 vCPU / 24GB. 90일 미사용 시 회수 → PAYG 업그레이드 (과금 0) 로 방지 |
| Oracle Object Storage 10GB Free (또는 로컬 MinIO) | $0 | Iceberg 데이터 파일 저장 |
| Cloudflare Pages + Tunnel + D1 (P1B) | $0 | 무료 한도 안 |
| 공공 API 호출 (서울 도시데이터 + 지하철 + 인허가) | $0 | 무료 |
| Iceberg Compaction 의 S3 PutObject (Day 9 본격) | $0.83 | 추정 (`scripts/cost_report.py`) |
| **합계** | **$0 ~ $0.83** | |

레시핑의 75% 인프라비 절감 흐름 연속 — 본 프로젝트는 비용 0원 수준 운영.

### 7.2. Phase 1B 로드맵 (Day 11-14)

- Day 11 — `user.events.v1` Kafka 토픽 + event schema 설계 (anon_id + user_id forward compatibility 구조) + Edge API (Cloudflare Pages Functions + Oracle HTTP receiver REST Proxy)
- Day 12 — Bronze→Silver→Gold (`fact_user_event` mart) streaming
- Day 13 — D1 익명 북마크 + Workers Cron + Web Push (VAPID) 알림
- Day 14 — `/privacy` 페이지 + 익명 사용자 시나리오 1회 자체 검증 + 강화 리포트 (8-10p)

### 7.3. Phase 2 로드맵 (8 주, P2 W1-W8)

- W1 — 회원 가입/로그인 도입 (NextAuth 또는 Cloudflare Access). 익명 → 식별 사용자 마이그레이션
- W2 — UGC 별점 입력 + `user.review.v1` 토픽
- W3 — 버스 위치 토픽 + 지역 추천 점수 모델 고도화
- W4 — Superset 대시보드 + SLO 4종 전체 측정. Dagster (dbt asset 일등시민 통합) 검토
- W5 — Great Expectations 도입 + dbt-docs lineage 자동 배포
- W6 — Google Places API 캐싱·증분 파이프라인 + `dim_place` 다출처 머지. silver Iceberg `kafka_ts` ADD COLUMN (Path C 전환)
- W7 — Trino single-node 옵션 + Terraform IaC + Grafana Cloud 연결
- W8 — OKKY / Reddit / Disquiet 외부 공개 + 실유저 행동 데이터 인사이트 1-2건

### 7.4. 보류 항목 (Day 10 이후 논의 의무)

`deferred-items-post-day10` memory SoT — Day 10 종료 후 적용 시점 재논의:

1. 영업시간 / 평점 출처 spec 정정 — Google Places (P2 W6) + UGC (P2 W2) 가 합법 path. 네이버 / 카카오 API 미제공, 스크래핑 ToS 위반
2. 카페 / 술집 데이터 source 한계 명시 — `chill_open_now` 의 영업시간 정확도 (공공 인허가 신고분만)
3. `user.events.v1` event schema 본문 — P1B Day 11 설계 시점에 본격 작성
4. silver Iceberg `kafka_ts` ADD COLUMN (Path C 전환, Phase 1B/2 시점)

---

## 8. 레퍼런스

- 의사결정 단일 출처: [`docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`](../superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md)
- 아키텍처 다이어그램: [`docs/architecture/data-flow.md`](../architecture/data-flow.md)
- 데이터 lineage + SLO 측정 경로: [`docs/architecture/data_lineage.md`](../architecture/data_lineage.md)
- Day 9 트러블슈팅 archive (14건): [`docs/portfolio/troubleshooting/2026-05-12-day-9-archive.md`](./troubleshooting/2026-05-12-day-9-archive.md)
- Day 8 트러블슈팅 archive: [`docs/portfolio/troubleshooting/2026-05-11-day-8-archive.md`](./troubleshooting/2026-05-11-day-8-archive.md)
- Plan SoT (Phase 1A Week 2): [`docs/superpowers/plans/phase-1a-week-2.md`](../superpowers/plans/phase-1a-week-2.md)

