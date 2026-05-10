# Day 5 Airflow 운영 runbook

> 작성: 2026-05-10
> 영역: Day 5 Task 5.5 (Airflow 셋업) + Task 5.6 (dbt_full_run DAG) + Task 5.7 (backfill DAG) + Task 5.8 (iceberg_maintenance 골격)
> 관련 PR: #30 (Task 5.5~5.6) + #31 (Task 5.7~5.8)

## 평소 운영

### 환경 셋업 (1회)

```bash
# .env 에 Airflow secret 추가 (FERNET_KEY / WEBSERVER__SECRET_KEY 생성)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"
# .env 에 AIRFLOW_FERNET_KEY / AIRFLOW__WEBSERVER__SECRET_KEY / AIRFLOW_UID=50000 추가

# host bind mount target 디렉토리
mkdir -p airflow/logs

# image build (1회, ~5분)
docker compose build airflow-init
```

### 평소 기동

```bash
docker compose up -d postgres-airflow-bootstrap airflow-init airflow-webserver airflow-scheduler

# 가동 순서: postgres-airflow-bootstrap (CREATE DATABASE airflow_meta) → airflow-init
# (db migrate + admin user) → airflow-webserver / airflow-scheduler

# webserver ready 대기 (~30초)
until curl -sf http://localhost:8080/health >/dev/null; do sleep 5; done

# UI 접속: http://localhost:8080  (admin / admin, .env 의 AIRFLOW_ADMIN_*)
```

### 정지

```bash
# 일시 정지 (volume 보존)
docker compose stop airflow-webserver airflow-scheduler

# 완전 정지 (init container 도 삭제)
docker compose down airflow-webserver airflow-scheduler airflow-init
```

### 헬스체크

```bash
./scripts/healthcheck.sh
# 6 components — Kafka / Postgres / MinIO / Lakekeeper / Airflow / docker compose ps

# 실 사용량 측정
docker stats --no-stream | grep airflow
free -h  # Mac 사용량 19.2GB 임계
```

### Airflow CLI

```bash
# DAG 등록 확인
docker compose exec -T airflow-scheduler airflow dags list

# import error 진단 (DAG load fail 시 첫 단계)
docker compose exec -T airflow-scheduler airflow dags list-import-errors

# task graph 시각
docker compose exec -T airflow-scheduler airflow tasks list <dag_id> --tree

# 수동 trigger
docker compose exec -T airflow-scheduler airflow dags trigger <dag_id>

# manual run (test)
docker compose exec -T airflow-scheduler airflow dags test <dag_id> $(date +%Y-%m-%d)
```

---

## DAG 4 운영 (Day 5 종료 시점 = 3 등록 + Day 10 추가 1개)

| DAG | 도입 | schedule | 상태 | 용도 |
|---|---|---|---|---|
| `dbt_full_run` | Task 5.6 (PR #30) | `0 2 * * *` (02:00 KST) | paused (default) | dbt staging → marts 순차 실행 + tests + Discord callback |
| `backfill_silver_from_bronze` | Task 5.7 (PR #31) | None (수동 trigger) | paused | Dynamic Task Mapping 기반 시간 partition 별 silver 재처리. Day 9 본격 |
| `iceberg_maintenance` | Task 5.8 (PR #31) | None (Day 9 활성) | paused | Compaction + snapshot expire 골격. Day 9 SparkSubmitOperator 교체 |
| `slo_daily_report` | Day 10 추가 | TBD | TBD | BranchPythonOperator + SLO 분기 |

### `dbt_full_run` (Task 5.6)

본진 기능:
- TaskGroup `staging` (run + test) → TaskGroup `marts` (run + test). staging test 실패 시 marts 자동 skip.
- retry policy — retries=2, exponential backoff, retry_delay 5분.
- SLA 30분.
- on_failure_callback — Discord webhook (env 빈 값 시 stdout fallback).

Activate 시 — Airflow UI 에서 toggle 또는:
```bash
docker compose exec -T airflow-scheduler airflow dags unpause dbt_full_run
```

### `backfill_silver_from_bronze` (Task 5.7)

본진 기능:
- Dynamic Task Mapping `process_partition.partial(cfg=cfg).expand(partition=partitions)` — 런타임 N 개 task 자동.
- Params (UI 입력) — `start_ts`, `end_ts`, `tables`, `dry_run`.
- max_active_tis_per_dag=2 — Spark 동시 submit 2개 제한.

Day 5 시점 = Spark job 본문 echo placeholder. Day 9 Task 9.2 진입 시점에 spark-submit 실 호출 + 멱등 MERGE INTO + dedup_key.

UI trigger 시 Params:
```json
{
  "start_ts": "2026-05-09T00:00:00",
  "end_ts": "2026-05-09T03:00:00",
  "tables": ["silver.hotspot_congestion"],
  "dry_run": true
}
```

### `iceberg_maintenance` (Task 5.8)

본진 기능 (Day 9 본격):
- 병렬 TaskGroup `rewrite` (rewrite_fact_hotspot_congestion_5min + rewrite_dim_place).
- max_active_tis_per_dag=3.
- XCom — before/after 메트릭 (file_count / total_bytes / snapshot_count).
- SLA 1시간.

Day 5 시점 = BashOperator echo placeholder. Day 9 Task 9.3 진입 시점에 SparkSubmitOperator 교체 + Iceberg `rewrite_data_files` / `expire_snapshots` / `remove_orphan_files`.

---

## 메모리 mitigation 정책 (spec §5-8 / §9-3)

24GB 환경 + Kafka + Lakekeeper + MinIO + Postgres + (Day 9 시 Spark 일시 기동) 와의 ceiling 관리:

- **LocalExecutor + 기존 scp-postgres backend** — 별도 Postgres meta container 안 띄움. ~30MB Postgres DB 만 추가. (plan 의 SQLite metadata 가 LocalExecutor lock 충돌이라 fall back).
- **`AIRFLOW__CORE__PARSING_PROCESSES=1`** — scheduler 의 DAG parsing process 수 제한.
- **`AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300`** — DAG file 재 parse 주기 5분 (CPU 절감).
- **DAG schedule 야간 정책** — `dbt_full_run` 02:00 KST, `iceberg_maintenance` (Day 9) 03:00 KST. streaming peak (낮 시간) 회피.
- **Day 9 Spark 일시 기동 시점** — `docker compose stop airflow-scheduler` 로 ~600MB 회수.

현재 (Day 5 종료) docker stats 기준:
```
scp-airflow-scheduler   ~555MB
scp-airflow-webserver   ~734MB
Airflow 합계           ~1.3GB
전체 docker            ~2.8GB
Mac 사용량              ~19GB (80% 임계 19.2GB 안)
```

---

## 환경 편차 / 정책

### 의존성 충돌 영역 (Task 5.5)

| 충돌 | 해결 | 참조 |
|---|---|---|
| LocalExecutor + SQLite — lock 충돌 | LocalExecutor + 기존 scp-postgres 안 airflow_meta DB | [archive §2](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#2-이슈-1--sqlite-metadata--localexecutor-lock-충돌-task-55) |
| pip ResolutionTooDeep | uv resolver 채택 (`COPY --from=ghcr.io/astral-sh/uv`) | [archive §3](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#3-이슈-2--pip-resolutiontoodeep-task-55) |
| postgres provider 5.7~6.0 vs constraints 6.0.0 | 상한 제거 (constraints lock 우선) | [archive §4](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#4-이슈-3--airflow-constraints-vs-requirements-의-postgres-provider-버전-충돌-task-55) |
| dbt-core protobuf vs Airflow constraints | dbt 별도 venv (`/opt/airflow/dbt-venv`) | [archive §5](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#5-이슈-4--dbt-core-protobuf-vs-airflow-protobuf-양립-불가-task-55) |
| system venv permission denied | USER root install + USER airflow 복귀 | [archive §6](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#6-이슈-5--system-venv-permission-denied-task-55) |

### Airflow reserved keyword 회피 (Task 5.7)

DAG 의 task 인자명 / local 변수 이름 — 다음 reserved 회피:

```
dag, task, ti, task_instance,
params, run_id, execution_date, logical_date,
data_interval_start, data_interval_end,
var, conn, macros, prev_*
```

권장 인자명 — `cfg` / `config` / `payload` / `data` / `args` (도메인 의미 명시).

상세 — [archive §7](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#7-이슈-6--airflow-reserved-keyword-params-충돌-task-57).

---

## 자주 발생하는 문제

| 증상 | 원인 | 해결 | 참조 |
|---|---|---|---|
| `airflow dags list` 가 새 DAG 미인식 | `dag_dir_list_interval=300` (5분 parse 주기) | 5분 대기, 또는 `airflow dags reserialize` (강제 재 parse) | 본 runbook §평소 운영 |
| DAG load fail (import error) | Reserved keyword 충돌 / syntax / import 경로 | `airflow dags list-import-errors` 로 정확한 line 확인 | [archive §7](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#7-이슈-6--airflow-reserved-keyword-params-충돌-task-57) |
| `docker compose up airflow-init` exit 1 | airflow_meta DB 미생성 / Postgres 미가동 | `postgres-airflow-bootstrap` service 가 먼저 가동 (depends_on) 확인 | docker-compose.yml |
| Airflow webserver 가 8080 미응답 | webserver init 진행 중 (~30초) | `until curl -sf http://localhost:8080/health; do sleep 5; done` | 본 runbook §평소 기동 |
| `dbt run` BashOperator 가 `dbt: command not found` | dbt 가 별도 venv (`/opt/airflow/dbt-venv`) 인데 PATH 미지정 | `${DBT_VENV_BIN}/dbt run ...` 또는 `/opt/airflow/dbt-venv/bin/dbt run ...` | [archive §5](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#5-이슈-4--dbt-core-protobuf-vs-airflow-protobuf-양립-불가-task-55) |
| `cannot call partial() on task context variable 'params'` | TaskFlow API 의 인자명이 Airflow reserved | 변수명 `cfg` 등으로 변경 | [archive §7](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md#7-이슈-6--airflow-reserved-keyword-params-충돌-task-57) |
| 메모리 사용량 80% (19.2GB) 초과 | Airflow + Spark 동시 가동 | `docker compose stop airflow-scheduler` (Day 9 정책). DAG 야간 실행 (02:00 / 03:00 KST) 으로 streaming peak 회피 | spec §9-3 |

---

## Day 9 Spark 본격 진입 시 점검

- `iceberg_maintenance` DAG 의 BashOperator → SparkSubmitOperator 교체.
- `backfill_silver_from_bronze` 의 spark_submit helper 본문 (`spark/jobs/backfill_silver_partition.py`) — 멱등 MERGE INTO + dedup_key (spec §10 의 레시핑 미해결 closure 패턴 일관).
- `airflow-scheduler` 일시 stop 정책 활성 (Spark 기동 직전).
- DAG `unpause` (`airflow dags unpause iceberg_maintenance`).

---

## Day 10 `slo_daily_report` 추가 시 점검

- BranchPythonOperator + SLO 분기 (P95 < 7분 vs 위반).
- lib/duckdb_iceberg 가 4번째 사용처 (Day 5 dbt = 3번째).
- DAG `schedule` 활성 + 야간 실행 정책.

---

## 관련 문서

- [troubleshooting `2026-05-10-day-5-airflow-setup.md`](../portfolio/troubleshooting/2026-05-10-day-5-airflow-setup.md) — Task 5.5 + 5.7 트러블슈팅 본문
- [Day 5 dbt runbook](./day5_dbt.md) — dbt 영역 운영 절차
- [PR #30](https://github.com/benidjor/seoul-citydata-platform/pull/30) — Task 5.5~5.6
- [PR #31](https://github.com/benidjor/seoul-citydata-platform/pull/31) — Task 5.7~5.8
- [airflow-decision 메모리](https://github.com/benidjor/seoul-citydata-platform) — 본진 4 DAG 도입 결정
