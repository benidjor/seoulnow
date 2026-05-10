# Day 5 Airflow 셋업 트러블슈팅

> 작성: 2026-05-10
> 영역: Day 5 Task 5.5 (Airflow LocalExecutor 셋업) + Task 5.7 (backfill_silver_from_bronze DAG)
> 관련 PR: #30 (Day 5 Task 5.5~5.6) + #31 (Day 5 Task 5.7~5.8)

## 1. 배경

Day 5 PR γ (#30) 의 Airflow 셋업 — apache-airflow:2.10.5 + LocalExecutor + 본 프로젝트 의존성 (dbt-core / pyiceberg / duckdb / boto3) install 시도. plan §5-8 의 "LocalExecutor + SQLite metadata" 와 "단일 image 안 의존성" 가정이 reality 와 다수 충돌 → 5건 fix.

추가로 PR δ (#31) 의 Task 5.7 backfill DAG 진입 시 6번째 이슈 (Airflow reserved keyword 충돌) 발견 + 해결.

본 archive 는 6 이슈 묶음 — Airflow 셋업 + DAG 작성 영역의 spec / reality gap.

---

## 2. 이슈 1 — SQLite metadata + LocalExecutor lock 충돌 (Task 5.5)

### 2.1. 증상

plan §5-8 명시: "LocalExecutor + SQLite metadata DB → ~700MB. Postgres meta / Celery / Redis 미사용".

Airflow 2.10 docs 검토 결과:
> SQLite is only intended for development purposes ... LocalExecutor + SQLite is not supported because LocalExecutor uses multiple worker processes that conflict on SQLite write lock.

즉 plan 의 조합이 Airflow 2.10 의 실제 동작과 양립 불가.

### 2.2. 해결 — 기존 scp-postgres 안 airflow_meta DB

3 옵션:

| 옵션 | 메모리 영향 | plan 의도 보존 | 결정 |
|---|---|---|---|
| A. SequentialExecutor + SQLite | minimal | ◎ (plan 그대로) | ✗ Day 5~6 buffer Task 5.7 dynamic mapping 동시 실행 불가 |
| B. LocalExecutor + 별도 Postgres container | +200MB | ✗ "Postgres meta 미사용" 위반 | ✗ |
| **C. LocalExecutor + 기존 scp-postgres 안 airflow_meta DB** | +30MB (DB 만 추가) | ○ (별도 container 안 띄움) | **채택** |

C 채택 — plan 의 "별도 container 추가 회피" 의도 보존 + production-ready + 5.7 dynamic mapping 동시 실행 OK.

`docker-compose.yml` 갱신:

```yaml
postgres-airflow-bootstrap:
  image: postgres:16
  depends_on:
    postgres: { condition: service_healthy }
  environment:
    PGPASSWORD: scp_dev_password
  entrypoint: >
    /bin/sh -c "
    psql -h postgres -U scp -d scp -tc \"SELECT 1 FROM pg_database WHERE datname='airflow_meta'\" | grep -q 1 ||
    psql -h postgres -U scp -d scp -c 'CREATE DATABASE airflow_meta'
    "
  restart: "no"
```

`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`:
```
postgresql+psycopg2://scp:scp_dev_password@postgres:5432/airflow_meta
```

---

## 3. 이슈 2 — pip ResolutionTooDeep (Task 5.5)

### 3.1. 증상

`docker compose build airflow-init` 시:

```
File ".../pip/_vendor/resolvelib/resolvers.py", line 457, in resolve
    raise ResolutionTooDeep(max_rounds)
pip._vendor.resolvelib.resolvers.ResolutionTooDeep: 200000
```

pip resolver 가 backtracking 200000 round 후 give up. dbt-core / pyiceberg / Airflow transitive dependencies 가 너무 깊어 발생.

### 3.2. 해결 — uv resolver 채택

pip → `uv pip install` 교체 + Airflow constraints URL 적용:

```dockerfile
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

USER root
RUN uv pip install --no-cache-dir --system \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /opt/airflow/requirements.txt
```

결과:
```
Resolved 159 packages in 2.41s
Installed 159 packages in 112ms
```

uv 의 PubGrub 기반 resolver 가 pip 의 backtracking 보다 효율적.

---

## 4. 이슈 3 — Airflow constraints vs requirements 의 postgres provider 버전 충돌 (Task 5.5)

### 4.1. 증상

`uv pip install` 결과:

```
× No solution found when resolving dependencies:
╰─▶ Because you require apache-airflow-providers-postgres>=5.7,<6.0 and
    apache-airflow-providers-postgres==6.0.0, we can conclude that your
    requirements are unsatisfiable.
```

Airflow 2.10.5 constraints URL 안에 `apache-airflow-providers-postgres==6.0.0` 이 lock 됐음. requirements 의 `<6.0` 과 양립 불가.

### 4.2. 해결 — 상한 제거

`requirements.txt`:
```
# 상한 안 박음 — Airflow 2.10.5 constraints 가 6.0.0 으로 lock (constraints 우선).
apache-airflow-providers-postgres>=5.7
```

constraints 가 자동으로 6.0.0 채택. provider major 버전 변경 영향은 Airflow constraints 가 보증.

---

## 5. 이슈 4 — dbt-core protobuf vs Airflow protobuf 양립 불가 (Task 5.5)

### 5.1. 증상

`uv pip install` 결과 (constraints 적용 후):

```
× No solution found when resolving dependencies:
╰─▶ Because dbt-core>=1.9.9,<=1.9.10 depends on protobuf>=6.0,<7.0 and
    protobuf==4.25.6, we can conclude that dbt-core>=1.9.9,<=1.9.10 cannot be used.
    ...
    dbt-core>=1.9.0,<1.10 cannot be used.
    And because you require dbt-core>=1.9,<1.10, we can conclude that your
    requirements are unsatisfiable.
```

- Airflow 2.10.5 constraints: `protobuf==4.25.6` lock
- dbt-core 1.9.9~1.9.10: `protobuf>=6.0,<7.0` 요구
- dbt-core 1.9.0~1.9.8: 다른 transitive 충돌 (`(1)` 표기)

→ dbt-core 1.9 전체 범위가 Airflow 2.10 venv 와 양립 불가.

### 5.2. 해결 — dbt 별도 venv 격리

3 옵션:

| 옵션 | 격리 | 시도 비용 | 위험 |
|---|---|---|---|
| A. dbt-core 1.8 (낮은 protobuf 호환) | ✗ | 낮음 | dbt 1.9+ 기능 손실 + host venv (1.9) 와 불일치 |
| B. protobuf override (constraints 무시) | ✗ | 낮음 | Airflow runtime 미검증 영역 |
| **C. dbt 만 별도 venv** | ◎ | 중간 | venv 2개 관리 부담 (단 명시적) |

C 채택. Dockerfile:

```dockerfile
USER airflow

# dbt 전용 venv — airflow user 에 write 권한 있는 /opt/airflow 안.
RUN uv venv /opt/airflow/dbt-venv --python 3.11 && \
    uv pip install --no-cache-dir --python /opt/airflow/dbt-venv/bin/python \
    -r /opt/airflow/dbt-requirements.txt

ENV DBT_VENV_BIN=/opt/airflow/dbt-venv/bin
```

DAG 의 BashOperator 가 `${DBT_VENV_BIN}/dbt` 호출:

```python
DBT_BIN = "/opt/airflow/dbt-venv/bin/dbt"
BashOperator(
    task_id="dbt_run_staging",
    bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --select staging",
)
```

검증:
```
$ docker compose exec airflow-scheduler /opt/airflow/dbt-venv/bin/dbt --version
Core: dbt-core 1.9.10
Plugins: duckdb 1.9.6
```

---

## 6. 이슈 5 — system venv permission denied (Task 5.5)

### 6.1. 증상

USER airflow 로 실행한 `uv pip install --system` 시:

```
error: Failed to install: opentelemetry_sdk-1.27.0-py3-none-any.whl
  Caused by: failed to create directory `/usr/local/lib/python3.11/site-packages/...`:
  Permission denied (os error 13)
```

Airflow base image 의 system venv (`/usr/local/lib/python3.11/site-packages`) 는 root 소유. airflow user 는 write 권한 없음.

### 6.2. 해결 — USER root 분리 + USER airflow 복귀

Dockerfile 의 USER 단계 분리:

```dockerfile
FROM apache/airflow:2.10.5-python3.11
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

# Airflow system venv install — root 권한 필요
USER root
RUN uv pip install --no-cache-dir --system --constraint "..." -r requirements.txt

# dbt 전용 venv — airflow user 에 write 권한 있는 /opt/airflow 안
USER airflow
RUN uv venv /opt/airflow/dbt-venv --python 3.11 && \
    uv pip install --python /opt/airflow/dbt-venv/bin/python -r dbt-requirements.txt
```

Airflow 의 표준 user 가 `airflow` 라 마지막에 USER airflow 복귀. dbt-venv 는 `/opt/airflow/dbt-venv` (airflow user write 가능) 안.

---

## 7. 이슈 6 — Airflow reserved keyword `params` 충돌 (Task 5.7)

### 7.1. 증상

Task 5.7 의 `backfill_silver_from_bronze.py` 작성 후 DAG load:

```
ValueError: cannot call partial() on task context variable 'params'
```

DAG file 자체가 import 단계 fail → `airflow dags list-import-errors` 에 등록.

### 7.2. 원인

Airflow 2.x 의 task context reserved 키워드 `params` 와 충돌. TaskFlow API 의 `partial()` 호출이 reserved 키워드를 인자로 받지 못함:

```python
# 충돌 코드
@task
def process_partition(partition: str, params: dict):
    ...

processed = process_partition.partial(params=params).expand(...)
#                                     ^^^^^^^^^^^^^ ValueError
```

Airflow 의 reserved 키워드 (task context 안 자동 주입):
- `dag` / `task` / `ti` / `task_instance`
- `params` (DAG-level Params 객체)
- `run_id` / `execution_date` / `logical_date`
- `data_interval_start` / `data_interval_end`

### 7.3. 해결 — 변수명 `cfg` 일관 변경

Reserved 키워드 회피:

```python
@task
def process_partition(partition: str, cfg: dict):
    for table in cfg["tables"]:
        ...

cfg = validate_params()
partitions = generate_hourly_partitions(cfg)
processed = process_partition.partial(cfg=cfg).expand(partition=partitions)
```

검증:
```
$ airflow dags list-import-errors  → No data found
$ airflow tasks list backfill_silver_from_bronze --tree
validate_params
└─ generate_hourly_partitions
   └─ Mapped: process_partition         ← Dynamic Task Mapping 정상
      └─ verify_silver_row_count
         └─ post_backfill_summary
```

### 7.4. 학습

향후 TaskFlow API 작성 시 reserved keyword 회피 패턴:
- 함수 인자명 권장 — `cfg` / `config` / `payload` / `data` / `args` (도메인 의미 명시)
- 함수 인자명 회피 — 위 reserved 7종 + Airflow context 의 다른 키 (`var` / `conn` / `macros` / `prev_*`)

DAG load fail 시 `airflow dags list-import-errors` 가 정확한 line 표시 — 진단 첫 단계.

---

## 8. 영향 / 다음 단계

### 8.1. Day 6 CDC 진입 시 검토

- Postgres CDC (Debezium) 의 Airflow 연동은 sensor / trigger 가 아닌 streaming 영역 → Airflow scope 외. 단 CDC 데이터의 dbt 재처리는 본 PR γ 의 `dbt_full_run` DAG 가 자동 처리 (silver `dim_place` 추가 시 automatic).
- `dim_place` SCD2 의 dbt python model 작성 시 lib/duckdb_iceberg 우회 패턴 재사용 — `2026-05-10-day-5-dbt-iceberg-compat.md` archive 참조.

### 8.2. Day 9 Spark 본격 진입 시

- `airflow-scheduler` 일시 stop 정책 (메모리 mitigation) — `docker compose stop airflow-scheduler` 로 ~600MB 회수.
- `iceberg_maintenance` DAG 의 BashOperator → SparkSubmitOperator 교체. 본격 본문은 Day 9 Task 9.3.
- `backfill_silver_from_bronze` DAG 의 spark_submit helper 도 Day 9 Task 9.2 시점에 본격 (멱등 MERGE INTO + dedup_key).

### 8.3. Discord webhook 활성

본 PR γ / δ 의 `on_failure_callback` 은 `DISCORD_WEBHOOK_URL` env 빈 값 시 stdout fallback. Day 6+ 운영 시점에 `.env` 에 webhook 설정 → 실 발신 활성.

### 8.4. pytest TDD 후속

PR γ Task 5.6 + PR δ Task 5.7~5.8 에서 host venv 의 apache-airflow 미설치로 TDD pytest 미작성. 후속 — Airflow image 안 pytest install + `docker compose exec airflow-scheduler pytest` 패턴 또는 host venv 별도 (apache-airflow only) 마련.

---

## 관련 문서

- [Day 5 Airflow runbook](../../runbook/day5_airflow.md) — 운영 절차 + 메모리 mitigation + DAG 4 운영
- [Day 5 dbt × Iceberg 호환 archive](./2026-05-10-day-5-dbt-iceberg-compat.md) — Task 5.2 우회안 B (관련)
- [PR #30](https://github.com/benidjor/seoul-citydata-platform/pull/30) — Day 5 Task 5.5~5.6 (이슈 1~5)
- [PR #31](https://github.com/benidjor/seoul-citydata-platform/pull/31) — Day 5 Task 5.7~5.8 (이슈 6)
- [airflow-decision 메모리](https://github.com/benidjor/seoul-citydata-platform/wiki) — 본진 4 DAG 도입 결정
