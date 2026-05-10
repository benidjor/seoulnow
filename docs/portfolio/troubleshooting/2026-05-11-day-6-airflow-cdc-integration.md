# Day 6 — Airflow ↔ Iceberg ↔ CDC 통합 트러블슈팅

> 작성: 2026-05-11
> 영역: Day 6 진입 전 hotfix 5건 + Day 6 본 작업 (Task 6.1~6.4) 의 부수 fix + 운영 발견 3건
> 관련 PR: #33 (α0), #34 (α1), #35 (α2), #36 (α Task 6.1+6.2), #37 (α3), #38 (β Task 6.3+6.4 + lint + deviation D), #39 (γ)
> 운영 runbook: [`day6_cdc.md`](../../runbook/day6_cdc.md) (정상 경로) + [`day1_infra.md`](../../runbook/day1_infra.md) (Lakekeeper BASE_URI 진단 표)

## 0. 진입 흐름 요약

Phase 1A Week 1 종료 (PR #32) + Day 6 entry plan 결정 후 진입 직전 검증 단계 = `dbt_full_run` manual trigger 1회. 이 trigger 1건이 Day 5 PR #29 (dbt) ↔ PR #30 (Airflow) 통합 미완 5단계를 연쇄 식별. 각각 별도 hotfix PR 으로 root cause closure.

| 단계 | PR | 발견 | 해결 |
|---|---|---|---|
| 0 | (없음) | manual trigger 1회 시도 | `dbt_full_run` scheduled run 자동 trigger + manual run queue 대기 |
| α0 | #33 | `ModuleNotFoundError: No module named 'flink_jobs'` | src mount + dbt-venv 의존성 + PYTHONPATH + docker DNS env override |
| α1 | #34 | `ConnectionRefusedError(host='localhost', port=8181)` (env override 적용 후에도 재발) | BashOperator `append_env=True` inherit 결손 → `dbt_env` dict 명시 set |
| α2 | #35 | env transmission OK 확인했으나 여전히 `localhost:8181` connect | Lakekeeper REST `/v1/config` 의 `overrides.uri` 가 client kwargs 강제 override (Iceberg REST spec) |
| α3 | #37 | `image not found "debezium/connect:2.7"` | Debezium tagging convention `<major>.<minor>.<patch>.Final` (`2.7.3.Final`) |
| β | #38 | (Task 6.1~6.4 본 작업 + 부수 fix 2건) | dim_place python model 변환 (deviation D), CI lint UP017/F401 |
| γ | #39 | `Object identifier must consist of 1 to 3 parts` | `register_iceberg_catalog` 가 flat database 등록 → 3-part `ice.silver.dim_place` |

## 1. Issue 1 — Airflow dbt-venv 가 `flink_jobs` / `platform_common` 통합 안 됨 (PR #33 α0)

### 증상

`docker compose exec scp-airflow-scheduler airflow dags trigger dbt_full_run` 직후 첫 task `staging.dbt_run_staging` 가 10초 만에 fail. log:

```
[subprocess.py:106] INFO -   ModuleNotFoundError: No module named 'flink_jobs'
[subprocess.py:106] INFO -     File "/tmp/tmp38tf_i7y.py", line 23, in model
[subprocess.py:106] INFO -       from flink_jobs.lib.duckdb_iceberg import (
[subprocess.py:106] INFO -           build_catalog,
[subprocess.py:106] INFO -           configure_duckdb,
[subprocess.py:106] INFO -           table_paths,
[subprocess.py:106] INFO -       )
```

### 원인

Day 5 의 dbt python model `stg_hotspot_silver.py` 가 `flink_jobs.lib.duckdb_iceberg` (그 안에서 다시 `platform_common.config.Settings`) 를 import. 호스트 (uv) 환경에서는 `pyproject.toml` 의 `[tool.hatch.build.targets.wheel] packages = ["src/platform_common", "src/producers", "src/flink_jobs"]` + `[tool.uv] package = true` 로 자동 install + path 등록되지만 **Airflow 컨테이너 dbt-venv 는 `dbt-core`, `dbt-duckdb` 만 설치되어 있고 src 도 mount 안 됨**.

통합 미완 4건 동시 식별:

1. `./src/` volume mount 누락 — airflow services 의 mount = `dags/`, `plugins/`, `logs/`, `dbt/` 만
2. dbt-venv 의존성 부족 — `pyiceberg`, `pydantic-settings`, `pydantic` 모두 부재
3. `PYTHONPATH` 미설정 — dbt subprocess 가 mount 된 src 를 sys.path 에 못 추가
4. docker DNS 미설정 — `platform_common.Settings` 의 default `lakekeeper_url=http://localhost:8181`, `minio_endpoint=http://localhost:9000` 이 컨테이너 안에서 self loopback 라 무효

### 해결

`docker-compose.yml` 의 `x-airflow-common` anchor 1곳 변경 → init/webserver/scheduler 3 service 일관 전파:

```yaml
volumes:
  - ./src:/opt/airflow/repo-src:ro
environment:
  LAKEKEEPER_URL: http://lakekeeper:8181
  MINIO_ENDPOINT: http://minio:9000
```

`airflow/dbt-requirements.txt` 에 `pyiceberg[duckdb,s3fs]`, `pydantic-settings`, `pydantic` 추가. `airflow/dags/dbt_full_run.py` 의 `dbt_env` 에 `PYTHONPATH=/opt/airflow/repo-src` 추가.

검증 명령:

```bash
docker compose exec -T airflow-scheduler ls /opt/airflow/repo-src/   # flink_jobs / platform_common / producers
docker compose exec -T airflow-scheduler env | grep -E "LAKEKEEPER|MINIO_ENDPOINT"
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -c \
  "import pyiceberg, pydantic_settings; print('OK', pyiceberg.__version__)"
```

## 2. Issue 2 — `BashOperator(env=dict, append_env=True)` 의 inherit 결손 (PR #34 α1)

### 증상

α0 적용 후 retry attempt 3 에서 같은 task 가 9초만에 또 fail. log 의 stack trace:

```
urllib3.exceptions.NewConnectionError: HTTPConnection(host='localhost', port=8181):
  Failed to establish a new connection: [Errno 111] Connection refused
```

### 진단

scheduler 컨테이너 env 는 정상 (`LAKEKEEPER_URL=http://lakekeeper:8181`). 컨테이너 내 직접 호출 (`/opt/airflow/dbt-venv/bin/python -c "from platform_common import get_settings; print(get_settings().lakekeeper_url)"`) 도 정상 (`http://lakekeeper:8181`). 그러나 **dbt subprocess 는 여전히 `localhost:8181` 시도** — env transmission 이 dict 명시 set 키는 통과 (`PYTHONPATH` 로 module import 정상), inherit 의존 키는 결손.

### 원인

Airflow `BashOperator(env=<dict>, append_env=True)` 는 docs 상 `os.environ + env` merge 가 정상이지만, **dbt-duckdb 가 spawn 한 python model subprocess 까지 inherit 안 됨**. 정확한 mechanism 은 단정 불가하나 결정적 증거 = 같은 dict 내 `PYTHONPATH` 는 transmitted, dict 외 env (`LAKEKEEPER_URL`) 는 untransmitted.

### 해결

`dbt_env` dict 에 명시 set:

```python
dbt_env = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "PYTHONPATH": "/opt/airflow/repo-src",
    "LAKEKEEPER_URL": "http://lakekeeper:8181",
    "MINIO_ENDPOINT": "http://minio:9000",
}
```

docker-compose 의 `x-airflow-common.environment` 는 그대로 유지 (defense in depth — 다른 DAG 가 평소 inherit 정상 작동 케이스 cover).

### 향후 패턴

다른 DAG (`iceberg_maintenance`, `backfill_silver_from_bronze`, `slo_daily_report`) 도 BashOperator 가 호스트 env 를 의존하면 같은 패턴 적용. **dict 명시 set 을 default 로 잡고, 호스트 env 는 fallback 으로만**.

## 3. Issue 3 — Lakekeeper REST `overrides.uri` 가 client kwargs 강제 override (PR #35 α2, root cause)

### 증상

α1 적용 후 attempt 4 에서 또 같은 connection refused. **client kwargs 의 `uri=http://lakekeeper:8181/catalog` 를 명시했음에도** 실제 connect 시도는 `localhost:8181`.

### 진단

컨테이너 안 직접 호출:

```bash
docker compose exec -T airflow-scheduler curl -s "http://lakekeeper:8181/catalog/v1/config?warehouse=seoul"
```

응답:

```json
{"overrides":{"idempotency-key-lifetime":"PT30M","uri":"http://localhost:8181/catalog"}, "defaults":{...}}
```

**Iceberg REST spec 의 `/v1/config` 응답 처리 규칙**: server 가 vend 한 `overrides` 는 client kwargs 를 **강제 덮어씀** (defaults < client kwargs < overrides). 즉 client 가 `uri=http://lakekeeper:8181/catalog` 를 보냈지만 server vend 의 `overrides.uri=http://localhost:8181/catalog` 가 final config.

### 원인

Day 1 setup 시 `docker-compose.yml` 의 `lakekeeper.environment.LAKEKEEPER__BASE_URI=http://localhost:8181` 가 host-friendly 로 set. host 측 client (PyFlink, dbt host run, slo_metrics) 는 host port mapping `8181:8181` 덕분에 우연히 동작. **컨테이너 client 첫 진입 = Day 5 dbt python model 시점에 결손 발견**.

### 해결

`LAKEKEEPER__BASE_URI=http://lakekeeper:8181` (docker hostname 통일) + 호스트 `/etc/hosts` 에 `127.0.0.1 lakekeeper minio` alias 추가.

흐름:

- 컨테이너 client → docker DNS resolve `lakekeeper` → OK
- host client → `/etc/hosts` alias → `127.0.0.1` → port mapping `8181:8181` → lakekeeper container OK

`.env.example` / `README.md` Quick Start 의 0번 step / `day1_infra.md` 트러블슈팅 표에 같은 진단 cross-link.

### 학습

- **server-vended config 가 client kwargs 를 덮어쓰는 spec 은 production-style infra 에서 흔한 pitfall** — Lakekeeper / Polaris / Tabular 같은 REST catalog 모두 같은 패턴.
- **localhost vs docker hostname 결정은 Day 1 setup 시점에 future client 까지 고려**. 호스트 측 client 만 cover 하면 항상 후속 hotfix 발생.
- 회피 patterns:
  - host /etc/hosts alias (본 PR 채택 — 가장 단순)
  - reverse proxy (nginx 컨테이너 등 — 복잡)
  - dual hostname `extra_hosts: ["localhost:host-gateway"]` 시도했으나 default `127.0.0.1 localhost` 와 우선순위 충돌로 비추

## 4. Issue 4 — `debezium/connect:2.7` image tag 형식 불일치 (PR #37 α3)

### 증상

PR #36 (Task 6.1~6.2) 머지 후 `docker compose up -d` 시:

```
Error response from daemon: failed to resolve reference "docker.io/debezium/connect:2.7":
  docker.io/debezium/connect:2.7: not found
```

### 진단

docker hub registry API 로 사용 가능한 tag 확인:

```bash
curl -s "https://registry.hub.docker.com/v2/repositories/debezium/connect/tags/?page_size=20"
```

→ `2.7.3.Final`, `2.7.2.Final`, `2.7.1.Final`, `2.7.0.Final`, `3.0.0.Final` 등 **`<major>.<minor>.<patch>.Final` 형식**. 단순 `2.7` tag 는 존재 안 함 (2.6 / 2.5 같은 major.minor alias 는 일부 존재).

### 해결

`debezium/connect:2.7` → `debezium/connect:2.7.3.Final` (2.7 series 의 최신 안정 patch).

### 학습

- **Confluent / Bitnami / Apache 와 다른 tagging convention 가진 vendor 는 docker hub 직접 확인이 안전**. plan 작성 시 Week 2 plan 의 `debezium/connect:2.7` 표기를 검증 없이 docker-compose 로 옮긴 게 root cause.

## 5. Issue 5 — dbt-duckdb adapter 의 Iceberg source 자동 read 미지원 (PR #38 deviation D)

### 증상

PR #38 의 `dim_place.sql` 은 plan 본문 그대로 `{{ source('silver', 'dim_place') }}` SQL view 로 작성. dbt parse 통과. 그러나 Day 5 `stg_hotspot_silver.py` 가 python model + `flink_jobs.lib.duckdb_iceberg` 우회 패턴을 쓴 이유와 정확히 같은 문제 — dbt-duckdb adapter 는 Lakekeeper Iceberg source 의 `external_location` 을 자동 read 못함.

### 진단

PR β subagent 가 머지 직전 사용자 검증 단계에서 fail 가능성을 보고. 사실상 dbt run 단계에서 `Catalog 'silver' not found` 또는 `Table 'dim_place' not found` 류 에러 예상.

### 해결

같은 brunch 에 follow-up commit (`0fcfbe7`) — `dim_place.sql` 삭제 + `dim_place.py` 신규 (Day 5 `stg_hotspot_silver.py` 와 동일 패턴):

```python
def model(dbt, session):
    dbt.config(materialized="table", schema="gold")
    from flink_jobs.lib.duckdb_iceberg import build_catalog, configure_duckdb, table_paths
    catalog = build_catalog()
    file_paths = table_paths(catalog, "silver.dim_place")
    if not file_paths:
        return session.sql("SELECT CAST(NULL AS BIGINT) AS place_id, ... WHERE 1=0")
    configure_duckdb(session)
    return session.sql(
        f"WITH ranked AS (SELECT *, row_number() OVER (PARTITION BY place_id ORDER BY valid_from DESC) AS rn "
        f"FROM read_parquet({file_paths!r}, hive_partitioning=true)) "
        f"SELECT ... FROM ranked WHERE rn=1 AND cdc_op<>'d' AND status='active'"
    )
```

부수 발견 — plan 본문의 docstring 안 jinja syntax `{{ source('silver', 'dim_place') }}` 가 dbt python model parser 에서 `"No jinja in python model code is allowed"` 로 reject. plain text 로 풀어쓰기 필요.

### 학습

- **dbt-duckdb 의 source 자동 read 가능 여부 = adapter 별 다름**. duckdb adapter 는 Lakekeeper / Iceberg source 자동 read 불가. python model + lib 우회 패턴이 정공.
- **Day 5 stg_hotspot_silver.py 의 패턴이 future dbt python model 의 template** — `dim_place.py` 도 동일 구조 재사용.
- **dbt python model 의 docstring 안 jinja syntax 도 parser 가 reject** — plain text 로 풀어쓰기.

## 6. Issue 6 — Flink Table API identifier 4-part 위반 (PR #39 γ)

### 증상

PR #38 머지 후 검증 6단계 진행 중 Flink job 가동 시:

```
py4j.protocol.Py4JJavaError: An error occurred while calling o8.executeSql.
: org.apache.flink.table.api.ValidationException: Object identifier must consist of 1 to 3 parts.
```

### 진단

`cdc_to_dim_place.py` 의 identifier:

```python
cat = warehouse_namespace()  # = "seoul"
t_env.execute_sql(f"CREATE TABLE IF NOT EXISTS ice.{cat}.silver.dim_place (...)")
```

→ `ice.seoul.silver.dim_place` = **4 parts**. Flink Table API 의 identifier max = 3 parts (`catalog.database.table`).

`bronze_to_silver.py` (`ice.silver.hotspot_congestion`) / `silver_to_gold.py` (`ice.gold.fact_hotspot_congestion_5min`, 주석에 4-part 금지 명시) 는 이미 3-part 사용 중. PR β 의 `cdc_to_dim_place.py` 만 잘못 작성.

### 해결

```python
t_env.execute_sql("CREATE TABLE IF NOT EXISTS ice.silver.dim_place (...)")
```

`warehouse_namespace` import + 호출 + f-string 도 동시 정리 (변수 없으므로 일반 string).

### 학습

- **`register_iceberg_catalog` 의 catalog alias `ice` 안에서 `bronze` / `silver` / `gold` 가 flat database** — Iceberg namespace tree 와 다름. 4-part `ice.<warehouse>.<schema>.<table>` 형식은 Flink 가 항상 거부.
- **`silver_to_gold.py` 의 docstring 주석이 future drift 방지의 single source** — 새 streaming job 작성 시 같은 docstring 인용 + 3-part identifier 의무.

## 7. Issue 7 — ruff lint UP017 + F401 (PR #38 의 추가 commit)

### 증상

PR #38 의 GitHub Actions CI / Python lint + unit test job 에서 ruff check 3건 fail:

- `UP017`: `src/flink_jobs/lib/scd2.py:81` + `tests/unit/test_scd2.py:61` — `timezone.utc` 대신 `datetime.UTC` alias 사용 권장
- `F401`: `tests/unit/test_scd2.py:8` — `pytest` import 안 쓰임

### 진단

Python 3.11+ 에서 `datetime.UTC` alias 추가됨. 본 프로젝트 `requires-python = ">=3.11"` 라 사용 가능. plan 본문 그대로 따라간 코드가 3.10 호환 형식 (`timezone.utc`) 을 사용한 게 원인.

### 해결

`from datetime import UTC, datetime` + `tz=UTC` / `tzinfo=UTC` 로 변경. 미사용 `pytest` import 삭제. 같은 brunch 에 commit 추가.

### 학습

- **plan 본문의 코드도 ruff lint 통과를 보장하지 않음**. PR 작성 시 implementer subagent 가 ruff check 까지 sanity 단계로 넣어야 회귀 0.
- **`datetime.UTC` alias 는 Python 3.11+ 만** — 3.10 호환 필요한 라이브러리는 `timezone.utc` 유지.

## 8. Issue 8 — plan deviation C: ts_ms epoch 변환 hand-calc 오기

### 증상

PR β implementer subagent 가 TDD 진행 중 pytest RED 단계에서 발견:

```python
# plan 본문
assert row.valid_from == datetime(2024, 4, 30, 12, 33, 20, tzinfo=timezone.utc).replace(tzinfo=None)
```

`ts_ms = 1714490000000` 의 정확한 변환은 **2024-04-30 15:13:20 UTC**. plan 본문은 `12:33:20` 으로 오기.

### 진단

plan 작성 시 hand-calc 로 변환한 값이 잘못. Day 4 Task 4.2 의 SLO percentile (414 → 393) 와 동일한 패턴.

### 해결

test 파일 + plan 본문 모두 정정 (`15, 13, 20`).

### 학습

- **plan 의 hand-calc 수치는 모두 cross-check 필요** — Day 4 (414→393) + Day 6 (12:33:20→15:13:20) 두 사례. percentile / epoch / aggregate 경계 / interval 모두 의심 대상.
- **TDD 의 RED 단계가 plan 검증 자체** — 코드가 plan 대로 작성됐을 때 RED 가 plan 가정을 falsify.

## 9. 부수 발견 — 운영 측면

### 9-1. Airflow `catchup=False` 의 unpause 자동 trigger

dbt_full_run DAG 가 unpause 직후 last interval (data_interval_start=2026-05-09 02:00:00, end=2026-05-10 02:00:00) 의 scheduled run 1회 자동 trigger. `catchup=False` 인데도 발생.

이는 Airflow known behavior — `catchup=False` 는 backfill 들 (last interval 외 과거 interval) 만 skip, **last interval 1회는 unpause 시 자동 trigger**. `start_date=datetime(2026, 5, 1)` 부터 unpause 시점까지의 interval 이 여러 개 있어도 last 1개만.

manual trigger 는 `max_active_runs=1` 때문에 scheduled run 끝까지 queue 대기.

회피 — DAG paused 상태에서 manual trigger 후 다른 trigger 안 도는지 확인. 또는 start_date 를 unpause 직전으로 설정.

### 9-2. `max_active_runs=1` 의 manual run queue

scheduled run + manual run 동시 trigger 시 manual run state = `queued`. scheduled run terminal (success/failed) 후 자동 시작. retry 들이 도는 동안 manual run 진행 안 됨 → 검증 시 시간 손실.

회피 — retry timer 가 길면 (`retry_exponential_backoff=True` + 5min/10min/20min) `airflow tasks clear` 로 task instance reset → 즉시 재시도. scheduled run 과 manual run 모두 한 번에 검증 가능.

### 9-3. Debezium 2.7 의 `VALUE_CONVERTER_SCHEMAS_ENABLE=false` 효력 안 남

worker-level + connector-level 모두 set 했음에도 토픽 message 가 `{schema, payload}` wrapping 으로 발행. PR β 의 PyFlink source DDL 을 wrapping 가정으로 작성:

```sql
CREATE TEMPORARY TABLE place_cdc_src (
  `payload` ROW<
    `op` STRING,
    ts_ms BIGINT,
    `before` ROW<...>,
    `after` ROW<...>
  >
)
```

INSERT 문도 `payload.op`, `payload.before.*`, `payload.after.*` 로 unwrap.

미해결 — Debezium 2.7.x 의 known behavior 인지, 본 프로젝트 setup 의 다른 설정 충돌인지 미진단. wrapping 처리로 우회.

## 10. 학습 패턴 5종

### 10-1. manual trigger 1회 = 통합 검증의 single source

Day 5 의 dbt PR #29 + Airflow PR #30 가 분리 머지된 상태에서 통합 검증을 안 함. Day 6 entry plan 의 "잔여 deviation #1 = manual trigger 1회" 가 정확히 이 검증을 담당. 1회 trigger 가 5단계 hotfix 를 연쇄 식별. **PR 분리 시 마지막에 통합 검증 1회 의무**.

### 10-2. 환경 변경 hotfix 는 BASE_URI / mount / env override 까지 root cause 추적

Issue 1 → 2 → 3 의 흐름은 표면 → 통신 결손 → server-side spec 충돌 까지 3단계 진단. 표면 fix 만 적용했으면 attempt 4 가 또 fail. **REST catalog 의 server-vended config 처럼 spec 차원 issue 는 docs 정독 + curl 직접 호출로 vend response 확인 필수**.

### 10-3. plan 본문의 코드는 검증 안 된 가정

- hand-calc 수치 (Day 4 414→393, Day 6 12:33:20→15:13:20)
- image tag 형식 (`debezium/connect:2.7` → docker hub 미존재)
- ruff lint 통과 여부 (UP017/F401)
- Flink identifier 형식 (4-part vs 3-part)

implementer subagent 의 sanity 단계에 ruff / pytest / docker compose config / `python -c "import"` 를 모두 넣어서 plan 가정 falsify.

### 10-4. dbt-duckdb 의 Iceberg source 패턴 정착

Day 5 `stg_hotspot_silver.py` (Lakekeeper UUID-prefix path 학습) → Day 6 `dim_place.py` (deviation D follow-up) 로 패턴 재사용. 모든 dbt python model 의 source read 는 `flink_jobs.lib.duckdb_iceberg.{build_catalog, configure_duckdb, table_paths}` + `read_parquet(paths, hive_partitioning=true)` 우회.

### 10-5. troubleshooting archive 의 진단 단계 기록 = future 자산

Day 4 Task 1 (silver fix) 의 7단계 진단 archive → Day 6 Issue 3 의 직접 inspiration. 환경 fix 시도 → 실측 → root cause → 학습 흐름 보존이 future 디버깅 시간 절감.

## 11. 관련 문서

- 운영 runbook: [`day6_cdc.md`](../../runbook/day6_cdc.md) — Day 6 Task 6.1~6.4 정상 경로 + fallback
- Day 1 인프라 진단: [`day1_infra.md`](../../runbook/day1_infra.md) — 트러블슈팅 표에 Lakekeeper BASE_URI 행 1건 추가
- spec §5-8 (Airflow 본진 4 DAG) / §6-1 Day 6 / §9-1 Day 6 fallback
- Phase 1A Week 2 plan §Day 6 Task 6.1~6.4
- 직전 Day 의 환경 학습:
  - `2026-05-09-day-4-tasks-4_1-4_3.md` — Lakekeeper UUID-prefix path / pyiceberg `plan_files()` 우회 패턴 단일 출처
  - `2026-05-10-day-5-airflow-setup.md` — Airflow LocalExecutor + dbt-venv 분리 + 메모리 mitigation
  - `2026-05-10-day-5-dbt-iceberg-compat.md` — `stg_hotspot_silver.py` python model 결정
