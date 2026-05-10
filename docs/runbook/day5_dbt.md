# Day 5 dbt 운영 runbook

> 작성: 2026-05-10
> 영역: Day 5 Task 5.1~5.4 (dbt-duckdb scaffold + 모델 + tests + GitHub Actions CI)
> 관련 PR: #29

## 평소 운영

### 환경 셋업

```bash
# dbt extras 만 sync (flink 와 conflicts 라 분리)
uv sync --extra dbt

# profiles.yml 첫 사용 시 example 복사 + .env 의 MinIO credentials 일치
cp dbt/seoul/profiles.yml.example dbt/seoul/profiles.yml
# 편집 — s3_endpoint / s3_access_key_id / s3_secret_access_key 를 .env 와 맞춤
```

### dbt 명령

```bash
cd dbt/seoul

# parse / compile (lint, model 인식 검증)
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. --extra dbt dbt parse
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. --extra dbt dbt compile

# run — silver Iceberg → DuckDB staging + mart build
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. --extra dbt dbt run

# test — generic 10 + singular 1 = 11
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. --extra dbt dbt --no-partial-parse test
```

### profiles.yml 의 path 정책

- **`:memory:`** — example 의 default. `dbt run` 종료 시 데이터 사라짐. CI 에서도 in-memory.
- **`target/dbt_seoul.duckdb`** — local 디버깅 시 사용. dbt run 후 별도 process 에서 query 가능. `target/` gitignored 라 자동 정리.

전환 — `dbt/seoul/profiles.yml` 의 `path:` line 만 변경 (gitignored 라 자유).

### CI 검증

GitHub Actions `.github/workflows/ci.yml` 의 dbt job:

```yaml
- name: dbt deps
  working-directory: dbt/seoul
  run: uv run --project ../.. --extra dbt dbt deps

- name: dbt parse
  working-directory: dbt/seoul
  run: uv run --project ../.. --extra dbt dbt parse

- name: dbt compile
  working-directory: dbt/seoul
  run: uv run --project ../.. --extra dbt dbt compile
```

CI 환경엔 MinIO 가 없어 `dbt run` / `dbt test` 는 미실행. 실 데이터 검증은 local 에서 수행 후 PR 본문에 결과 첨부.

---

## 환경 편차 / 정책

### dbt 와 flink extras 의 conflicts (`pyproject.toml`)

```toml
[tool.uv]
conflicts = [
    [
        { extra = "flink" },
        { extra = "dbt" },
    ],
]
```

PyFlink 1.20 transitive `apache-beam 2.48` 의 `protobuf<4.24` 와 dbt-core 1.9 의 protobuf 요구 양립 불가 → 별도 extra 분리. local 에선:

- streaming 작업 (PyFlink) → `uv sync --extra dev --extra flink`
- dbt 작업 → `uv sync --extra dev --extra dbt`

CI 도 job 별 분리 (python job = flink / dbt job = dbt).

### iceberg_scan 우회 패턴 (lib/duckdb_iceberg)

dbt staging 이 silver Iceberg read 시 `iceberg_scan('s3://...')` 직접 호출 회피. 이유 — Lakekeeper REST 가 vending 하는 path 가 UUID-prefix 두 단계 (`019dfe9d-303a-...`) 라 plain path 와 mismatch.

해결 패턴 (`stg_hotspot_silver.py` 참조):

```python
def model(dbt, session):
    dbt.config(materialized="table")
    from flink_jobs.lib.duckdb_iceberg import (
        build_catalog, configure_duckdb, table_paths,
    )
    catalog = build_catalog()
    file_paths = table_paths(catalog, "silver.hotspot_congestion")
    if not file_paths:
        return session.sql("SELECT ... WHERE 1 = 0")  # 빈 schema fallback
    configure_duckdb(session)
    return session.sql(
        f"SELECT ... FROM read_parquet({file_paths!r}, hive_partitioning = true)"
    )
```

bronze / gold 모델 추가 시 동일 패턴 재사용. 상세 — [troubleshooting archive §2](../portfolio/troubleshooting/2026-05-10-day-5-dbt-iceberg-compat.md#2-이슈-1--iceberg_scanplain-path-uuid-prefix-mismatch-task-52).

### SQL comment 안 jinja 표기 회피

dbt parser 가 SQL `--` / `/* */` comment 안의 `{{ ... }}` 도 dependency 로 해석. 본 프로젝트 회피 패턴:

```sql
-- 권장 — 평문 (괄호 표기 등)
-- plan 의 source 호출 (silver.hotspot_congestion) 은 sources.yml 제거 변경
-- 으로 ref 호출 (stg_hotspot_silver) 로 변경.

-- 금지 — jinja 표기 (dbt parser 가 dependency 로 등록)
-- {{ source('silver', 'hotspot_congestion') }} 는 ...
```

상세 — [troubleshooting archive §3](../portfolio/troubleshooting/2026-05-10-day-5-dbt-iceberg-compat.md#3-이슈-2--sql-comment-안의-jinja--source--오인식-task-53).

---

## 자주 발생하는 문제

| 증상 | 원인 | 해결 | 참조 |
|---|---|---|---|
| `dbt run` 의 `Failed to read iceberg table. No version was provided` | `iceberg_scan(plain path)` 사용. Lakekeeper UUID-prefix path 미해결 | dbt python model + `lib/duckdb_iceberg` 패턴으로 변경 | [archive §2](../portfolio/troubleshooting/2026-05-10-day-5-dbt-iceberg-compat.md#2-이슈-1--iceberg_scanplain-path-uuid-prefix-mismatch-task-52) |
| `dbt test` 가 singular 미인식 (`Found ... 10 data tests` 인데 11 expected) | SQL comment 안의 jinja 가 dependency 로 오인식 → 그 file 의 test 인식 실패 | comment 의 `{{ ... }}` 를 평문 (괄호 표기) 으로 | [archive §3](../portfolio/troubleshooting/2026-05-10-day-5-dbt-iceberg-compat.md#3-이슈-2--sql-comment-안의-jinja--source--오인식-task-53) |
| `uv sync --extra dbt --extra flink` 실패 (resolution unsatisfiable) | dbt-core 와 apache-beam 의 protobuf 양립 불가 | extra 별도 sync (`--extra dbt` 또는 `--extra flink`). conflicts 정책상 정상 | pyproject.toml `[tool.uv].conflicts` |
| dbt run 후 데이터가 사라짐 (별 process 에서 query fail) | `path: ":memory:"` 라 process 종료 시 lost | `path: "target/dbt_seoul.duckdb"` (gitignored profiles.yml 만 변경) | 본 runbook §profiles.yml path 정책 |
| GitHub Actions dbt job 의 `dbt deps` 실패 | dbt extra 미지정 또는 working-directory 누락 | `uv run --project ../.. --extra dbt dbt deps` + `working-directory: dbt/seoul` | `.github/workflows/ci.yml` |

---

## Day 6 CDC 진입 시 점검

- `dim_place` SCD2 모델 — 동일 패턴 (dbt python model + lib/duckdb_iceberg). `2026-05-10-day-5-dbt-iceberg-compat.md` 참조.
- bronze / silver / gold 의 추가 mart 도 `models/marts/schema.yml` 의 generic tests 추가 — 본 PR β 의 11 tests 패턴 재사용.

---

## Day 10 dbt-docs 진입 시 점검

- `dbt docs generate` + `upload_docs` task 추가 — 본 PR γ Task 5.6 의 plan 변경 항목 (Day 10 본격 시점에 제거된 task 복원).
- sources.yml 도 lineage docs 가치를 위해 다시 추가 검토 — placeholder external_location 으로.

---

## 관련 문서

- [troubleshooting `2026-05-10-day-5-dbt-iceberg-compat.md`](../portfolio/troubleshooting/2026-05-10-day-5-dbt-iceberg-compat.md) — Task 5.2 + 5.3 트러블슈팅 본문
- [troubleshooting `2026-05-09-day-4-tasks-4_1-4_3.md`](../portfolio/troubleshooting/2026-05-09-day-4-tasks-4_1-4_3.md) — lib/duckdb_iceberg 추출 배경
- [Day 4 Silver→Gold runbook](./day4_silver_to_gold.md)
- [PR #29](https://github.com/benidjor/seoul-citydata-platform/pull/29) — Day 5 Task 5.1~5.4
