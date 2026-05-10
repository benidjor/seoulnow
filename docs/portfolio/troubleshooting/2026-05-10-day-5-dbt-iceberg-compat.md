# Day 5 dbt × Iceberg 호환 트러블슈팅

> 작성: 2026-05-10
> 영역: Day 5 Task 5.2 (자치구 시간단위 mart + python staging) + Task 5.3 (dbt tests)
> 관련 PR: #29 (Day 5 Task 5.1~5.4 dbt + CI)

## 1. 배경

Day 5 PR β (#29) 진입 — Task 5.2 의 dbt 모델 (`stg_hotspot_silver` + `fact_hotspot_congestion_hourly`) 작성 후 `dbt run` 검증 단계. plan 원안 시도 → 즉시 fail → 우회안 결정 + 적용 흐름.

추가로 Task 5.3 의 singular test 작성 시 `dbt test` 의 test 인식 실패 발견 → 평문 회피.

본 archive 는 두 이슈 묶음 — dbt-duckdb 와 Lakekeeper-vended Iceberg path 의 호환 이슈 + dbt parser 의 jinja 해석 영역.

---

## 2. 이슈 1 — `iceberg_scan(plain path)` UUID-prefix mismatch (Task 5.2)

### 2.1. 증상

plan 원안의 `stg_hotspot_silver.sql`:

```sql
{{ config(materialized='view') }}
select ... from iceberg_scan('s3://seoul-warehouse/warehouse/silver/hotspot_congestion')
where congest_level_score > 0
```

`dbt run` 실행 결과:

```
Runtime Error in model stg_hotspot_silver
  Failed to read iceberg table. No version was provided and no version-hint
  could be found, globbing the filesystem to locate the latest version is
  disabled by default as this is considered unsafe and could result in
  reading uncommitted data. To enable this use 'SET unsafe_enable_version_guessing = true;'
```

DuckDB 의 iceberg ext 가 plan path 안에서 `metadata.json` 못 찾음.

### 2.2. 진단

`flink_jobs.lib.duckdb_iceberg.table_paths()` 로 실제 silver iceberg location 측정:

```
plan 원안 path  : s3://seoul-warehouse/warehouse/silver/hotspot_congestion
실제 metadata   : s3://seoul-warehouse/warehouse/019dfe9d-303a-77d1-973f-ac641a25248b/019dfe9d-308c-7890-8bcc-c26b6925e990/metadata/00010-...gz.metadata.json
실제 location   : s3://seoul-warehouse/warehouse/019dfe9d-303a-77d1-973f-ac641a25248b/019dfe9d-308c-7890-8bcc-c26b6925e990
```

**원인 = Lakekeeper REST catalog 가 Iceberg table storage path 를 UUID-prefix 두 단계로 할당** (warehouse-id / table-id ulid). plan 작성자는 plain `silver/hotspot_congestion` 같은 직관적 path 가정. 실제는 multi-tenant 충돌 방지 정책상 자체 UUID 부여.

Day 4 archive (`2026-05-09-day-4-tasks-4_1-4_3.md`) 의 학습 재현 — 해당 시점에 lib/duckdb_iceberg.py 의 pyiceberg `plan_files()` + DuckDB `read_parquet()` 우회 패턴이 만들어진 배경.

### 2.3. 해결 — 우회안 B 채택 (dbt python model + lib 호출)

3 우회 후보:

| 후보 | 방식 | 안정성 | lib 재사용 | 시도 비용 |
|---|---|---|---|---|
| A | sources.yml `external_location` 에 read_parquet glob (UUID hardcoded) | △ warehouse 재생성 시 UUID 갱신 | ✗ | ◎ |
| B | dbt python model + lib/duckdb_iceberg | ◎ catalog 가 latest path lookup | ◎ | ○ |
| C | DuckDB ATTACH iceberg REST catalog | ? Lakekeeper 호환 미검증 | ? | ✗ R&D 1~2시간 |

채택 = B. 사유:
- PR α (#28) 의 lib 추출 의도 정합 (Day 10 `slo_daily_report` 가 3번째 사용처 예정).
- UUID hardcoded 회피 → Day 1 fallback / Day 4 v0.5→v0.12 업그레이드 같은 환경 재구성에 robust.
- DuckDB in-memory 환경에선 view vs table materialize 의 storage 차이 0 (python model 의 table 강제 비용 무시).
- Day 4 archive 의 lib 우회 패턴 검증 완료.

적용 코드 (`dbt/seoul/models/staging/stg_hotspot_silver.py`):

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

### 2.4. 부산물 변경 4건

우회안 B 채택의 자연 귀결:

1. **staging materialize view → table**. dbt-duckdb python model 제약. in-memory 환경에선 비용 무시.
2. **`congest_level_score > 0` filter 를 staging → mart 로 이동**. Task 5.3 의 singular test 가 staging ref 직접 사용해 score = 0 row (region 매핑 안 된) 의 congest_level 도 검증해야 함. staging 은 unfiltered raw projection.
3. **sources.yml 제거**. external_location 자동 치환이 UUID-prefix 미해결로 작동 안 함. lineage docs 가치는 Day 10 dbt-docs 진입 시점에 별도 처리.
4. **profiles.yml 의 path `:memory:` → `target/dbt_seoul.duckdb`** (gitignored 사본만, example 은 plan 그대로). row 검증 / 디버깅 시 dbt run 종료 후에도 query 가능.

### 2.5. 검증

`dbt run` 2/2 PASS:
- `stg_hotspot_silver` (python table, 2.7s) — 30 parquet → 306 row.
- `fact_hotspot_congestion_hourly` (sql table, 0.02s) — 자치구 3 × 시각 8 시간 = 24 row.

DuckDB CLI 로 row 직접 query (file-based path):
```python
con = duckdb.connect("target/dbt_seoul.duckdb", read_only=True)
con.execute("SELECT COUNT(*) FROM main.stg_hotspot_silver").fetchone()  # (306,)
con.execute("SELECT COUNT(*) FROM main_gold.fact_hotspot_congestion_hourly").fetchone()  # (24,)
```

---

## 3. 이슈 2 — SQL comment 안의 Jinja `{{ source(...) }}` 오인식 (Task 5.3)

### 3.1. 증상

Task 5.3 의 singular test 작성 후 `dbt test` 결과:

```
WARNING: Test 'test.seoul.assert_congest_level_valid' depends on a source
named 'silver.hotspot_congestion' in package '' which was not found
Found 2 models, 10 data tests, 593 macros
Done. PASS=10 ...
```

**generic 10건만 인식, singular 1건 (`assert_congest_level_valid`) 누락**. WARNING 의 "depends on source 'silver.hotspot_congestion'" 이 의문 — 본 PR 에선 sources.yml 제거 + `{{ ref('stg_hotspot_silver') }}` 사용으로 변경했음.

### 3.2. 원인

singular test SQL 본문의 주석 안에 plan 원안 표기를 그대로 둠:

```sql
-- plan 의 `{{ source('silver', 'hotspot_congestion') }}` 는 sources.yml 제거 변경
-- 으로 `{{ ref('stg_hotspot_silver') }}` 로 변경.
```

**dbt parser 가 SQL comment 안의 jinja 도 dependency 로 해석**. 본 PR 에선 source 가 없어 WARNING + 그 file 의 test 자체 인식 실패.

### 3.3. 해결

Comment 의 jinja 표기를 평문으로:

```sql
-- plan 의 source 호출 (silver.hotspot_congestion) 은 sources.yml 제거 변경
-- 으로 ref 호출 (stg_hotspot_silver) 로 변경.
-- (Jinja 표기 회피 — dbt parser 가 comment 안 source/ref 도 dependency 로
-- 해석하기 때문.)
```

`dbt --no-partial-parse test` 결과:

```
Found 2 models, 11 data tests, 593 macros
2 of 11 START test assert_congest_level_valid
2 of 11 PASS  assert_congest_level_valid
Done. PASS=11
```

11/11 PASS — generic 10 + singular 1.

### 3.4. 학습

dbt parser 의 jinja 처리 — comment 안이라도 `{{ ... }}` 표기는 모두 dependency 로 등록. 본 패턴은 SQL 의 `--`, `/* */` 양쪽 모두.

향후 dbt model / test 작성 시 comment 안 jinja 표기 회피 패턴:
- 함수 표기를 괄호 형태로 풀어쓰기 (`source('x', 'y')` → `source 호출 (x.y)`).
- backtick 안에 넣어도 dbt 가 해석 (회피 안 됨).
- 실제 회피 = 평문 자연어.

---

## 4. 영향 / 다음 단계

### 4.1. lib/duckdb_iceberg 사용처 확장

본 archive 의 우회안 B 적용으로 lib/duckdb_iceberg 의 사용처 = 3 자리:
- `slo_metrics.fetch_samples_from_gold` (Day 4)
- `scripts/duckdb_check.py` (Day 4)
- `dbt/seoul/models/staging/stg_hotspot_silver.py` (Day 5, 본 archive)

Day 10 의 `slo_daily_report` DAG 가 4번째 사용처 예정. lib 추출 (PR #28) 의 의도 정합 확인.

### 4.2. Day 6 CDC + Day 9 Spark 진입 시 검토

- Day 6 의 `dim_place` SCD2 모델도 dbt python model + lib/duckdb_iceberg 패턴 재사용 가능 — Postgres CDC 의 Iceberg 적재 path 도 Lakekeeper UUID-prefix 라 plain path mismatch 동일 발생.
- Day 9 의 Spark MERGE INTO + Compaction 도 Lakekeeper REST catalog 통해 access — 단 Spark 는 Iceberg connector 가 catalog API 직접 사용하므로 path mismatch 영향 없음. dbt 의 한계는 DuckDB iceberg ext 의 catalog 미지원에 있음.

### 4.3. dbt parser 의 jinja 해석 영역

향후 dbt 모델 / test 작성 시 comment 안 jinja 표기 회피 — runbook `day5_dbt.md` §환경 편차 에 명문화.

---

## 관련 문서

- [Day 4 task 4.1~4.3 archive](./2026-05-09-day-4-tasks-4_1-4_3.md) — lib/duckdb_iceberg 추출 배경 + 우회 패턴 본문
- [Day 5 dbt runbook](../../runbook/day5_dbt.md) — 운영 절차 + 자주 발생하는 문제
- [PR #28](https://github.com/benidjor/seoul-citydata-platform/pull/28) — lib 추출 PR
- [PR #29](https://github.com/benidjor/seoul-citydata-platform/pull/29) — Day 5 Task 5.1~5.4 (본 archive 영역)
