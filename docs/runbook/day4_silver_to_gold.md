# Day 4 Silver→Gold + SLO + DuckDB 검증 Runbook

PyFlink 1.20 + Iceberg 1.7.1 + Lakekeeper REST 환경에서 Silver→Gold 5분 텀블링 streaming + 데이터 신선도 SLO 리포트 + DuckDB 3-layer 검증 운영 매뉴얼.

> **plan 코드 보강 진단 archive**: 본 runbook 의 silver→gold + SLO + DuckDB 검증 코드 작성 시 식별된 plan 코드 보강 6건은 [`2026-05-09-day-4-tasks-4_1-4_3.md`](../portfolio/troubleshooting/2026-05-09-day-4-tasks-4_1-4_3.md) archive 참조.
>
> 진단 자산을 운영 절차로 정리. Gold mart 적재 / SLO 측정 / DuckDB 검증 마주칠 때 사용.

## 사전 조건

- Day 3 PyFlink streaming runbook ([`day3_pyflink.md`](./day3_pyflink.md)) 의 환경 셋업
    - Kafka / Postgres / MinIO / Lakekeeper container 4 healthy
    - Lakekeeper v0.12.1 + `remote-signing-enabled=false` + warehouse `seoul`
    - JDK 17 + JAVA_HOME + `/etc/hosts` 의 `127.0.0.1 minio` 매핑
- Day 4 silver fix runbook ([`day4_silver_fix.md`](./day4_silver_fix.md)) 의 fix main 정착
    - `bronze_to_silver.py:build_env` 의 `classloader.parent-first-patterns.additional`
    - `infra/lakekeeper/bootstrap.py` 의 `remote-signing-enabled=false` 멱등 적용
- main HEAD 에 silver→gold + SLO + DuckDB 검증 적용됨 (`de2fd2a` 이후)
    - `src/flink_jobs/silver_to_gold.py`
    - `src/flink_jobs/slo_metrics.py` + `tests/unit/test_slo_metrics.py`
    - `scripts/duckdb_check.py`

## 본 runbook 의 사용 시점

다음 작업 / 증상 마주치면 본 runbook 의 절차 진입:

- silver→gold 5분 텀블링 streaming job 가동 / 재기동 / 디버깅
- 데이터 신선도 SLO 리포트 출력 (1회 또는 daily — Day 10 `slo_daily_report` DAG 진입 시)
- Bronze / Silver / Gold 3-layer 의 row count + 샘플 + 자치구별 latest 검증
- 새 환경 셋업 후 Gold 적재 정상 작동 확인
- 다음 증상 stack trace 또는 출력 마주침:
    - Gold snapshot count 0 (TUMBLE 윈도우 close 안 됨)
    - DuckDB `iceberg_scan(s3://...)` IOError (Lakekeeper UUID path 미지원)
    - pyiceberg `t.scan().to_arrow()` 의 `concat_tables(promote_options=...)` TypeError
    - SLO 첫 측정 `slo_violated=True`

## 평소 기동

### 0. Python 환경 + Flink JAR + warehouse 멱등 (Day 3 runbook 통과)

```bash
uv sync --extra dev --extra flink                      # 의존성 일괄
ls infra/flink/jars/                                    # 5 JAR 확인
uv run --with httpx python infra/lakekeeper/bootstrap.py    # warehouse 'seoul' 멱등
docker compose ps                                       # 4 healthy
```

### 1. 3-job 동시 가동 (Producer + Bronze→Silver + Silver→Gold)

silver→gold 는 silver 의 streaming source 를 read 하므로 `bronze_to_silver` 가 동시 가동 + producer 가 데이터 흘려 줘야 새 row 가 윈도우 close 마다 Gold 로 도달.

3 셸 또는 background 분리:

```bash
# 셸 1 — hotspot producer (5분 polling cycle)
uv run python -m producers.hotspot_producer

# 셸 2 — Bronze → Silver streaming
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
  uv run --extra flink python -m flink_jobs.bronze_to_silver

# 셸 3 — Silver → Gold streaming
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
  uv run --extra flink python -m flink_jobs.silver_to_gold
```

또는 `nohup` background 패턴:

```bash
nohup uv run python -m producers.hotspot_producer > /tmp/producer.log 2>&1 &
nohup JAVA_HOME=... uv run --extra flink python -m flink_jobs.bronze_to_silver > /tmp/b2s.log 2>&1 &
nohup JAVA_HOME=... uv run --extra flink python -m flink_jobs.silver_to_gold > /tmp/s2g.log 2>&1 &
disown
```

각 job 의 default = **long-running mode** (SIGTERM 까지 대기 + 1h heartbeat, hotfix PR #46 + `src/flink_jobs/lib/lifecycle.py` SoT). smoke 검증을 원하면 환경변수 명시 export 의무:

```bash
# smoke 모드 — 30분 가동 후 자연 종료
FLINK_SMOKE_RUN_SECONDS=1800 nohup uv run --extra flink python -m flink_jobs.silver_to_gold > /tmp/s2g.log 2>&1 &
```

5분 텀블링 윈도우가 close 되려면 최소 10 분+ 가동 필요 (1 ~ 2 윈도우 close). long-running 모드는 무한 가동이므로 자동 충족.

### 2. Gold 적재 확인

10 분+ 가동 후:

```bash
WAREHOUSE_ID=$(curl -s http://localhost:8181/management/v1/warehouse \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["warehouses"][0]["warehouse-id"])')

curl -s "http://localhost:8181/catalog/v1/$WAREHOUSE_ID/namespaces/gold/tables/fact_hotspot_congestion_5min?snapshots=all" \
  | python3 -c "
import sys, json
m = json.load(sys.stdin).get('metadata', {})
snaps = m.get('snapshots', [])
print(f'snapshots: {len(snaps)}')
print(f'current: {m.get(\"current-snapshot-id\")}')
last = snaps[-1].get('summary', {}) if snaps else {}
print(f'last total-records: {last.get(\"total-records\")}')
print(f'last added-records: {last.get(\"added-records\")}')
"
```

정상 (10분 / 5min tumbling): snapshots >= 1, total-records >= 1, partition by district 3개 (강남구·마포구·영등포구).

### 3. SLO 리포트 출력

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
  uv run --extra flink python -m flink_jobs.slo_metrics
```

출력 예시:

```
== Freshness SLO Report ==
count          : 27
p50 seconds    : 14721
p95 seconds    : 85604  (SLO threshold: 420)
p99 seconds    : 211346
max seconds    : 255494
SLO violated   : True
```

`count` 가 24 시간 이내 Gold row 수. `slo_violated=True` 가 첫 측정에서 흔함 — fixture 시차 / 새 streaming 가동 후 임계값 정상화 검증은 별도.

### 4. DuckDB 3-layer 검증

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
  uv run --extra flink python scripts/duckdb_check.py
```

출력 4 section:

```
== bronze.hotspot_raw count ==
(<count>,)

== silver.hotspot_congestion sample ==      # 5 rows, 9 column tuple
('POI002', '홍대입구역(2호선)', '마포구', '여유', 1, 10000, 12000, ...)

== gold.fact_hotspot_congestion_5min sample ==     # 5 rows, 5 column tuple
(<window_start>, <window_end>, '강남구', 1, 1.0)

== district 별 latest avg_congest_score ==     # district 별 1 row, ORDER BY avg_congest_score DESC
('강남구', 1.0)
('마포구', 1.0)
('영등포구', 1.0)
```

### 5. unit test 회귀 확인

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
  uv run --extra dev --extra flink pytest tests/unit/test_slo_metrics.py -v
```

5 / 5 PASS.

## 환경 편차 / 주의 사항

### `silver_stream_wm` 의 7 컬럼 hardcode

`silver_to_gold.py:create_silver_source_with_watermark` 안의 임시 source-table `silver_stream_wm` 가 silver 17 컬럼 중 7 개 (area_code / district / gu_code / congest_level_score / population_min / population_max / api_response_ts) 만 명시.

silver schema 변경 시 silently miss-projection 가능. mitigation — 함수 docstring 의 drift 주석 (commit `0c645a3`). silver schema 가 바뀌면 `bronze_to_silver.py:create_silver_table` + `silver_to_gold.py:create_silver_source_with_watermark` 양쪽 동기 수정 필요.

Day 5 진입 전 `lib/env.py` + `lib/duckdb_iceberg.py` 추출 시점에 schema 자동 sync 도입 검토.

### Lakekeeper UUID path

Lakekeeper REST 가 vend 하는 actual MinIO path 는 디렉토리 이름 (`gold/fact_hotspot_congestion_5min`) 이 아니라 UUID-prefix:

```
s3://seoul-warehouse/warehouse/<warehouse-uuid>/<table-uuid>/data/district=강남구/...parquet
```

→ DuckDB `iceberg_scan('s3://seoul-warehouse/warehouse/gold/fact_hotspot_congestion_5min')` 직접 호출은 IOError. 정공법은 pyiceberg `plan_files()` 가 catalog 의 metadata 에서 path resolve.

### pyarrow 11 호환

PyFlink 1.20 의 transitive dependency 가 pyarrow 11.0 에 묶여 있음. pyiceberg `t.scan().to_arrow()` 가 pyarrow 14+ 의 `concat_tables(promote_options=...)` 인자를 요구해 fail.

→ `plan_files()` 만 사용해서 path 만 가져오고, 실 read 는 DuckDB `read_parquet(?, hive_partitioning=true)` 가 담당.

### SLO 첫 측정 violated

streaming job smoke run 직후 측정 시 fixture 시차 (silver 의 `api_response_ts` 가 옛 polling 데이터, gold_arrival_ts 는 측정 시점) 때문에 `slo_violated=True` 흔함. 정상 가동 + 새 데이터 유입 후 30분+ 재측정으로 임계값 정상화 검증.

Day 4 종료 게이트는 "리포트 1회 이상 출력" 자체이며 P95 < 420 통과는 별도 게이트.

## 진단 절차 (이슈 마주칠 때)

### 1. Gold snapshot count 0 (TUMBLE 윈도우 close 안 됨)

```bash
# Gold metadata 확인
curl -s "http://localhost:8181/catalog/v1/$WAREHOUSE_ID/namespaces/gold/tables/fact_hotspot_congestion_5min?snapshots=all" \
  | python3 -c 'import sys, json; m=json.load(sys.stdin)["metadata"]; print("snapshots:", len(m.get("snapshots", [])))'
# snapshots: 0
```

원인 후보:
- **bronze→silver 가동 안 됨 / silver row 0** — silver_to_gold 의 source 가 silver 인데 silver 가 비어 있으면 윈도우 close 데이터 없음. `bronze_to_silver` 셸 살아있는지 확인
- **producer 가동 안 됨** — silver 에 새 row 안 흘러옴. hotspot producer 셸 확인
- **wallclock 부족** — 5분 윈도우 + 1분 watermark delay → 최소 6 분+ 가동 필요. 짧은 smoke run 으로는 close 안 됨
- **silver_stream_wm 의 watermark 정의 깨짐** — `silver_to_gold.py:107` 의 `WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE` 누락 시 close 영원히 안 됨. 진단:

```bash
docker compose logs --tail=0 -f lakekeeper > /tmp/lk-access.log &
disown
sleep 600
grep '"method":"POST"' /tmp/lk-access.log | grep gold | wc -l
# 0 → silver_to_gold 의 commit step 도달 못함 → watermark 또는 ClassLoader 점검
```

### 2. DuckDB `iceberg_scan(s3://...)` IOError

```python
con.execute("SELECT * FROM iceberg_scan('s3://seoul-warehouse/warehouse/gold/fact_hotspot_congestion_5min')").fetchall()
# IOError: No such file or directory
```

→ Lakekeeper UUID path 와 호환 안 됨. **본 환경에서는 iceberg_scan 직접 호출 금지**.

대신 pyiceberg `plan_files()` + DuckDB `read_parquet` 우회:

```python
from pyiceberg.catalog import load_catalog
catalog = load_catalog("rest", uri="http://localhost:8181/catalog", warehouse="seoul",
    **{"s3.endpoint": "http://localhost:9000", "s3.access-key-id": "minioadmin",
       "s3.secret-access-key": "minioadmin", "s3.path-style-access": "true",
       "s3.region": "us-east-1"})
table = catalog.load_table("gold.fact_hotspot_congestion_5min")
file_paths = [f.file.file_path for f in table.scan().plan_files()]

import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("CREATE OR REPLACE SECRET (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', "
            "ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
rows = con.execute("SELECT * FROM read_parquet(?, hive_partitioning = true) LIMIT 5",
                   [file_paths]).fetchall()
```

`hive_partitioning=true` 가 Iceberg 의 `district=강남구` 파티션을 컬럼으로 자동 복원. `slo_metrics.fetch_samples_from_gold` + `duckdb_check._configure_duckdb` 가 이 패턴 적용.

### 3. pyiceberg `to_arrow()` TypeError

```python
df = table.scan().to_arrow().to_pandas()
# TypeError: concat_tables() got an unexpected keyword argument 'promote_options'
```

→ pyarrow 11.0 (PyFlink 1.20 transitive) 가 pyarrow 14+ 인자 미지원. **`to_arrow()` 사용 금지**.

대신 `plan_files()` + `read_parquet` (§2 와 동일).

### 4. SLO 첫 측정 `slo_violated=True`

fixture 시차 / 새 streaming 가동 후 정상 — Day 4 종료 게이트는 "리포트 출력" 자체. 임계값 정상화 검증:

```bash
# silver_to_gold + bronze_to_silver + producer 30 분+ 가동 후 재측정
JAVA_HOME=... uv run --extra flink python -m flink_jobs.slo_metrics
```

`count` 가 증가하고 `p95_seconds` 가 7 분 (420 초) 이내로 줄어드는지 확인. 안 줄어들면:
- `silver_to_gold` 의 wallclock latency 자체가 큼 — checkpoint interval / watermark delay 검토
- 또는 producer 의 polling 시각이 너무 옛날 (fixture 시차) — producer 재기동 후 새 polling 시작 시점 기준 재측정

### 5. unit test 5 PASS 안 됨

```bash
uv run --extra dev pytest tests/unit/test_slo_metrics.py -v
# AssertionError: assert rep.p95_seconds == 414  (또는 393)
```

원인:
- **plan 의 414 잔재** — `numpy.percentile([60,90,...,420], 95, method='linear')` = 393 이 정답. 414 는 사실 p99 의 값 (414.6 을 정수로 변환). test 기댓값이 393 으로 정정되어야 정확
- **`_percentile` 구현이 소수점 버림 사용** — `int(...)` 가 epsilon 으로 392 반환 가능. `int(round(...))` 로 정정

main HEAD `de2fd2a` 에서 둘 다 정정된 상태.

## production 검증 (smoke run 30 분)

기준 결과:

| 항목 | 값 |
|---|---|
| Gold snapshot 수 | >= 1 (30 분 / 5 min tumbling = 5+ 윈도우 close) |
| Gold parquet 파일 수 | >= 1 |
| Gold row 수 | partition × window 수 (3 districts × 5 = 15 안팎) |
| Gold partition by district | 3 (강남구·마포구·영등포구) — POI001~003 의 region 매핑 정확 |
| avg_congest_score 범위 | [1.0, 4.0] (한밤중 polling 시 1.0 동률) |
| ERROR / WARN | 0 건 (Lakekeeper access log) |
| SLO 리포트 출력 | count > 0, p50/p95/p99/max 모두 정수, slo_violated 명시 |
| DuckDB 4 section | 모두 정상 출력, zero-row 0건 |
| unit test | 5 / 5 PASS |

## 학습 패턴

본 PR (#26) 의 6 학습 패턴 ([archive §8](../portfolio/troubleshooting/2026-05-09-day-4-tasks-4_1-4_3.md) 참조):

1. **plan 코드 보강 식별 → 적용 → 양쪽 명문화** — silver fix 학습 위에서 plan 코드 vs main sibling 모듈 비교로 사전 식별 (4건). docstring + commit body 양쪽 명문화가 drift 회피의 정공법
2. **DuckDB iceberg ext vs pyiceberg + read_parquet** — Lakekeeper REST + UUID 경로 환경에서는 후자가 정공. `hive_partitioning=true` 가 결정적
3. **spec / plan 의 기댓값 교차 검증** — 414 / 393 사례. TDD 의 Red 단계에서 기댓값을 numpy 호출 결과 또는 본인이 직접 따져 본 값으로 교차 검증하는 절차가 plan 위 작업의 표준
4. **TDD 영역 분리** — 순수 함수 (`compute_freshness_seconds` / `_percentile` / `summarize`) = unit test, streaming job / I/O = smoke run. 두 영역 섞지 않음
5. **PR body 의 grep 자체 점검 절차** — `gh pr create / edit --body-file` / `git commit -F` 직전 또는 직후 `grep -nE "포트폴리오|1번|어필|면접|JD|이력서|취업|회고|서사|narrative|사용자가" <body-file>` 0건 확인 필수
6. **spec reviewer subagent 의 식별 가치** — implementer self-review + ruff 통과한 spec 위반 (duckdb_check 4번 query) 도 plan 명세 한 줄씩 비교로 식별 가능. 두 단계 review (spec compliance → code quality) 의 첫 단계가 본 사례에서 효과 입증

## 후속 작업 link

- **Day 5 진입 전 lib 추출 3건** (선행 필수):
    - `flink_jobs/lib/env.py` (`build_streaming_env()`) — `silver_to_gold.build_env` ↔ `bronze_to_silver.build_env` 7줄 거의 동일 중복
    - `flink_jobs/lib/classpath.py` — `_classpath` private import 정리 (`silver_to_gold.py:38` TODO 주석 명시 상태)
    - `flink_jobs/lib/duckdb_iceberg.py` — pyiceberg `plan_files()` + DuckDB `read_parquet` 패턴 통합 (`slo_metrics.fetch_samples_from_gold` ↔ `duckdb_check._configure_duckdb` 중복). Day 10 `slo_daily_report` DAG 가 3번째 사용처
- **Day 5 dbt-core + Airflow 본진 4 DAG** (별도 runbook 예정)
- **Day 6 Postgres CDC Debezium** (별도 runbook 예정 — spec §6-1 기준 본 runbook 과 무관)
- **Day 9 Spark MERGE INTO** — Spark 의 Iceberg connector 도 같은 ClassLoader 충돌 가능성 검증 필요 ([day4_silver_fix §7](./day4_silver_fix.md#7-classloader-충돌-fix))
- **Day 10 `slo_daily_report` DAG** — 본 runbook 의 `slo_metrics.summarize` 직접 호출 + `lib/duckdb_iceberg.py` 가 3번째 사용처

## 관련 문서

- 트러블슈팅 archive
    - [`2026-05-08-day-4-silver-fix-resolved.md`](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) — Day 4 Task 1 ClassLoader fix 적용으로 silver streaming silent commit fail 해결 (Lakekeeper v0.12.1 포함)
    - [`2026-05-09-day-4-tasks-4_1-4_3.md`](../portfolio/troubleshooting/2026-05-09-day-4-tasks-4_1-4_3.md) — 본 runbook 의 plan 코드 보강 6건 진단 / 시도 / 결정 과정
- 이전 runbook
    - [`day1_infra.md`](./day1_infra.md) — Day 1 인프라
    - [`day2_producers.md`](./day2_producers.md) — Day 2 producer 운영
    - [`day3_pyflink.md`](./day3_pyflink.md) — Day 3 PyFlink streaming
    - [`day4_silver_fix.md`](./day4_silver_fix.md) — Day 4 Task 1 silver fix
- spec / plan
    - `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §6-1 / §6-2 / §6-3
    - `docs/superpowers/plans/phase-1a-week-1.md` Day 4 (L2208~L2628)
