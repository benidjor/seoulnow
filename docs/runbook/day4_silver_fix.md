# Day 4 Silver Fix Runbook

Day 3 Task 3.4 의 silver 0 silent commit fail 의 closure 작업. 비슷한 ClassLoader / catalog 인증 모델 충돌 재발 시 진단 + fix 매뉴얼.

> **silver fix 진단 흐름의 archive**: [`2026-05-07-day-3-task-3.4-silver-debug.md`](../portfolio/troubleshooting/2026-05-07-day-3-task-3.4-silver-debug.md) (Day 3 미완) → [`2026-05-08-day-4-silver-fix-resolved.md`](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) (Day 4 closure).
>
> 본 runbook 은 archive 의 진단 자산을 운영 절차로 정리. PyFlink + Lakekeeper REST + Iceberg 환경의 silent commit fail 마주칠 때 사용.

## 사전 조건

- Day 3 PyFlink streaming runbook ([`day3_pyflink.md`](./day3_pyflink.md)) 의 환경이 셋업됨
- main HEAD 에 silver fix 적용됨 (`5125b7d` 이후)
    - `bronze_to_silver.py` 의 `build_env()` 에 `classloader.parent-first-patterns.additional` 포함
    - `infra/lakekeeper/bootstrap.py` 의 `remote-signing-enabled=false` 멱등 적용
    - `docker-compose.yml` 의 Lakekeeper image `v0.12.1`

## 본 runbook 의 사용 시점

다음 증상 마주치면 본 runbook 의 진단 절차 진입:

- PyFlink streaming job 실행 중 stdout 에 ERROR 없음
- Iceberg snapshot count 가 0 으로 멈춤 (checkpoint interval 지나도 commit 안 됨)
- `mc ls` 의 parquet 파일은 누적되지만 metadata.json 추가 안 됨
- 또는 `Invalid config: must be non-empty` / `SignError: token not available` / `LinkageError: com.codahale.metrics` 중 하나 stack trace

## 진단 절차 (7 단계)

### 1. 환경 점검

```bash
docker compose ps                            # 4 healthy
grep minio /etc/hosts                        # 127.0.0.1 minio
docker inspect scp-lakekeeper --format '{{.Config.Image}}'  # quay.io/lakekeeper/catalog:v0.12.1
ls infra/flink/jars/                         # 5 JAR
```

v0.5 image 잔재면 v0.12.1 로 업그레이드 ([§2 참조](#2-lakekeeper-v05--v0121-업그레이드)).

### 2. Lakekeeper v0.5 → v0.12.1 업그레이드

`docker-compose.yml` 의 image tag 확인 + 필요 시 마이그레이션:

```bash
# image tag v0.5 → v0.12.1 변경 후
docker compose pull lakekeeper-migrate lakekeeper
docker compose stop lakekeeper && docker compose rm -f lakekeeper lakekeeper-migrate
docker compose up -d lakekeeper-migrate
docker compose logs lakekeeper-migrate         # "Database migration complete." 확인
docker compose up -d lakekeeper
docker compose ps lakekeeper                   # healthy
```

**healthcheck binary 이름** 도 v0.12.1 에서 `iceberg-catalog` → `lakekeeper` 변경. `docker-compose.yml` 의 `test` 명령도 수정 필요.

### 3. warehouse storage-profile 검증

```bash
curl -s "http://localhost:8181/management/v1/warehouse" | python3 -m json.tool | grep -E "remote-signing-enabled|sts-enabled|flavor"
# 정상:
#   "flavor": "s3-compat"
#   "sts-enabled": false
#   "remote-signing-enabled": false
```

`remote-signing-enabled=true` 면 `bootstrap.py` 재실행:

```bash
uv run --with httpx python infra/lakekeeper/bootstrap.py
# updated warehouse 'seoul' storage-profile (remote-signing-enabled=false)
```

### 4. pyiceberg 직접 commit 으로 책임 소재 분리

PyFlink (Java) vs Lakekeeper (REST server) 의 silent fail 위치 식별:

```bash
uv run --extra flink python <<'PY'
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, NestedField
import pyarrow as pa

catalog = load_catalog("lakekeeper", **{
    "type": "rest", "uri": "http://localhost:8181/catalog", "warehouse": "seoul",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin", "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1", "s3.path-style-access": "true",
})
schema = Schema(NestedField(1, "k", StringType()), NestedField(2, "v", StringType()))
try:
    catalog.drop_table("silver.diag_test")
except Exception:
    pass
table = catalog.create_table("silver.diag_test", schema=schema)
table.append(pa.table({"k": ["a"], "v": ["1"]}))
print(f"snapshots after commit: {len(catalog.load_table('silver.diag_test').metadata.snapshots)}")
PY
```

- snapshots > 0 → **Lakekeeper REST 정상**, PyFlink 측 문제 (§5 진입)
- snapshots == 0 또는 `SignError` / `Invalid config` → **Lakekeeper 측 문제** (§3 재검토 또는 catalog 변경)

### 5. Lakekeeper access log 분석 — PyFlink 의 commit 요청 도달 확인

```bash
docker compose logs --tail=0 -f lakekeeper > /tmp/lk-access.log &
disown

# PyFlink 가동 (90 ~ 120 초 가동)
nohup uv run --extra flink python -m flink_jobs.bronze_to_silver < /dev/null > /tmp/flink.log 2>&1 &
disown
sleep 100

# 분석
grep '"method":"POST"' /tmp/lk-access.log | grep -oE '"uri":"[^"]+"' | sort | uniq -c
# 정상: bronze/silver tables/{table} 에 POST 요청 (snapshot commit) 보임
# 비정상: POST 가 namespaces/tables 생성만 있고 commit 0건 → PyFlink 측 commit step 도달 못함 (§6 진입)
```

### 6. PyFlink 의 silent fail 베일 벗기기

`restart-strategy=none` + `result.wait()` 로 첫 fail stack trace 강제 노출:

```python
# isolation diagnostic script 작성 (예: /tmp/diag_kafka_to_bronze.py)
from pyflink.table import EnvironmentSettings, TableEnvironment
from flink_jobs.lib.iceberg_sink import register_iceberg_catalog

settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)
t_env.get_config().set("pipeline.jars", "...")
t_env.get_config().set("restart-strategy.type", "none")        # 핵심
t_env.get_config().set("execution.checkpointing.interval", "10 s")
register_iceberg_catalog(t_env, catalog_alias="ice")

# Kafka → bronze.diag_test 의 작은 INSERT 1개
result = t_env.execute_sql("INSERT INTO ice.bronze.diag_test ...")
result.wait(60_000)   # TimeoutException (정상 streaming) 또는 Job execution failed (실 root cause stack trace)
```

흔한 stack trace 패턴:
- `LinkageError: com.codahale.metrics.Histogram` → ClassLoader 충돌. §7 fix 적용
- `SignError: Signer set, but token is not available` → §3 재검토 (`remote-signing-enabled=false`)
- `Invalid config: must be non-empty` → §2 재검토 (Lakekeeper v0.12.1 인지)

### 7. ClassLoader 충돌 fix

`bronze_to_silver.py` 의 `build_env()` 에 다음 1 줄 추가:

```python
t_env.get_config().set(
    "classloader.parent-first-patterns.additional",
    "com.codahale.metrics.;io.dropwizard.metrics.",
)
```

`com.codahale.metrics` / `io.dropwizard.metrics` 패키지를 Flink ChildFirstClassLoader 가 system app loader 에 위임 → 단일 loader 가 처리 → LinkageError 회피. **본 fix 가 main 에 이미 정착** (`5125b7d`).

다른 ClassLoader 충돌 후보 (Spark MERGE INTO 등 추가 connector 도입 시 검증 필요):
- `org.slf4j.`
- `com.fasterxml.jackson.`
- `org.apache.arrow.`

## production 검증

```bash
# Producer + PyFlink 5분 가동 후
WAREHOUSE_ID=$(curl -s http://localhost:8181/management/v1/warehouse | python3 -c 'import sys, json; print(json.load(sys.stdin)["warehouses"][0]["warehouse-id"])')

curl -s "http://localhost:8181/catalog/v1/$WAREHOUSE_ID/namespaces/bronze/tables/hotspot_raw?snapshots=all" | python3 -c "
import sys, json
d = json.load(sys.stdin)
m = d.get('metadata', {})
snaps = m.get('snapshots', [])
print(f'snapshots: {len(snaps)}')
print(f'current: {m.get(\"current-snapshot-id\")}')
last = snaps[-1].get('summary', {}) if snaps else {}
print(f'last ckpt: {last.get(\"flink.max-committed-checkpoint-id\")}')
print(f'last total-records: {last.get(\"total-records\")}')
"
# 정상 (5 분 / 30 s checkpoint): snapshots >= 2 (시작 + 새 데이터 cycle), ckpt >= 5
```

silver 도 같은 방식. silver 의 `added-records` 가 bronze 의 새 row 와 일치 (region 매핑 100%, drop 0).

## 학습 패턴

본 fix 의 5 학습 패턴 ([§2.5 archive](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) 참조):

1. **silent fail 진단 임계점** — `restart-strategy=none` + `result.wait()` 명시 패턴
2. **ClassLoader 충돌 패턴** — Flink + 외부 connector 의 흔한 충돌. parent-first-patterns 로 위임
3. **책임 소재 분리** — pyiceberg 직접 commit 으로 PyFlink (Java) vs Lakekeeper (REST) 분리
4. **archive 진단 검증 필요** — `mc ls` 의 파일 누적 vs Iceberg snapshot count 의 차이 (trap)
5. **systematic-debugging Phase 4.5** — fix 3+ 실패 시 architecture 의문 / fallback 진입 임계점

## 후속 작업 link

- Day 4 의 다른 task (Postgres CDC Debezium 등): 별도 runbook 예정
- Day 5 dbt + Airflow: 별도 runbook 예정
- Day 9 Spark MERGE INTO: 본 runbook 의 §7 ClassLoader fix 패턴 적용 가능성 검증 필요

## 관련 문서

- 트러블슈팅 archive
    - [`2026-05-07-day-3-task-3.4-silver-debug.md`](../portfolio/troubleshooting/2026-05-07-day-3-task-3.4-silver-debug.md) — Day 3 silver 0 silent fail 진단 (12 fix + F2 fallback)
    - [`2026-05-08-day-4-silver-fix-resolved.md`](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) — Day 4 silver fix closure (7 단계 + ClassLoader fix + 5 학습 패턴)
- 이전 runbook
    - [`day1_infra.md`](./day1_infra.md) — Day 1 인프라
    - [`day2_producers.md`](./day2_producers.md) — Day 2 producer 운영
    - [`day3_pyflink.md`](./day3_pyflink.md) — Day 3 PyFlink streaming
