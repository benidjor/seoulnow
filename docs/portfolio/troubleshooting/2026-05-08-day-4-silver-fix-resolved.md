# Day 4 silver fix — 7 단계 진단 + 진짜 root cause closure

> 직전 archive (`2026-05-07-day-3-task-3.4-silver-debug.md`) 의 F2 fallback 진입 상태에서 Day 4 first task 로 silver fix 재개. 누적 12 + 신규 7 단계 진단 끝에 진짜 root cause = **`com.codahale.metrics.Histogram` ClassLoader 충돌** 식별 및 1줄 fix.
>
> 이전 archive: `2026-05-07-day-3-task-3.4-silver-debug.md` (Day 3 fix #1~5 + F2 fallback 결정).

## 0. 한줄 결과

**production 정식 작동** — bronze + silver streaming pipeline 5분 가동 기준 11회 / 12회 checkpoint commit, ERROR 0건, Producer 두 번째 polling cycle (+3 row) 정확히 incremental 처리.

main HEAD 진입 시점: `ffd3df8` (직전 PR #22 squash). 본 brunch: `phase-1a/day-4-silver-fix`.

## 1. 직전 archive 의 진단 오류 정정

직전 archive (Day 3 Task 3.4) 의 "bronze 측 streaming 정상 (Kafka source → Iceberg bronze 매 30초 commit)" 진단이 **부분적으로 오류**. 본 세션 진입 시 bronze 의 metadata 직접 확인:

| 지표 | 직전 archive 의 진단 | 본 세션 확인 결과 |
|---|---|---|
| Iceberg snapshot count | "정상 commit" (정확한 count 미확인) | **0** — 한 번도 snapshot commit 못함 |
| MinIO data file | 누적 (POI별 parquet) | 누적 ✓ (writer task 의 staging 단계) |
| metadata.json 파일 | (미확인) | 1개 (00000-init.gz.metadata.json) |

즉 bronze 의 MinIO parquet 누적은 Flink writer task 의 partition 별 staging 단계 결과이고 **Iceberg snapshot commit 까지는 한 번도 도달 못했음**. silver 만 silent fail 이 아니라 **bronze + silver 모두 silent commit fail**.

`mc ls` 의 parquet 파일 누적 = "정상 작동" 이라고 본 게 진단 trap. snapshot count 가 commit 의 정확한 지표.

## 2. 7 단계 진단 여정

### 2.1. 1단계 — Lakekeeper v0.5 → v0.12.1 업그레이드 (archive §1.6 fix path #1)

직전 archive 의 우선순위 1 fix path 적용:
- `docker-compose.yml`: `quay.io/lakekeeper/catalog:v0.5` → `v0.12.1`
- `lakekeeper-migrate` 컨테이너로 metadata 마이그레이션 (기존 warehouse `seoul` + 모든 namespace + table 보존)
- healthcheck binary 이름 변경: `/home/nonroot/iceberg-catalog` → `/home/nonroot/lakekeeper`

LoadTable response 비교:

```diff
# v0.5
"storage-credentials": [{"prefix": "...", "config": {}}]   # 빈 dict
# Iceberg 1.7.x: IllegalArgumentException: "Invalid config: must be non-empty"

# v0.12.1
"storage-credentials": [{"prefix": "...", "config": {
    "s3.endpoint": "http://minio:9000/",
    "s3.region": "us-east-1",
    "client.region": "us-east-1",
    "s3.path-style-access": "true"
}}]
# Iceberg 1.7.x validation 통과
```

**v0.5 의 root cause (빈 config 거부) 는 fix.** 그러나 PyFlink 재실행 시 silver/bronze snapshot 모두 0 그대로 → 새 silent fail 영역 존재.

### 2.2. 2단계 — vended-credentials 헤더 제거 시도

직전 archive fix #5 의 마지막 상태 (`'header.X-Iceberg-Access-Delegation' = 'vended-credentials'`) 가 v0.12 의 default 동작과 충돌 가능성 의심. iceberg_sink.py 의 catalog DDL 에서 헤더 제거 후 재실행. 같은 silent.

### 2.3. 3단계 — pyiceberg 직접 commit 검증 (책임 소재 분리)

PyFlink (Java) vs Lakekeeper (REST server) 의 책임 소재 분리. pyiceberg 0.7.1 (Python) 으로 silver namespace 에 직접 commit 시도.

```
pyiceberg.exceptions.SignError: Signer set, but token is not available
  at pyiceberg/io/fsspec.py:85 → s3v4_rest_signer
```

**중대 진단**: v0.12.1 의 LoadTable response config 에 `s3.signer = S3V4RestSigner` 박혀 있음 → client 가 sign API 호출 시도 → Lakekeeper authentication disable 모드라 token 없음 → SignError. v0.5 시절 빈 config 와는 다른 새 root cause.

```
WARN message=Authentication is disabled. This is not suitable for production!
config 에 s3.signer.uri = http://localhost:8181/catalog/
config 에 s3.signer.endpoint = v1/signer/{warehouse-id}/.../v1/aws/s3/sign
```

### 2.4. 4단계 — `remote-signing-enabled = false` patch (Lakekeeper 측 fix)

Lakekeeper management API 로 storage-profile patch:

```
POST /management/v1/warehouse/{id}/storage
{
  "storage-profile": { ..., "remote-signing-enabled": false, "sts-enabled": false, ... },
  "storage-credential": { "type": "s3", "credential-type": "access-key", ... }
}
```

LoadTable response 갱신 — `s3.signer` 사라짐, config 가 endpoint/region 만 깔끔. pyiceberg 재테스트:

```
PYI: append() returned without exception
PYI: snapshots count = 1
PYI: FIRST SNAPSHOT: id=2094977361455510964, summary=operation=Operation.APPEND
```

**Lakekeeper REST 의 commit path 자체 정상 작동 검증.** pyiceberg 가 silver 에 첫 snapshot 추가 성공.

### 2.5. 5단계 — Lakekeeper access log 분석 (PyFlink 측 silent 위치 식별)

PyFlink 재실행 시에도 silent. 같은 fix 가 PyFlink 에는 안 통하는 이유 식별 필요.

`docker compose logs -f lakekeeper` 로 90초 access log tracking:

| HTTP method | URI | 횟수 | 의미 |
|---|---|---|---|
| GET | tables/.../?snapshots=all | 다수 | Iceberg streaming source 의 monitor-interval read (정상) |
| POST | namespaces | 6 | 시작 시 namespace 생성 (모두 409 already exists) |
| POST | namespaces/bronze/tables | 2 | 시작 시 bronze 테이블 생성 |
| POST | namespaces/silver/tables | 2 | 시작 시 silver 테이블 생성 |
| **POST** | **bronze/tables/{tbl} 의 commit** | **0** | **PyFlink 가 commit 요청 자체를 안 보냄** |

**결정적 발견**: PyFlink 의 Iceberg sink 가 commit step 까지 도달 못함. Lakekeeper 가 받는 건 source 측 read 뿐.

추가 확인:
- Kafka consumer group `flink-bronze-hotspot` 미등록 (`GroupIdNotFoundException`) — Kafka source 도 시작 못함
- Kafka topic 에는 메시지 23건 있음 (producer 발행 정상)
- PyFlink stdout 에 Kafka source / Iceberg sink 시작 메시지 0건 — silent

### 2.6. 6단계 — `restart-strategy=none` + 명시 wait 로 silent 베일 벗기기

PyFlink 의 default `restart-strategy = fixed-delay 무한 retry` 가 silent 의 원인. 분리된 isolation 진단 script (`/tmp/diag_kafka_to_bronze.py`) 작성:

- Kafka source 만 1개 INSERT (Kafka → bronze)
- `restart-strategy.type = none` 강제
- `result.wait(60_000)` 로 명시적 wait + exception 잡기

결과 — JobExecutionFailed → 진짜 stack trace 노출:

```java
Caused by: java.lang.LinkageError: loader constraint violation:
loader 'app' wants to load class com.codahale.metrics.Histogram.
A different class with the same name was previously loaded by
org.apache.flink.util.ChildFirstClassLoader @33505649.

at org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper.update(DropwizardHistogramWrapper.java:41)
at org.apache.iceberg.flink.sink.IcebergStreamWriterMetrics.lambda$updateFlushResult$0(IcebergStreamWriterMetrics.java:77)
at org.apache.iceberg.flink.sink.IcebergStreamWriter.flush(IcebergStreamWriter.java:113)
at org.apache.iceberg.flink.sink.IcebergStreamWriter.prepareSnapshotPreBarrier(IcebergStreamWriter.java:67)
```

**진짜 root cause**: `com.codahale.metrics.Histogram` 클래스가 두 ClassLoader 에 의해 동시 로드 → LinkageError. iceberg-flink-runtime-1.7.1 jar 내부의 codahale 와 PyFlink JVM system classpath 의 codahale 가 충돌. `IcebergStreamWriter.prepareSnapshotPreBarrier()` (= checkpoint 직전 단계) 매번 fail → commit step 도달 안 됨 → silent.

silent 의 정확한 메커니즘:
- `restart-strategy = fixed-delay 무한 retry` (streaming default) → 매 fail 마다 task restart, main thread 에 exception 안 올라옴
- INFO log 에 "Restarting task" 메시지가 떴어야 정상이지만, mini-cluster 의 logger 가 stdout 안 가서 silent
- 결과: 무한 restart loop + main 정상 sleep + 사용자 시점 silent

### 2.7. 7단계 — `classloader.parent-first-patterns.additional` 1줄 fix

```python
t_env.get_config().set(
    "classloader.parent-first-patterns.additional",
    "com.codahale.metrics.;io.dropwizard.metrics.",
)
```

ChildFirstClassLoader (Flink default) 가 위 패키지에 대해서만 parent loader (system app) 우선 위임 → 단일 loader 가 처리 → LinkageError 회피.

`/tmp/diag_kafka_to_bronze.py` 재실행:
- JobExecutionFailed 사라짐 → TimeoutException (streaming 의 정상 동작)
- bronze.diag_kafka_test snapshot 추가, added-records=23 (Kafka topic 의 누적 메시지 모두 흡수)
- `flink.max-committed-checkpoint-id: 1` (첫 checkpoint commit 성공)
- engine-name: flink, iceberg-version: Apache Iceberg 1.7.1

production `bronze_to_silver.py` 의 `build_env()` 에 동일 적용 후 5분 가동 검증 (§3 참조).

## 3. production 정식 검증 (5분 가동)

### 3.1. multi-checkpoint 안정성

| 시점 | bronze snapshot/total/added | silver snapshot/total/added |
|---|---|---|
| baseline (시작 직전) | 1/26/26 | 1/26/26 |
| T+60s | 2/55/29 | 2/81/55 |
| T+300s (Producer 새 cycle 후) | 3/58/3 | 3/84/3 |

bronze current-snapshot 의 `flink.max-committed-checkpoint-id = 11`, silver = `12`. 30초 checkpoint interval × 5분 = 10회+ checkpoint 정상.

### 3.2. ERROR / commit POST 분석

5분 가동 동안 Lakekeeper access log:
- ERROR / WARN level 응답: **0건**
- commit POST (`POST /tables/{tbl}` update): bronze 2회 + silver 2회 (새 데이터 있는 ckpt 만 commit, empty commit 회피 = 정상)
- GET (source streaming read): 90건

### 3.3. schema / partition / enrich 검증

| 항목 | 결과 |
|---|---|
| bronze schema | 13 columns (raw + ingest_ts/kafka_ts) |
| silver schema | 17 columns (bronze 13 + enrich 4: district/gu_code/lat/lng/score + silver_arrival_ts) |
| partition by district | 3 partition (강남구/마포구/영등포구) — URL-encoded path 정상 |
| silver enrich UDTF 매핑 | bronze 새 row 3건 → silver 3건 (drop 0, region CSV POI001~003 100% 매칭) |
| MinIO data file | 203개 누적 (parquet + manifest list + manifest + metadata.json) |

## 4. 본 fix 의 5가지 코드 변경

| # | 파일 | 변경 |
|---|---|---|
| 1 | `docker-compose.yml` | Lakekeeper image `v0.5` → `v0.12.1` (lakekeeper + lakekeeper-migrate 둘 다) |
| 2 | `docker-compose.yml` | healthcheck binary 이름 `iceberg-catalog` → `lakekeeper` (v0.12 binary rename) |
| 3 | `infra/lakekeeper/bootstrap.py` | storage-profile schema v0.12 호환 + `remote-signing-enabled=false` 명시 + 기존 warehouse `update_warehouse_storage` 추가 (멱등) |
| 4 | `src/flink_jobs/lib/iceberg_sink.py` | catalog DDL 의 `header.X-Iceberg-Access-Delegation = vended-credentials` 제거 (Lakekeeper 의 default 동작에 위임, remote-signing-enabled=false 와 정합) |
| 5 | `src/flink_jobs/bronze_to_silver.py` | `build_env()` 에 `classloader.parent-first-patterns.additional = com.codahale.metrics.;io.dropwizard.metrics.` 추가 ← **진짜 root cause fix** |

## 5. 학습 포인트

### 5.1. silent fail 의 진단 임계점

`restart-strategy = fixed-delay 무한 retry` 는 streaming 의 운영상 정답이지만 **fail 진단의 베일**. 진단 모드에서는 명시적으로 `none` 으로 강제해야 첫 fail 의 stack trace 확보. PyFlink LocalEnvironment + mini-cluster 환경에서 task manager log 가 stdout 으로 안 가는 한계까지 합쳐지면 silent 가 깊어짐.

진단 패턴: isolation script (작은 INSERT 1개) + `restart-strategy=none` + `result.wait()` 명시 → 진짜 stack trace 가 main thread 로 올라옴.

### 5.2. ClassLoader 충돌은 Flink + external connector 의 흔한 패턴

Flink 의 `ChildFirstClassLoader` (default) 는 user code / connector jar 안의 클래스를 우선 로드. shaded 안 된 외부 jar 가 system classpath 의 같은 클래스와 conflict 시 LinkageError. `classloader.parent-first-patterns.additional` 로 특정 패키지만 parent-first 위임이 표준 fix.

다른 흔한 충돌 후보:
- `org.slf4j.` (logging)
- `com.fasterxml.jackson.` (JSON)
- `io.dropwizard.metrics.` (본 case 와 같은 영역)

### 5.3. 책임 소재 분리의 가치

PyFlink (Java client) vs Lakekeeper (REST server) 의 silent 책임 소재가 모호했던 시점에 **pyiceberg (Python client) 직접 commit 시도** 가 결정적. Lakekeeper REST 자체는 정상 작동하는 게 확인되어 PyFlink 측 fix 에 집중 가능. 이걸 안 했으면 Lakekeeper REST 영역에서 더 많은 시간 낭비 가능했음.

### 5.4. archive 진단의 검증 필요성

직전 archive 의 "bronze 정상" 진단이 부분 오류였던 게 본 세션의 핵심 발견. **`mc ls` 의 parquet 파일 누적 = commit 정상 이라는 추론이 trap**. Iceberg snapshot count + metadata.json 개수가 정확한 지표. 진단 자산도 검증 필요.

### 5.5. systematic-debugging Phase 4.5 의 적용 평가

직전 archive 의 F2 fallback 결정 (silver 0 채로 본 brunch 마무리) 은 정당했음 — 본 세션의 7 단계 진단 자체가 큰 작업이었고 직전 brunch (Day 3 Task 3.4) 의 scope 안에 들이기엔 부담. F2 fallback 은 "문제 미루기" 가 아니라 "진단 자산 위에서 명확한 출발점 확보" 의 효과.

본 세션의 시간 투자: 약 2 시간+ (v0.12 업그레이드 + remote-signing patch + 7 단계 진단 + classloader fix + 5분 검증). 사용자가 "한 번 더 시도" 약속 후 "추가 디버그" 결정 → 7 단계 끝에 진짜 root cause 식별 + 1줄 fix. 시간 RoI 매우 높음.

## 6. 다음 단계 영향

| 영역 | 영향 |
|---|---|
| Day 4 의 원래 first task | Postgres CDC Debezium 도입 진입 가능 |
| Day 5+ Airflow 4 DAG | 본 fix 의 Iceberg pipeline 위에서 정상 작동 가능 |
| Day 9 Spark MERGE INTO | 본 fix 의 Lakekeeper v0.12 + remote-signing-disabled 환경에서 진행. Spark 의 Iceberg connector 도 같은 ClassLoader 충돌 가능성 — 검증 필요 |
| Day 5+ DuckDB read | Lakekeeper 의 vended-credentials 응답에 access-key 미포함 → DuckDB ATTACH 시 client side 인증 옵션 필요. Day 5 dbt/DuckDB 통합 시 별도 fix |
| portfolio 서사 | Lakekeeper REST main path 유지 + 본 archive 의 7 단계 진단 자체가 자산 (Lakekeeper v0.5 빈 config → v0.12 remote-signing → ClassLoader 충돌 의 3 단계 closure) |

## 7. 본 archive 의 위치

- 상위 진단 archive (Day 3): `2026-05-07-day-3-task-3.4-silver-debug.md`
- 이전 archives (Day 1~3): `2026-05-01-lakekeeper-v05-setup.md`, `2026-05-02-day-2-producers-troubleshooting.md`, `2026-05-05-day-3-pre-entry.md`
- 본 archive (Day 4): `2026-05-08-day-4-silver-fix-resolved.md`
