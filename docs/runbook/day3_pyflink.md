# Day 3 PyFlink Streaming Runbook

PyFlink 1.20 + Iceberg 1.7.1 + Lakekeeper REST 환경에서 Kafka → Bronze → Silver streaming job 운영 매뉴얼.

> **silver fix closure**: 본 runbook 의 silver streaming 은 [`2026-05-07-day-3-task-3.4-silver-debug.md`](../portfolio/troubleshooting/2026-05-07-day-3-task-3.4-silver-debug.md) (Day 3 silver 0 silent fail) → [`2026-05-08-day-4-silver-fix-resolved.md`](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) (Day 4 ClassLoader fix) 의 closure 후 production 정상 작동.

## 사전 조건

- Day 1 인프라가 healthy ([`day1_infra.md`](./day1_infra.md))
    - Kafka, Postgres, MinIO, Lakekeeper container 4 healthy
    - **Lakekeeper v0.12.1 적용** (Day 4 fix 후 main 정착)
- Day 2 producer 가동 ([`day2_producers.md`](./day2_producers.md))
    - hotspot producer 가 `seoul.hotspot.congestion.v1` 토픽에 메시지 발행 중
- JDK 17 (Eclipse Temurin) 설치 + `JAVA_HOME` 설정
    - macOS: `JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home`
- `/etc/hosts` 에 `127.0.0.1 minio` 매핑
    - Lakekeeper REST 가 vending 하는 `http://minio:9000` 엔드포인트를 호스트의 Iceberg client 가 resolve 가능해야 함
    - Day 1 환경 셋업 시 1회 적용 ([`2026-05-01-day-1-lakekeeper-v05-setup.md`](../portfolio/troubleshooting/2026-05-01-day-1-lakekeeper-v05-setup.md) 참조)

## 평소 기동

### Python 환경 + Flink JAR 동기화

```bash
uv sync --extra dev --extra flink         # PyFlink 1.20 + dev (pytest / mypy / ruff) 일괄
ls infra/flink/jars/                       # 5 JAR 확인
# flink-sql-connector-kafka-3.3.0-1.20.jar
# hadoop-client-api-3.3.4.jar
# hadoop-client-runtime-3.3.4.jar
# iceberg-aws-bundle-1.7.1.jar
# iceberg-flink-runtime-1.20-1.7.1.jar
```

JAR 누락 시 다운로드:

```bash
bash infra/flink/download_jars.sh          # Maven Central 에서 5 JAR 멱등 다운로드
```

### Lakekeeper warehouse 보장 (멱등)

```bash
uv run --with httpx python infra/lakekeeper/bootstrap.py
# 출력 1: warehouse 'seoul' exists, syncing storage profile
#       updated warehouse 'seoul' storage-profile (remote-signing-enabled=false)
# 또는
# 출력 2: created warehouse 'seoul'
```

`bootstrap.py` 가 v0.12.1 schema + `remote-signing-enabled=false` 로 storage profile 멱등 동기화. 새 환경에서 `docker compose up -d` 후 1회 실행 필요.

### Bronze → Silver streaming job 실행

```bash
# smoke 검증 (10분 가동)
FLINK_SMOKE_RUN_SECONDS=600 \
  JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
  uv run --extra flink python -m flink_jobs.bronze_to_silver
```

기본 동작:
- Kafka source `seoul.hotspot.congestion.v1` 의 메시지 read
- `ice.bronze.hotspot_raw` 에 INSERT (PyFlink 의 Iceberg sink, 30 초 checkpoint 마다 commit)
- `ice.bronze.hotspot_raw` → 같은 catalog 의 `ice.silver.hotspot_congestion` (region 매핑 + congest score 변환 + partition by district)
- `FLINK_SMOKE_RUN_SECONDS` 환경변수 의존 — **default 0 (long-running mode)**, SIGTERM 까지 대기 + 1h heartbeat. **smoke 검증 시 명시적으로 `FLINK_SMOKE_RUN_SECONDS=600` export 의무** (위 명령 line 56 패턴). hotfix PR #46 + `src/flink_jobs/lib/lifecycle.py` SoT.

### 백그라운드 실행 (장기)

```bash
nohup uv run --extra flink python -m flink_jobs.bronze_to_silver < /dev/null > /tmp/flink.log 2>&1 &
disown
```

PyFlink LocalEnvironment 의 mini-cluster 는 main thread 종료 = job 종료. `nohup` + `disown` + `< /dev/null` 조합이 stdin 상실로 인한 silent 종료 회피.

## 정지

```bash
# graceful — SIGTERM
pkill -f bronze_to_silver
# checkpoint complete 후 자동 종료
```

## 검증 — Iceberg snapshot commit

**핵심 지표**: Iceberg snapshot count + `flink.max-committed-checkpoint-id`. parquet data file 누적 (`mc ls`) 만으로 commit 정상 판단 금지 (writer staging 단계 결과일 수 있음, [`2026-05-08-day-4-silver-fix-resolved.md` §1](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) 의 진단 trap 참조).

```bash
# bronze.hotspot_raw snapshot 확인
curl -s "http://localhost:8181/catalog/v1/$(curl -s http://localhost:8181/management/v1/warehouse | python3 -c 'import sys, json; print(json.load(sys.stdin)["warehouses"][0]["warehouse-id"])')/namespaces/bronze/tables/hotspot_raw" | python3 -c "
import sys, json
d = json.load(sys.stdin)
m = d.get('metadata', {})
s = m.get('snapshots', [])
last = s[-1].get('summary', {}) if s else {}
print(f'snapshots: {len(s)}')
print(f'current-snapshot-id: {m.get(\"current-snapshot-id\")}')
print(f'last ckpt: {last.get(\"flink.max-committed-checkpoint-id\")}')
print(f'last total-records: {last.get(\"total-records\")}')
"
# 정상 출력: snapshots > 0 + current-snapshot-id != None + last ckpt > 0
```

`silver.hotspot_congestion` 도 같은 방식으로 확인. silver 의 `added-records` 가 bronze 의 새 row 와 일치 (region 매핑 100%, drop 0).

## 자주 발생 케이스

### silent commit fail 의심 — snapshot 0

**증상**: PyFlink stdout 에 INFO 만 보이고 ERROR 없음. snapshot count 0 으로 멈춤.

**진단 1 단계**: `restart-strategy=none` + `result.wait()` 명시로 첫 fail stack trace 확보.

```python
# 진단용 build_env() 임시 변경
t_env.get_config().set("restart-strategy.type", "none")
t_env.get_config().set("execution.checkpointing.interval", "10 s")
# stmt_set.execute() 후
result.wait(60_000)  # TimeoutException 또는 Job execution failed 의 stack trace 노출
```

**진단 2 단계**: Lakekeeper access log 에서 PyFlink 의 commit POST 요청 횟수 확인.

```bash
docker compose logs -f lakekeeper > /tmp/lk-access.log &
# PyFlink 가동 + 90s 후
grep -oE '"method":"[^"]+"' /tmp/lk-access.log | sort | uniq -c
grep '"method":"POST"' /tmp/lk-access.log | grep -oE '"uri":"[^"]+"' | sort | uniq -c
# bronze/silver 테이블에 POST commit 요청이 있는지 확인
# 0 건이면 PyFlink 측 문제 (commit step 도달 못함)
```

**진단 3 단계**: pyiceberg 직접 commit 으로 책임 소재 분리.

```python
from pyiceberg.catalog import load_catalog
catalog = load_catalog("lakekeeper", **{
    "type": "rest", "uri": "http://localhost:8181/catalog", "warehouse": "seoul",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin", "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1", "s3.path-style-access": "true",
})
# silver namespace 에 직접 테이블 생성 + append → commit 작동 여부
# 작동 → Lakekeeper REST 정상, PyFlink 측 fix
# 안 됨 → Lakekeeper REST commit path 문제
```

상세 — [`2026-05-08-day-4-silver-fix-resolved.md` §2](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) 의 7 단계 진단 흐름.

### `Invalid config: must be non-empty` (Iceberg 1.7.x strict validation)

Lakekeeper v0.5 의 잔재. v0.12.1 업그레이드 후 사라짐. 만약 환경이 v0.5 라면 `docker-compose.yml` 의 image tag 확인.

상세 — [`2026-05-07-day-3-task-3.4-silver-debug.md` §1](../portfolio/troubleshooting/2026-05-07-day-3-task-3.4-silver-debug.md).

### `SignError: Signer set, but token is not available` (pyiceberg)

Lakekeeper 의 `remote-signing-enabled=true` (default) + auth disable 환경 충돌. fix:

```bash
# warehouse storage-profile patch (또는 bootstrap.py 재실행 — 멱등)
uv run --with httpx python infra/lakekeeper/bootstrap.py
# 출력: updated warehouse 'seoul' storage-profile (remote-signing-enabled=false)
```

상세 — [`2026-05-08-day-4-silver-fix-resolved.md` §2.4](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md).

### `LinkageError: com.codahale.metrics.Histogram` (PyFlink)

ClassLoader 충돌. `bronze_to_silver.py` 의 `build_env()` 에 다음 설정이 있는지 확인:

```python
t_env.get_config().set(
    "classloader.parent-first-patterns.additional",
    "com.codahale.metrics.;io.dropwizard.metrics.",
)
```

본 설정이 main 의 코드에 정착됨 (Day 4 fix). 누락되면 silent commit fail.

### Producer 메시지가 흐르는데 PyFlink Kafka source 가 안 받음

확인:

```bash
# topic 의 latest offset
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic seoul.hotspot.congestion.v1 --time -1

# consumer group 등록 확인
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list
# 'flink-bronze-hotspot' 가 보이면 정상
# 안 보이면 PyFlink Kafka source 시작 못함 → §"silent commit fail 의심" 진단
```

## 메모리 / 비용 mitigation

PyFlink LocalEnvironment 의 mini-cluster 는 ~ 1.5 GB 메모리 사용. Day 5+ Airflow + Day 9 Spark 와 동시 가동 시 24 GB Oracle Cloud VM 제한 주의:

```bash
# 메모리 사용량 확인
docker stats --no-stream
# free 메모리 80% (19.2 GB) 임계 초과 시
# - airflow-scheduler 일시 stop (~700 MB 회수)
# - PyFlink 도 SMOKE_RUN_SECONDS 단축 후 재기동
```

## 후속 작업 link

- Day 4 작업 (Postgres CDC Debezium): 별도 runbook 예정
- Day 5 dbt + Airflow 본진 4 DAG: 별도 runbook 예정
- Day 9 Spark MERGE INTO: 본 runbook 의 silver 위에서 진행. ClassLoader 충돌 가능성 검증 필요 ([`2026-05-08-day-4-silver-fix-resolved.md` §6](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md))

## 관련 문서

- 트러블슈팅 archive
    - [`2026-05-05-day-3-pre-entry.md`](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md) — Day 3 진입 전 환경 셋업
    - [`2026-05-07-day-3-task-3.4-silver-debug.md`](../portfolio/troubleshooting/2026-05-07-day-3-task-3.4-silver-debug.md) — silver 0 silent fail 진단 (12 fix + F2 fallback)
    - [`2026-05-08-day-4-silver-fix-resolved.md`](../portfolio/troubleshooting/2026-05-08-day-4-silver-fix-resolved.md) — silver fix 완결 (7 단계 + ClassLoader fix)
- 이전 runbook
    - [`day1_infra.md`](./day1_infra.md) — Day 1 인프라
    - [`day2_producers.md`](./day2_producers.md) — Day 2 producer 운영
