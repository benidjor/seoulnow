# Day 10 — SLO 두 가지 분리 측정 + `slo_daily_report` DAG 운영 매뉴얼

Day 10 PR α (#57) 의 SLO 두 가지 분리 재설계 + Airflow 본진 4 DAG 라인업 마지막 정착 (`slo_daily_report`) 운영 절차. Day 8 의 24h SLO 첫 실측 (`count=846 / p95=3.3시간`) 에서 source lag 한계 발견 후 분리 재설계 한 결과를 매일 09:00 KST 자동 측정 + Discord 알림 으로 가시화.

> **archive SoT** — 본 runbook 의 학습 자산은 Day 10 주제별 archive 3건 참조:
> - [`../portfolio/troubleshooting/2026-05-14-day-10-slo-redesign-and-path-b.md`](../portfolio/troubleshooting/2026-05-14-day-10-slo-redesign-and-path-b.md) — SLO 분리 재설계 + Path B + DuckDB BinderException + slo_daily_report DAG 본진 기능 4종 (PR α 학습 자산 4건)
> - [`../portfolio/troubleshooting/2026-05-14-day-10-flink-mini-cluster-and-backfill.md`](../portfolio/troubleshooting/2026-05-14-day-10-flink-mini-cluster-and-backfill.md) — Flink mini-cluster 진단 + backfill 영향 53h SLO 왜곡 (본 세션 #60 후속 학습 자산 2건)
> - [`../portfolio/troubleshooting/2026-05-14-day-10-pr-convention-regression.md`](../portfolio/troubleshooting/2026-05-14-day-10-pr-convention-regression.md) — Task 10.1 plan-update drift + PR template 컨벤션 회귀 (PR β + 본 세션 #61-#63 후속 학습 자산 2건)

## 사전 조건

- Day 9 runbook ([`day9_spark.md`](./day9_spark.md)) 의 Spark 일시 기동 + airflow-scheduler 절차 정착
- Day 8 runbook ([`day8_chill_open.md`](./day8_chill_open.md)) 의 chill-open 데모 + Cloudflare Pages 배포 정착
- Airflow 본진 3 DAG (`dbt_full_run` / `iceberg_maintenance` / `backfill_silver_from_bronze`) 정착 (Day 5-9)
- silver / gold Iceberg 적재 정상 (PyFlink mini-cluster host process — `bronze_to_silver` + `silver_to_gold` + `cdc_to_dim_place`)
- Discord webhook URL 발급 + Airflow Variable `discord_slo_webhook` 등록
- DuckDB + pyiceberg dbt-venv 정착 (Day 9 PR γ Option B 패턴 — Airflow base image 에 duckdb / pyiceberg 미설치 회피)

## 본 runbook 의 사용 시점

다음 작업 / 증상 마주치면 본 runbook 의 절차 진입:

- `slo_daily_report` DAG 자동 실행 결과 확인 (매일 09:00 KST)
- `slo_daily_report` DAG manual trigger
- SLO 측정 명령 (`slo_query.py`) 재현 — host process 직접 호출
- backfill 후 평시 SLO 측정 절차 (Day 9-10 streaming 일시 정지 같은 사례)
- Discord webhook 알림 수신 + 위반 SLO 식별
- Path B 한계 (bronze→silver lag 미포함) + Phase 1B/2 시점 Path C 전환 검토

## 두 가지 SLO 정의

| SLO | 정의 | 임계값 | 의미 |
|---|---|---|---|
| **(α) Data Freshness** | `gold_arrival_ts - api_response_ts(tm)` | P95 < **45분** | 사용자 관점 데이터 나이 (서울 OpenAPI source lag 31m+ 포함) |
| **(β) Platform Latency** | `gold_arrival_ts - silver_arrival_ts` | P95 < **7분** | 우리 통제 구간 (silver→gold) — Path B 결정 |

**한계 명시** — (α) 는 source 측 (서울시 데이터 갱신 주기) 한도를 우리가 통제 불가. 위반 시 source rate 변경 가능성을 우선 점검. (β) 의 Path B 한계 = bronze→silver lag (Kafka broker → silver 적재) 미포함, silver Iceberg `kafka_ts` 컬럼 부재 (PR α 작업 도중 발견) 가 원인.

## SLO 측정 명령 (재현)

### 1. host process 직접 측정

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
  uv run --extra flink python airflow/dags/common/slo_query.py
```

stdout 마지막 라인 = JSON `SLOReport`:

```json
{"data_freshness":  {"name": "data_freshness", "threshold_seconds": 2700,
                     "count": 941, "p50_seconds": 35247, "p95_seconds": 190645,
                     "p99_seconds": 469705, "max_seconds": 675145, "slo_violated": true},
 "platform_latency":{"name": "platform_latency", "threshold_seconds": 420,
                     "count": 809, "p50_seconds": 0, "p95_seconds": 0,
                     "p99_seconds": 0, "max_seconds": 0, "slo_violated": false},
 "any_violated": true}
```

### 2. Airflow 안 dbt-venv subprocess (Option B 패턴)

```bash
docker compose exec -T airflow-scheduler bash -lc \
  '/opt/dbt-venv/bin/python /opt/airflow/dags/common/slo_query.py'
```

(Day 9 PR γ Option B — Airflow base image 에 duckdb / pyiceberg 미설치 회피, dbt-venv 의 python 으로 실행)

### 3. DuckDB 직접 query (수동 디버깅)

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("CREATE SECRET (TYPE 'iceberg', endpoint 'http://lakekeeper:8181/catalog', warehouse 'seoul')")

# Data Freshness 24h window
freshness = con.execute("""
  SELECT count(*) c,
         quantile_cont(epoch(gold_arrival_ts - last_api_response_ts), 0.50) AS p50,
         quantile_cont(epoch(gold_arrival_ts - last_api_response_ts), 0.95) AS p95
    FROM iceberg_scan('gold.fact_hotspot_congestion_5min', union_by_name=true)
   WHERE gold_arrival_ts > now() - INTERVAL 24 HOUR
""").fetchone()
```

## `slo_daily_report` DAG 구조

```
slo_daily_report (schedule: 0 9 * * * KST, max_active_runs=1)
├── measure_slo (BashOperator)              # Option B: dbt-venv subprocess
│   └── do_xcom_push=True                    # stdout 마지막 라인 = JSON SLOReport
├── branch_on_slo_violation (BranchPythonOperator)
│   ├── any_violated == True                # → send_alert 분기
│   └── any_violated == False               # → no_op_skip 분기
├── send_alert (HttpOperator)               # Discord webhook POST
│   ├── trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS
│   └── payload: 위반 SLO 명시 + percentile 메트릭
├── no_op_skip (EmptyOperator)              # 정상 통과 분기
└── on_failure_callback                     # DAG 자체 실패 시 Discord webhook
```

### 본진 기능 발휘 (4종)

| 기능 | 발휘 위치 |
|---|---|
| BranchPythonOperator | `branch_on_slo_violation` (P95 위반 시만 alert 분기) |
| XCom | `measure_slo.do_xcom_push=True` → branch / send_alert pull |
| `on_failure_callback` | DAG 자체 실패 시 Discord webhook (alert 와 별도) |
| HttpOperator + trigger_rule | Discord webhook payload + branch 분기 후 실행 |

## 운영 절차

### 1. DAG manual trigger

```bash
# Airflow UI 또는 CLI
docker compose exec airflow-scheduler airflow dags trigger slo_daily_report

# 결과 확인
docker compose exec airflow-scheduler airflow dags list-runs -d slo_daily_report --limit 3

# task 상세
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run slo_daily_report <run_id>
```

### 2. XCom 측정 결과 확인

```bash
docker compose exec airflow-scheduler \
  airflow tasks render slo_daily_report measure_slo <run_id>

# XCom JSON pull
docker compose exec airflow-scheduler bash -lc \
  "airflow tasks list slo_daily_report && \
   sqlite3 /opt/airflow/airflow.db 'SELECT value FROM xcom WHERE dag_id=\"slo_daily_report\" ORDER BY timestamp DESC LIMIT 1'"
```

### 3. Discord 알림 확인

위반 발생 시 Discord 채널에 다음 형식 메시지 도착:

```
[SLO ALERT] slo_daily_report 2026-05-15 09:00:00 KST

(α) Data Freshness — VIOLATED
    P95 = 52.96h (threshold 45m, 71x 초과)
    count=941, p99=130.5h
    원인 후보: source lag (서울 OpenAPI tm 31m+) 또는 backfill 영향

(β) Platform Latency — PASS
    P95 = 0s (threshold 7m)
    count=809
```

### 4. backfill 후 평시 SLO 측정 절차

**상황**: Day 9 Spark 일시 기동 등으로 PyFlink streaming jobs 일시 정지 → bronze 에 historical row 누적 → 재기동 후 24h SLO window 안 backfill row 가 결과 왜곡.

**절차**:

```bash
# 1. Flink mini-cluster 가동 확인 (host process, docker container 아님!)
ps -ef | grep -E "bronze_to_silver|silver_to_gold|cdc_to_dim_place" | grep -v grep
# 3 process expected (각 host process 의 java + python launcher)

# 2. Kafka consumer LAG = 0 까지 backfill 처리 대기
docker exec scp-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group flink-bronze-hotspot
# CURRENT-OFFSET == LOG-END-OFFSET 모든 partition

# 3. backfill 처리 완료 후 24h 대기 — historical row 가 24h SLO window 밖으로 빠질 때까지
# (24h 대기 못 하면 measure 결과는 backfill 영향으로 SLO 위반 표시)

# 4. 평시 SLO 측정
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
  uv run --extra flink python airflow/dags/common/slo_query.py
```

## Path B 한계 + Path C 전환 가이드

### Path B (현재 채택)

`silver_arrival_ts = bronze→silver Flink job 의 CURRENT_TIMESTAMP` 활용. silver→gold lag 만 측정. silver Iceberg catalog 에 `kafka_ts` 컬럼 부재 (PR α 작업 도중 발견) 가 직접 원인.

**한계**: bronze→silver lag (Kafka broker → silver 적재) 미포함. Kafka 에 메시지 도착 후 bronze 적재 + silver 적재까지의 lag 가 안 측정됨.

### Path C 전환 (Phase 1B/2 검토)

silver Iceberg `kafka_ts` ADD COLUMN + `bronze_to_silver.py` 의 INSERT SELECT 정정 + Flink job 2개 재기동:

```sql
-- silver 테이블 ALTER (Phase 1B Day 11 또는 1B/2 silver schema 정정 시점)
ALTER TABLE ice.silver.hotspot_congestion ADD COLUMN kafka_ts TIMESTAMP_LTZ(3);
```

```python
# bronze_to_silver.py 정정
INSERT INTO ice.silver.hotspot_congestion
SELECT b.*,
       b.kafka_ts,                      -- 추가 (METADATA from Kafka header)
       CURRENT_TIMESTAMP AS silver_arrival_ts
  FROM ice.bronze.hotspot_congestion b
```

전환 후 Platform Latency 정의 = `gold_arrival_ts - kafka_ts(METADATA)` (Kafka→gold full coverage). `compute_platform_latency_seconds(source_ts, gold_ts)` 의 `source_ts` 변수명이 이미 future-proof reuse 가능 형태 — Path C 전환 시 함수 시그니처 변경 X.

## Discord webhook 설정

```bash
# 1. Discord 채널 → 채널 설정 → Integrations → Webhooks → New Webhook
# 2. Webhook URL 복사

# 3. Airflow Variable 등록 (CLI 또는 UI)
docker compose exec airflow-scheduler airflow variables set \
  discord_slo_webhook 'https://discord.com/api/webhooks/<id>/<token>'

# 4. .env 또는 docker-compose.yml 의 environment 에는 박지 않음 (secret leak 방지)
# Airflow Variable 또는 Connections 만 사용
```

DAG 안 사용:

```python
from airflow.models import Variable
discord_url = Variable.get("discord_slo_webhook")
```

## 산출물 (검증 명령)

| 검증 | 명령 | 기대 |
|---|---|---|
| DAG parse | `docker compose exec airflow-scheduler airflow dags list \| grep slo_daily_report` | 1 row 출력 |
| DAG manual trigger | `airflow dags trigger slo_daily_report` | run_id 반환 |
| measure_slo task SUCCESS | `airflow tasks states-for-dag-run slo_daily_report <run_id>` | `measure_slo: success` |
| Branch 분기 정확 | `airflow tasks states-for-dag-run ...` | `any_violated=true → send_alert: success`, `any_violated=false → no_op_skip: success` |
| Discord 알림 도착 | Discord 채널 확인 | 위반 SLO + percentile 메시지 |
| host process 직접 측정 | `uv run --extra flink python airflow/dags/common/slo_query.py` | JSON SLOReport 출력 |

## fallback 시나리오

### 1. Backfill 영향 SLO 결과 왜곡

**증상**: Data Freshness P95 가 비정상적으로 큼 (수십 시간).

**진단**:
1. Kafka consumer LAG 확인 (`flink-bronze-hotspot` partition 별)
2. silver Iceberg `count(*)` 가 24h 안 비정상적으로 큰 값 — backfill 처리 흔적

**처리**:
- backfill 완료 후 24h 대기 + 재측정
- 또는 §4.5 본문에 "backfill 직후 측정값" 명시 + 평시 결과는 별도 commit (phase1a_v1 §4.5 패턴)

### 2. DuckDB `BinderException` (gold 테이블 schema 변경 직후)

**증상**: `union_by_name=true` 옵션 사용에도 BinderException.

**처리**: `slo_query.py` 의 `try / except BinderException → graceful degrade` 적용 (count = 0 반환). 다음 measure cycle 에서 자동 정상화.

### 3. Discord webhook 실패

**증상**: `send_alert` task 가 5xx 또는 timeout.

**처리**:
- `on_failure_callback` 가 Discord 별도 webhook 으로 알림 (cyclic 회피 — 다른 webhook URL)
- Airflow UI 의 task log 확인 + retry 3회 + 실패 시 manual 발송

### 4. silver Iceberg `kafka_ts` 컬럼 추가 후 schema mismatch

**증상**: Path C 전환 직후 silver_to_gold streaming 의 `kafka_ts` 컬럼 mismatch 오류.

**처리**:
- silver_to_gold.py 의 SELECT 절에 `kafka_ts` propagation 추가
- gold ALTER ADD COLUMN `last_kafka_ts TIMESTAMP_LTZ(3)`
- Flink job 2개 재기동 (`bronze_to_silver` + `silver_to_gold`)
- `compute_platform_latency_seconds` 의 `source_ts` 인자에 `last_kafka_ts` 박음
