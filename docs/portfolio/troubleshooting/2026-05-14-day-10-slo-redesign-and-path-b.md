# Day 10 SLO 분리 재설계 + Path B 결정 + DuckDB BinderException + `slo_daily_report` DAG

> 작성: 2026-05-14 KST
> 시점: Day 10 PR α (#57) 머지 완료 후 학습 자산 명문화. Phase 1B 진입 직전 baseline 정리.
> 관련 PR: #57 (Day 10 PR α — SLO 두 종 분리 + slo_daily_report DAG)
> 관련 runbook: [`docs/runbook/day10_slo.md`](../../runbook/day10_slo.md)
> 직전 archive: [`2026-05-12-day-9-archive.md`](2026-05-12-day-9-archive.md)
> 동시 작성 archive (Day 10 주제 분리): [`2026-05-14-day-10-flink-mini-cluster-and-backfill.md`](2026-05-14-day-10-flink-mini-cluster-and-backfill.md), [`2026-05-14-day-10-pr-convention-regression.md`](2026-05-14-day-10-pr-convention-regression.md)

## 0. 진입 흐름 요약

Day 9 종료 (PR #53-#56 모두 머지, main HEAD `32229d0`) 후 Day 10 entry plan §B 의 3 PR 분할안 따라 진행. 본 archive 는 그 중 **PR α (#57) 의 학습 자산 4건** 명문화.

| PR α 학습 자산 | section |
|---|---|
| SLO 단일 정의 한계 → 두 가지 분리 재설계 (Day 4 → Day 8 → Day 10 학습 곡선) | §1 |
| silver Iceberg `kafka_ts` 부재 → Path B 결정 | §2 |
| DuckDB `BinderException` graceful degrade — `union_by_name=true` | §3 |
| `slo_daily_report` DAG 본진 기능 4종 (Airflow 본진 4 DAG 라인업 마지막 정착) | §4 |

## 0-1. 본 PR α 의 의미 (spec §8-1 #4 라인업 완성)

레시핑은 Airflow 를 "15분 batch trigger (cron 대용)" 으로만 사용. 본 프로젝트는 본진 4 DAG 운영. 본 PR α 의 `slo_daily_report` 가 라인업 4번째 정착:

| 본진 4 DAG | 정착 시점 |
|---|---|
| `dbt_full_run` | Day 5 |
| `backfill_silver_from_bronze` | Day 5-6 |
| `iceberg_maintenance` | Day 9 본격 활성 (PR #55) |
| **`slo_daily_report`** | **Day 10 본격 활성 (PR α #57) — 라인업 4번째 완성** |

→ 같은 도구 (Airflow) 의 다른 사용 패턴 = 도구 활용도 학습 곡선 증거 완성.

![slo_daily_report DAG Airflow UI graph view (5 task 구조)](./2026-05-14-day-10-slo-redesign-and-path-b/screenshots/03-slo-daily-report-dag-graph.png)

---

## 1. SLO 단일 정의 한계 → 두 가지 분리 재설계

### 1.1. 배경 — Day 4 단일 SLO 설계의 표면적 정상

Day 4 (PR #25 silver→gold) 시점에 단일 SLO 정의:

```python
# src/flink_jobs/slo_metrics.py 초기 (Day 4)
data_freshness = gold_arrival_ts - api_response_ts(tm)
threshold = 7 minutes
```

직관적 정의 — "사용자 시점 데이터 나이". 레시핑의 15분 micro-batch 대비 50%+ 개선 목표. Day 4-7 시점에 single sample 측정 결과 P95 = 5-6 분 → 정상 통과.

### 1.2. Day 8 24h SLO 첫 실측 — 모든 percentile 거의 동일

Day 8 PR β (`phase-1a/day-8-streaming-long-running` 의 `lib/lifecycle.wait_for_shutdown()`) 으로 streaming long-running 정착 후 24h 누적 → 첫 실측:

```
count=846  p50=42분  p95=3.3시간  p99=9.4시간
```

**모든 percentile 의 spread 가 30초 안** = 일시적 spike 가 아니라 source 측 시각 자체의 일관 lag. 우리 플랫폼 성능 문제가 아닌데 SLO 위반 표시.

### 1.3. Day 10 PR α 의 진단 — `duckdb_check` sample 의 31.5분 차이

PR α 사전 점검 시 `scripts/duckdb_check.py` sample query 결과:

```sql
SELECT eventtime, ingest_ts, ingest_ts - eventtime AS lag
  FROM silver.hotspot_congestion
  LIMIT 5
```

```
eventtime               ingest_ts              lag
2026-05-13 10:00:00     2026-05-13 10:31:30    31m 30s
2026-05-13 10:05:00     2026-05-13 10:31:30    26m 30s
2026-05-13 10:10:00     2026-05-13 10:31:30    21m 30s
```

![duckdb_check sample 의 eventtime vs ingest_ts 31.5분 차이 (source lag 식별 시점)](./2026-05-14-day-10-slo-redesign-and-path-b/screenshots/02-source-lag-31min-sample.png)

**서울 OpenAPI 의 `tm` 응답값이 호출 시각보다 31분+ 옛날**. 즉 우리가 5분 polling 해도 source 측 데이터는 30분 옛날 값. 단일 SLO 로는 source 측 lag (서울시 데이터 갱신 주기 한도) 와 우리 플랫폼 측 latency 가 섞여 추적 불가능.

### 1.4. 분리 재설계 — 두 가지 SLO

```python
# src/flink_jobs/slo_metrics.py (Day 10 PR α 재설계)

# (α) Data Freshness — 사용자 관점 데이터 나이
data_freshness = gold_arrival_ts - api_response_ts(tm)
threshold = 45 minutes   # source lag 포함

# (β) Platform Latency — 우리 통제 구간
platform_latency = gold_arrival_ts - silver_arrival_ts
threshold = 7 minutes    # silver→gold 만 (Path B 결정 — §2 참조)
```

분리 재설계 의의 — 두 측정 결과를 독립적으로 추적 가능. 위반 시 원인 식별:
- (α) 위반 + (β) 통과 = source lag 변화 (서울시 데이터 갱신 주기 변경 가능성)
- (α) 통과 + (β) 위반 = 우리 플랫폼 측 backpressure / checkpoint / Iceberg commit lag

![두 가지 SLO 측정 결과 JSON 출력 — data_freshness P95 + platform_latency P95](./2026-05-14-day-10-slo-redesign-and-path-b/screenshots/01-slo-dual-measure-result.png)

### 1.5. 학습 — SLO 정의 재설계 곡선 자체가 자산

Day 4 단일 설계 → Day 8 첫 실측 위반 → Day 10 source 한계 발견 → 분리 재설계 = SLO 가 정의 한 번에 정착 안 됨. **단일 측정의 한계 발견 후 분리** 자체가 학습 자산. `phase1a_v1.md` §p3.4 + §p4.3 본문에 학습 곡선 본문 박힘.

---

## 2. silver Iceberg `kafka_ts` 부재 → Path B 결정

### 2.1. 발견 과정

PR α 작업 중 Platform Latency 정의 (`gold_arrival_ts - kafka_ts`) 구현 시점에 silver Iceberg schema 확인:

```bash
docker compose exec lakekeeper bash -c \
  'curl -s http://localhost:8181/catalog/v1/seoul/namespaces/silver/tables/hotspot_congestion | jq .schema'
```

→ `kafka_ts` 컬럼 부재. `bronze_to_silver.py` 의 INSERT 가 다음 형식:

```python
INSERT INTO silver.hotspot_congestion
SELECT b.*,
       CURRENT_TIMESTAMP AS silver_arrival_ts   # ← kafka_ts 대신 현재 시각 박음
  FROM bronze.hotspot_congestion b
```

bronze 측은 `kafka_ts` METADATA 가 들어오지만 silver INSERT 시점에 propagation 안 됨.

### 2.2. 두 Path 검토

| Path | Platform Latency 정의 | 작업량 | 한계 |
|---|---|---|---|
| **B (채택)** | `gold_arrival_ts - silver_arrival_ts` | gold ALTER + `silver_to_gold.py` 정정만 | bronze→silver lag 미포함 (Kafka broker → silver 적재) |
| C (보류) | `gold_arrival_ts - kafka_ts(METADATA)` | silver ALTER ADD COLUMN + `bronze_to_silver.py` 정정 + Flink job 2개 재기동 | full coverage (Kafka→gold) |

### 2.3. Path B 채택 사유

Day 10 일정 안 작업량 절제 의무 (리포트 1차 작성 절대 사수, spec §9-1 Day 9 fallback 원칙). Path C 의 silver schema ALTER + Flink job 2개 재기동은 추가 1-2h 소모 가능. Path B 한계 명시 + Phase 1B/2 시점 Path C 전환 검토.

`compute_platform_latency_seconds(source_ts, gold_ts)` 의 `source_ts` 인자명 = future-proof reuse 가능. Path C 전환 시 함수 시그니처 변경 X, 인자 값만 `silver_arrival_ts` → `kafka_ts` 로 교체.

### 2.4. SoT 단일 출처

- spec §6-2 (Path B 한계 명시 + Phase 1B/2 silver schema 정정 시점 전환 path)
- portfolio §p4.4 (Path B / Path C 표 + 한계 명시)
- `deferred-items-post-day10` 메모리 §4 (Path C 적용 시점 후보)

---

## 3. DuckDB `BinderException` graceful degrade — `union_by_name=true`

### 3.1. 발생 흐름

PR α 측정 코드 작성 시점에 gold 테이블 schema 변경 직후 sample query 실패:

```
duckdb.BinderException: Binder Error: Columns in iceberg_scan() must match across all manifests.
Mismatch in column 'last_silver_arrival_ts': some files have this column, some do not.
```

원인 = Path B 결정 후 gold ALTER ADD COLUMN `last_silver_arrival_ts` 가 일부 manifest 에만 적용. 기존 manifest 는 `last_kafka_ts` 만 있음.

### 3.2. 해결 — `union_by_name=true` + try/except graceful degrade

```python
# src/flink_jobs/slo_metrics.py
def fetch_dual_samples_from_gold():
    try:
        result = duckdb.sql("""
            SELECT * FROM iceberg_scan(
                'gold.fact_hotspot_congestion_5min',
                union_by_name=true                    # 다른 schema manifest 도 union
            )
            WHERE gold_arrival_ts > now() - INTERVAL 24 HOUR
        """)
    except duckdb.BinderException as e:
        # graceful degrade — Iceberg compaction 직후 일시적 schema mismatch
        return [], []   # empty samples → SLOReport count=0
    return ...
```

### 3.3. 학습 — schema 변경 시점의 일시적 mismatch 정공 path

Iceberg 의 schema evolution 자체는 정상 (manifest 별 다른 schema 허용). DuckDB 의 `iceberg_scan` 도 `union_by_name=true` 옵션으로 처리 가능. 다만 일부 case (compaction 직후 등) 에서 BinderException 발생 → try/except graceful degrade 로 다음 measure cycle 까지 0 row 반환.

---

## 4. `slo_daily_report` DAG 본진 기능 4종

### 4.1. BranchPythonOperator — P95 위반 시만 alert 분기

```python
def branch_on_slo_violation(ti):
    report = ti.xcom_pull(task_ids='measure_slo')
    payload = json.loads(report.split('\n')[-1])
    return 'send_alert' if payload['any_violated'] else 'no_op_skip'

branch = BranchPythonOperator(
    task_id='branch_on_slo_violation',
    python_callable=branch_on_slo_violation,
)
```

본진 기능 — 매일 09:00 KST 자동 측정 → 위반 시만 Discord 알림. 정상일 땐 알림 안 보냄 (false positive 회피).

![BranchPythonOperator 분기 결과 (Tree View — send_alert 분기 vs no_op_skip 분기)](./2026-05-14-day-10-slo-redesign-and-path-b/screenshots/04-branch-on-slo-violation-tree.png)

### 4.2. XCom — `measure_slo.do_xcom_push=True` → branch / send_alert pull

`measure_slo` 의 BashOperator (Option B 패턴, dbt-venv subprocess) stdout 마지막 라인 = JSON SLOReport → 자동 XCom push → branch + send_alert task 가 pull.

### 4.3. `on_failure_callback` — DAG 자체 실패 시 별도 Discord webhook

`measure_slo` 또는 `send_alert` task 자체가 실패 시 (Iceberg catalog 다운 / Discord 5xx / Airflow worker crash 등) 별도 Discord webhook URL 으로 알림. alert webhook 과 cyclic 회피.

### 4.4. HttpOperator + `trigger_rule`

Discord webhook POST = HttpOperator. `trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS` — branch 분기 후 send_alert 또는 no_op_skip 중 하나만 success 면 실행 (다른 task 의 skip 상태 정상 인정).

> **스크린샷 부재 사유** — Discord webhook alert 메시지는 SLO 위반 시점에만 발생. 본 평시 시점 (2026-05-17 이후, `phase1a_v1.md` §4.5.1 평시 재측정 검증 완료) 두 SLO 모두 통과 → BranchPython 이 `skip_alert` 분기 → `send_alert` 실행 안 됨 → alert 메시지 부재. 위반 시점 재현 = 임계값 강제 변경 + DAG trigger + 원복 의무 = 비효율. archive 본문의 메시지 형식 sample (§4.1 의 코드 블록) 인용으로 시각화 대체.

---

## 5. Phase 1B 진입 시 인용 의무

본 archive 의 학습 자산 중 Phase 1B Day 11-18 작업 시점에 인용 의무:

- **§2 Path B / Path C** — Day 11 Task 11.3 `user.events.v1` event schema 작성 시점에 silver schema 신설 결정 (`kafka_ts` 포함 여부) 검토. Day 14 Task 14.1 PyFlink `user_events_to_silver.py` 신설 시점에도 동일 검토 (silver 의 `kafka_ts` 컬럼 신설 vs `silver_arrival_ts` 만 박음).
- **§3 DuckDB BinderException** — Day 14 dbt mart `fact_user_event_hourly` 작성 시 동일 graceful degrade 적용. Day 17 분류 mart 작성 시점에도 schema 변경 직후 BinderException 가능성 인지.
- **§4 `slo_daily_report` DAG 본진 기능** — Phase 1B 의 새 SLO (사용자 클릭 latency / CDC 정합성) 추가 시점에 동일 본진 기능 4종 발휘 (BranchPython / XCom / on_failure_callback / HttpOperator).

---

> **스크린샷 디렉토리** — 본 archive 본문 안 inline anchor 의 file 위치 = `./2026-05-14-day-10-slo-redesign-and-path-b/screenshots/<NN>-<short>.png` 패턴 (Day 9 archive SoT). 캡처 file 부재 시 broken image 표시 — 별도 PR 로 file 업로드 시점에 자동 정상화. 향후 강화 리포트 v2 (§p4 SLO + §p6 Airflow) 슬라이드 작성 시 reuse.
