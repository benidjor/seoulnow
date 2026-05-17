# Day 10 Flink mini-cluster 진단 + backfill 첫 단계 완료 vs 평시 결과 차이 (53h SLO 왜곡)

> 작성: 2026-05-14 KST
> 시점: Day 10 후속 PR #60 (`phase1a_v1.md` §4.5 실측 SLO 결과 반영) 머지 완료 후 학습 자산 명문화. Phase 1B 진입 직전 baseline 정리.
> 관련 PR: #60 (Day 10 후속 — phase1a_v1.md §4.5 실측 SLO 결과 반영)
> 관련 runbook: [`docs/runbook/day10_slo.md`](../../runbook/day10_slo.md) (§ "backfill 후 평시 SLO 측정 절차")
> 동시 작성 archive (Day 10 주제 분리): [`2026-05-14-day-10-slo-redesign-and-path-b.md`](2026-05-14-day-10-slo-redesign-and-path-b.md), [`2026-05-14-day-10-pr-convention-regression.md`](2026-05-14-day-10-pr-convention-regression.md)

## 0. 진입 흐름 요약

본 archive 는 Day 10 후속 PR #60 (Phase 1A 종료 직후 phase1a_v1.md §4.5 비워둔 본문 채움) 작업 도중 발견한 학습 자산 2건 명문화:

| 학습 자산 | section |
|---|---|
| Flink mini-cluster vs docker container 오판 (`docker ps` 만으로 "Flink 멈춤" 잘못 단정) | §1 |
| backfill 첫 단계 완료 (Kafka LAG=0) vs 평시 결과 차이 (53h SLO 왜곡) | §2 |

본 두 학습 자산은 본 프로젝트의 **Flink = host process (mini-cluster) + 24h SLO window 의 backfill 영향 inherent** 라는 아키텍처 결정 의 부산물. Phase 1B Day 17/18 (Superset / Trino 기동) 시점에도 동일 영향 가능 → 인용 의무.

---

## 1. Flink mini-cluster vs docker container 오판 — `docker ps` 만으로 단정 위험

### 1.1. 발생 흐름

본 세션 (2026-05-14 02:00 KST) 진입 시점 = Phase 1A 종료 직후. 발화 = "지금 backfill 완료 시점에 phase1a_v1.md §4.5 placeholder 보강".

진단 시작 — `docker ps` 결과:

```
scp-airflow-scheduler   Up 31 hours
scp-airflow-webserver   Up 34 hours (healthy)
scp-kafka               Up 40 hours (healthy)
scp-kafka-connect       Up 40 hours (healthy)
scp-lakekeeper          Up 40 hours (healthy)
scp-minio               Up 40 hours (healthy)
scp-postgres            Up 40 hours (healthy)
```

![docker ps 결과 (Flink 컨테이너 안 보임 — 진단 함정 시점)](./2026-05-14-day-10-flink-mini-cluster-and-backfill/screenshots/01-docker-ps-no-flink.png)

→ Flink 컨테이너 안 보임. 첫 판단 = "Flink 멈춤" → 잘못 보고.

### 1.2. 진짜 진단 — host process 였음

질문 "flink는 왜 멈추었고, backfill 진행은 스킵해도 되는거야?" 후 재진단:

```bash
grep -n "flink\|profile" docker-compose.yml
# 39: # /opt/airflow/dbt/seoul 안에서 dbt CLI 호출. profiles.yml 도 host bind
# 43: # 하게 `flink_jobs.lib.duckdb_iceberg` / `platform_common.config` 를
# 300: # Day 9 전용. profile=spark 로 평상시 미기동 ...
```

→ docker-compose.yml 에 flink 서비스 정의 X. 본 프로젝트의 Flink = PyFlink local mini-cluster (host process) 로 실행되는 구조.

```bash
ps -ef | grep -E "bronze_to_silver|silver_to_gold|cdc_to_dim_place" | grep -v grep
# 501 43364 1     uv run --extra flink python -m flink_jobs.silver_to_gold
# 501 43366 43364 Python -m flink_jobs.silver_to_gold
# 501 43367 43366 /usr/bin/java ... org.apache.flink.client.python.PythonGatewayServer
# 501 43700 1     uv run --extra flink python -m flink_jobs.bronze_to_silver
# 501 43702 43700 Python -m flink_jobs.bronze_to_silver
# 501 43703 43702 /usr/bin/java ... org.apache.flink.client.python.PythonGatewayServer
```

![ps -ef | grep flink 결과 (host process 6.5h 가동, Java + Python launcher)](./2026-05-14-day-10-flink-mini-cluster-and-backfill/screenshots/02-ps-ef-flink-host-process.png)

→ Flink 2개 host process 가동 중 (19:29 + 19:36 시작 = 6.5h 가동). 단순히 docker container 안 보였을 뿐 streaming 자체는 정상.

### 1.3. 학습 — 진단 첫 step 의 함정

본 프로젝트 아키텍처 결정 = Flink = PyFlink local mini-cluster (host process) 로 1인 운영 + 24GB 제약 + Day 9 Spark 일시 기동 OOM 회피. docker compose 의 다른 service (Kafka / Airflow / Postgres / MinIO / Lakekeeper) 와 다른 lifecycle.

**진단 시 의무**:
1. `docker ps` 만으로 "Flink 멈춤" 단정 X
2. `ps -ef | grep flink` 도 동시 확인 (host process 확인)
3. Kafka consumer LAG 확인 (`flink-bronze-hotspot` consumer group) — streaming 실제 처리 여부 직접 증거

본 archive 의 학습 곡선 자산 = Phase 1B 진입 시 새 디버깅 세션에서 동일 함정 회피.

---

## 2. backfill 첫 단계 완료 vs 평시 결과 차이 — 53h SLO 왜곡

### 2.1. backfill 진행 상태 진단

Flink mini-cluster 정상 가동 확인 후 Kafka consumer LAG 직접 확인:

```bash
docker exec scp-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group flink-bronze-hotspot
```

```
flink-bronze-hotspot  partition 0  CURRENT=1315  END=1315  LAG=0
flink-bronze-hotspot  partition 2  CURRENT=660   END=660   LAG=0
```

![Kafka consumer LAG=0 결과 (flink-bronze-hotspot partition 0+2)](./2026-05-14-day-10-flink-mini-cluster-and-backfill/screenshots/03-kafka-consumer-lag-zero.png)

→ Flink 가 Kafka 의 모든 메시지 소진. **backfill 첫 단계 (Kafka → bronze 처리) 완료**.

### 2.2. SLO 측정 결과 — 53h Data Freshness 왜곡

```json
{"data_freshness":  {"count": 941, "p50": 35247, "p95": 190645, "p99": 469705, "max": 675145, "slo_violated": true},
 "platform_latency":{"count": 809, "p50": 0, "p95": 0, "p99": 0, "max": 0, "slo_violated": false}}
```

- **(β) Platform Latency P95 = 0s** — silver→gold streaming 이 5min tumbling window 안에 즉시 처리. 평시 결과로 인정. 7분 임계 통과.
- **(α) Data Freshness P95 = 52.96h (190645s)** — 비정상적으로 큰 값.

> **스크린샷 부재 사유** — 본 53h 왜곡 결과는 PR #60 작업 시점 (2026-05-14 02:06 KST) 의 backfill 직후 일회성 측정값. 3일 후 (2026-05-17) 시점에 평시 결과 (Data Freshness P95 = 33m) 도달 = 본 결과 재현 불가. 본문 sample JSON 인용으로 시각화 대체.

### 2.3. 53h 왜곡 원인 식별

24h SLO window 안 row 의 `api_response_ts` 분포 확인:

```python
duckdb.sql("""
SELECT EXTRACT(EPOCH FROM (now() - last_api_response_ts)) / 3600 AS age_hours,
       count(*) AS c
  FROM gold.fact_hotspot_congestion_5min
 WHERE gold_arrival_ts > now() - INTERVAL 24 HOUR
 GROUP BY 1 ORDER BY 1 DESC
""")
```

```
age_hours   c
187.5       45
130.5       89
52.96       412     # ← P95 위치
35.2        185
9.79        128     # ← p50 위치
```

> **스크린샷 부재 사유** — 본 age 분포 (53h+ row 412개 = P95 위치 강조) 는 PR #60 작업 시점 (2026-05-14 02:06 KST) 의 backfill 직후 일회성 결과. 본 평시 시점 (2026-05-17 이후, `phase1a_v1.md` §4.5.1 평시 재측정 검증 완료) backfill row 모두 24h SLO window 밖으로 빠짐 → 모든 row 가 0.5h 수준 → 53h 왜곡 시각화 재현 불가. 본문 sample table 인용으로 시각화 대체.

→ **24h SLO window 안 backfill 처리된 historical row 의 `api_response_ts` (= API tm) 가 53h+ 옛날인 row 412개** = P95 위치. backfill 처리 시점에 silver→gold 가 즉시 처리됐기 때문에 (β) Platform Latency 는 0s 정상, (α) Data Freshness 는 source 측 옛 데이터 영향.

### 2.4. 처리 — Path B 한계 명시 + 평시 재측정 의무

- 본 측정값을 `phase1a_v1.md` §4.5 에 박음 ((β) = 평시 결과 인정, (α) = backfill 직후 측정값 명시)
- 평시 결과 재측정 = 본 시점 + 24h 후 (`slo_daily_report` DAG 자동 첫 실행) 결과 인용한 별도 commit (Phase 1B Day 14 또는 강화 리포트 v2 Day 18 시점)
- runbook `day10_slo.md` 의 "backfill 후 평시 SLO 측정 절차" 본문에 절차 박음

### 2.5. 학습 — 24h SLO window 의 backfill 영향 inherent

streaming 일시 정지 후 재기동 패턴이 본 프로젝트의 운영 원칙 (Day 9 Spark 일시 기동 + Phase 1B Day 17/18 Superset/Trino 기동 등) → backfill 영향이 24h 동안 SLO 결과에 남음. 이 inherent 영향을 인지하고 측정 시점 / 평시 vs backfill 직후 명시 의무.

---

## 3. Phase 1B 진입 시 인용 의무

본 archive 의 학습 자산은 Phase 1B Day 11-18 작업 시점에 인용 의무:

- **§1 Flink mini-cluster 진단** — Day 11 진입 직후 Flink streaming 상태 진단 시 `docker ps` 만으로 단정 회피. `ps -ef | grep flink` + Kafka consumer LAG 동시 확인. Day 14 Task 14.1 PyFlink `user_events_to_silver.py` 새 host process 추가 시점에 동일 진단 패턴 의무.
- **§2 backfill 영향** — Day 17 Superset / Day 18 Trino 기동 시점에 `airflow-scheduler` 일시 stop 패턴 reuse → streaming 일시 정지 가능성 → 24h SLO window 영향 명시 의무. Day 14 또는 Day 18 시점 평시 SLO 재측정 commit 으로 Phase 1A v1 §4.5 갱신 의무.

---

> **스크린샷 디렉토리** — 본 archive 본문 안 inline anchor 의 file 위치 = `./2026-05-14-day-10-flink-mini-cluster-and-backfill/screenshots/<NN>-<short>.png` 패턴 (Day 9 archive SoT). 캡처 file 부재 시 broken image 표시 — 별도 PR 로 file 업로드 시점에 자동 정상화. 향후 강화 리포트 v2 (§p4 SLO + §p10 운영 트러블슈팅) 슬라이드 작성 시 reuse.
