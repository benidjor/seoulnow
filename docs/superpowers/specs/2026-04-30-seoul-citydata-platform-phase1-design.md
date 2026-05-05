# Phase 1 설계 — 서울 실시간 지역 혼잡도 데이터 플랫폼

## 0. 메타

- 작성일: 2026-04-30
- 베이스 문서: 프로젝트 루트 `CLAUDE.md`
- 본 문서 위치: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`
- 상태: 사용자 리뷰 대기 → 승인 후 `writing-plans` 단계로 이행
- 본 문서는 **Phase 1 (1A + 1B, 14일)** 만 다룸. Phase 2(8주)는 의도적으로 비목표로 분리.

## 1. 목표와 성공 기준

### 1-1. 한 줄 목표

> 서울 공공 실시간 데이터 + Postgres CDC + 익명 사용자 행동 로그를 **Kafka 메시지 버스로 통합**하고, **PyFlink streaming + Spark batch + Iceberg(Lakekeeper) + dbt + GitHub Actions** 으로 처리·검증하는 플랫폼을 14일에 구축한다. 동시에 **익명 사용자가 실제 사용 가능한 작은 실서비스**(공개 도메인 + 동네 북마크 + Web Push 알림)를 같이 띄워, "1번 포트폴리오의 micro-batch / 시뮬레이션 데이터 / 미해결 이슈 / 도메인 편중" 약점을 한 번에 보강한다.

### 1-2. 성공 기준 (포트폴리오 약점 보강 매핑)

| 사용자 목표 | 보강 대상 약점 | 본 프로젝트의 해법 |
|---|---|---|
| Kafka 실시간 경험 | 1번이 사실상 micro-batch (Kafka는 적재 통로) | Kafka → PyFlink streaming → Iceberg, **데이터 신선도 P95 < 7분** SLO 측정 |
| 작게나마 실서비스 | 데이터가 둘 다 시뮬레이션/Kaggle | Phase 1B 익명 실서비스 + 본인이 만든 그 서비스의 행동 로그 토픽 |
| 비용 최소화 | 1번의 75% 절감 서사 연속 | Oracle Cloud Always Free + Cloudflare 무료 + 공공 무료 API = 월 $0~$2 |
| (보너스) 도메인 다변화 | 1·2번 둘 다 유저 행동 도메인 | 공공·공간 데이터 + CDC + 행동 로그 = 3종 이종 소스 |
| (보너스) 미해결 closure | 1번의 멱등성 / Compaction "예정" | Day 9 Spark MERGE INTO + `rewrite_data_files` 로 직접 해결 |

### 1-3. 비목표 (Out of Scope, Phase 2로 명시적 미룸)

- Trino single-node, Terraform IaC, Grafana Cloud, Great Expectations, Apache Superset
- 회원 가입 / 로그인 / 본격 사용자 인증 (Phase 1B는 익명 쿠키 기반만)
- Google Places API, UGC 별점, 버스 위치 토픽
- 본인 운영 실서비스 외부 공개·홍보 (OKKY/Reddit/Disquiet)
- Databricks 미니 프로젝트 (Day 14 이후 별도 결정)

## 2. 사용자 배경 / 동기 / 제약

- 신입 데이터 엔지니어 지원 중 (0~3년차). 서류 탈락 보강 목적.
- 기존 포트폴리오 2건: 레시핑(2025.07~10), E-commerce(2025.02~04).
- **사용자가 이번 프로젝트로 명시한 3가지 목표**:
  1. Kafka 실시간 데이터 경험을 더 쌓는다.
  2. 작게나마 실서비스를 직접 만들어본다.
  3. 비용을 최대한 적게 쓴다.
- 제약: 14일 안에 완료, 월 운영비 $0~$2, 합법 데이터 출처만 사용.

### 2-1. 기존 포트폴리오 약점 진단 (면접관 시각)

- **약점 A — "준실시간 라벨"이지만 사실상 micro-batch**: 1번은 Kafka가 적재 통로(Connect → S3)였고 처리는 15분 주기 Spark batch. streaming 처리 경험은 부재.
- **약점 B — 데이터가 둘 다 가짜**: 1번 시뮬레이터, 2번 Kaggle. "실데이터 더러움을 다뤄본 적 없는 사람"으로 분류될 위험.
- **약점 C — 도메인 편중**: 1·2번 모두 유저 행동 분석.
- **약점 D — 미해결 이슈가 "예정"으로 끝남**: 멱등성(Dynamic Partition Overwrite), Small File(Compaction) 모두 "도입 예정"으로 1번 페이지 9·11에 남아있음.

## 3. 의사결정 요약

| 영역 | 결정 |
|---|---|
| 메시징 | **Kafka KRaft single-node** (Redpanda 아님, ZooKeeper 없음) |
| 스트림 처리 | **PyFlink** 메인 |
| 배치 처리 | **Spark batch** 보조 (Day 9 Iceberg MERGE INTO 멱등성 검증) |
| 카탈로그 | **Lakekeeper REST Catalog** (fallback: JdbcCatalog) |
| 변환 | **dbt-core** (Silver→Gold 일부) |
| 워크플로우 오케스트레이션 | **Airflow (LocalExecutor + SQLite metadata)** — 본진 4 DAG (Day 5~10). 1번 cron 대용 사용 패턴 → 본진 기능(DAG 의존성·SLA·Branch·dynamic task mapping) 직접 운영 |
| 쿼리 | **DuckDB** (Iceberg 직접 쿼리) |
| CI/CD | **GitHub Actions** (dbt + PyFlink lint/test) |
| OLTP | **Postgres** (`places` 마스터 + Day 11 fallback용 events_inbox) |
| CDC | **Debezium** (Postgres → Kafka) |
| 프론트엔드 | **Next.js + Cloudflare Pages** (Phase 1A) |
| Edge API | **Cloudflare Pages Functions** + Oracle Cloud의 작은 **HTTP receiver** → Kafka (REST Proxy 패턴, Phase 1B) |
| 사용자 메타 저장소 | **Cloudflare D1** (북마크, push subscription) |
| 알림 | **Web Push** (VAPID) + Cloudflare Workers Cron |
| 비용 | **월 $0~$2** (Oracle Cloud Always Free + Cloudflare 무료) |
| 일정 | **14일** = Phase 1A (Day 1~10) + Phase 1B (Day 11~14) |
| 포트폴리오 분량 | 1A 단독 5~6p / 1A+1B 통합 8~10p |

## 4. 아키텍처

### 4-1. 컴포넌트 다이어그램 (이종 소스 4 → 1 버스 → 컨슈머 2)

```
[서울 도시데이터 API]  [지하철 도착정보 API]  [Postgres places]  [Cloudflare Edge API]
        ↓                    ↓                    ↓                    ↓
   hotspot.v1           subway.v1          places.cdc.v1        user.events.v1
                                                                      ↓
                                                            [HTTP receiver
                                                             on Oracle Cloud]
        └────────────────┴──────────────────┴──────────────────────┘
                                   ↓
                         Kafka (KRaft single-node)
                                   ↓
                ┌──────────────────┴──────────────────┐
                ↓                                     ↓
         PyFlink streaming                    Cloudflare Workers Cron
         (Bronze → Silver → Gold)             (alert sender, P1B)
                ↓                                     ↑
         Iceberg + Lakekeeper REST Catalog ───────────┘
                ↓                                     (DuckDB로 북마크 동네 한가도 조회)
         DuckDB / dbt
                ↓
         Next.js + Mapbox/Leaflet (Cloudflare Pages)
```

### 4-2. 데이터 레이어 (Medallion)

- **Bronze**: raw 이벤트 (스키마 검증만)
- **Silver**: 정규화, 행정구역/핫스팟 region join, 좌표 정제, 중복 제거
- **Gold**:
  - `fact_hotspot_congestion_5min`
  - `dim_region` (자치구 / 행정동 / 핫스팟)
  - `dim_place` (공공 인허가 단일 출처, SCD Type 2 골격)
  - `fact_place_status_hourly` (영업·혼잡)
  - `fact_user_event` (Phase 1B)

### 4-3. Kafka 토픽 명세

| 토픽 | 발행 주체 | 갱신 주기 | 시간당 건수 (대략) | 컨슈머 |
|---|---|---|---|---|
| `seoul.hotspot.congestion.v1` | Python producer (서울 도시데이터 API 폴링) | 5분 | ~1,440 | PyFlink |
| `seoul.transit.subway.v1` | Python producer (지하철 도착정보 API 폴링) | 30초~1분 | ~8,000~16,000 (폴링 주기 의존) | PyFlink |
| `place.master.cdc.v1` | Debezium (Postgres `places`) | 변경 시 | 거의 없음 | PyFlink |
| `user.events.v1` (Phase 1B) | Edge API → HTTP receiver → producer | 익명 클릭 | 일 100~1,000 | PyFlink + Workers Cron |

## 5. 스택 결정과 정당화

### 5-1. Kafka KRaft single-node — 정당화

- **이종 소스 4종 → 1개 메시지 버스 통합** (조건: input 이종성)
- **컨슈머 2개 이상** (PyFlink streaming + Workers Cron 알림 발신) (조건: fan-out)
- **Producer-Consumer 디커플링**: 공공 producer / Debezium / Edge API / Flink 가 각자 독립 운영
- **Replay 가능**: Iceberg 적재 실패 시 Flink job 재실행으로 Bronze 재구성
- **Flink + Kafka exactly-once**: streaming 정합성의 표준
- **단일 노드 + KRaft**: ZooKeeper 불필요, 메모리 절감, 1인 운영
- **single-node 결정 사유 (3-node 안 쓴 이유)**: 1번 포트폴리오에서 Kafka 3-node + Connect 2-node를 EC2 위에 운영해 production-like cluster 경험을 이미 했음. 본 프로젝트는 Oracle Cloud Free Tier 24GB + 1인 운영 + **Day 9 Spark batch 일시 기동 시 OOM 위험**을 회피하기 위해 의식적으로 single-node 채택. 데이터 손실 허용도 측면에서도 공공 API는 5분 polling 자연 복구 + CDC는 Debezium offset 보장 → HA 비즈니스 정당화 약함. **SPOF는 limitation으로 솔직히 인정.** Production SLA 환경이라면 3-node + RF=3 + ISR=2 가 맞음.
- **솔직한 카운터 응답 (Kafka 자체 정당화)**: "Phase 1A 단독이라면 Polling + Postgres로도 충분했지만, Phase 1B에서 토픽 4종 + 컨슈머 2개로 확장하면서 메시지 버스로 통합하는 게 합리적이 됐습니다. 단일 토픽이었으면 Kafka 안 썼을 겁니다."

### 5-2. PyFlink — 메인 스트리밍 엔진

- 1번 포트폴리오에 streaming 처리 자체가 없었음을 직시 → 본 프로젝트로 채움
- 신입 풀에서 Flink는 희소한 차별화 포인트
- fallback: 막히면 Spark Structured Streaming 으로 전환 (§9 트리거 참고)

### 5-3. Spark batch — Day 9 멱등성 검증 보조

- 1번에서 익힌 Spark·Iceberg를 본 프로젝트의 Day 9 **Iceberg MERGE INTO 멱등성 검증** 잡에 재활용 → 1번 페이지 9의 "Dynamic Partition Overwrite **예정**" 미해결 이슈를 **closure**
- Spark는 **Day 9 일시 기동만**, 메모리 충돌 회피
- 엔진을 **용도별로 분리** (streaming = Flink, batch = Spark) 라는 명확한 의사결정 서사

### 5-4. Lakekeeper REST Catalog

- 1번의 Hive Metastore 대비 차별화 + 메모리 절감
- fallback: 디버깅 2시간 초과 시 JdbcCatalog (Postgres backed)

### 5-5. dbt-core + GitHub Actions

- 1·2번 모두 부재였던 **변환 레이어 코드 관리 + CI/CD for Data**
- dbt tests 5~10개 + dbt-docs lineage

### 5-6. Cloudflare 스택 (Phase 1B)

- **Pages Functions**: Edge API. 익명 ID 쿠키 발급 + 이벤트 수집 endpoint.
- **D1 (SQLite)**: 사용자 메타만 (`bookmarks`, `push_subscriptions`). **행동 로그는 절대 D1에 넣지 않음.**
- **Workers Cron**: Iceberg를 DuckDB로 주기 쿼리 → 북마크 동네 한가해지면 Web Push 발신
- **Tunnel**: Oracle Cloud VM의 HTTP receiver 외부 노출

### 5-7. Databricks 검토 결과 — 미도입

- Community Edition은 streaming 24/7 운영 불가, 외부 Kafka 연결 제한
- Free Trial 14일 후 과금 발생 → 월 $0 목표 위반
- 별도 클라우드(AWS/Azure/GCP) 필요 → Oracle Cloud 단일 운영 컨셉 깨짐
- 1번 Spark + 본 프로젝트 Spark batch + Databricks(Spark) → "Spark 세 번"의 중복감
- **결론**: Phase 1에 미도입. Day 14 이후 별도 1주짜리 미니 프로젝트로 진행할지 추후 결정.

### 5-8. Airflow — 워크플로우 오케스트레이터 본진 사용

#### 도입 배경

- 1번 포트폴리오에서 Airflow 를 15분 batch trigger 로만 사용 (cron 대용 수준). DAG 의존성 / retry policy / SLA / 백필 / Branch 등 본진 기능 미사용 → **CLAUDE.md §2 약점 #11 로 정식 인지**
- DE JD 빈출 키워드: "Airflow 등 워크플로우 관리 도구 운영 경험" — 직접 대응 필요
- **stated 목표 ① Kafka 실시간 경험과 충돌하지 않음** — Flink streaming 자리는 그대로 유지

#### 사용 자리 — 본진 4 DAG (Day 5~10)

각 DAG 가 의도적으로 다른 본진 기능을 발휘. cron 대용 1번 패턴과 명확히 구분.

| DAG | 도입 Day | schedule | 본진 기능 발휘 |
|---|---|---|---|
| **`dbt_full_run`** | Day 5 | `@daily 02:00 KST` | **TaskGroup** (staging / marts 분리) + **task 의존성** (staging test 실패 시 marts 자동 skip) + **retry policy** (`retries=2, retry_exponential_backoff=True`) + **SLA** (30분) + **on_failure_callback** (Discord webhook) |
| **`iceberg_maintenance`** | Day 9 | `@daily 03:00 KST` | **병렬 실행** (rewrite_data_files 3개 동시) + **`max_active_tis_per_dag=3`** (Spark OOM 방지) + **XCom** (before/after 메트릭 비교) + **on_success_callback** (압축률 자동 보고) |
| **`backfill_silver_from_bronze`** | Day 5~6 buffer | 수동 trigger | **Dynamic task mapping** (`expand()` 로 백필 시간 partition 자동 task 생성, Airflow 2.3+) + **Params** (UI 에서 날짜 범위 입력) + **dry_run 모드** + **멱등 MERGE INTO** (재실행 안전) |
| **`slo_daily_report`** | Day 10 | `@daily 09:00 KST` | **BranchPythonOperator** (P95 > 7분일 때만 alert, false positive 방지) + **XCom** (metric → report → branch) + **시계열 archive** (`fact_slo_daily` 자체가 SLO 추세 데이터셋) |

→ **카운트 기준 1번 0개 → 본 프로젝트 12개** Airflow 본진 기능 발휘.

#### 사용하지 않는 자리 — 3계층 분리 원칙

- **streaming = Flink** (long-running job, JobManager 가 lifecycle 관리)
- **5분/30초 polling = cron / systemd timer** (Airflow 부적합)
- **PR 트리거 lint·test = GitHub Actions**
- **Web Push 알림 발신 = Cloudflare Workers Cron** (Cloudflare 측에 있어야 함)
- **batch ops 만 Airflow** — DAG 의존성·재시도·SLA·백필이 진짜 필요한 자리

#### 메모리 mitigation (24GB 제약)

| 설정 | 효과 |
|---|---|
| LocalExecutor + SQLite metadata DB | Postgres meta / Celery / Redis 미사용 → ~700MB |
| 야간(02~05시) 실행 | streaming peak 회피 |
| Day 9 Spark 기동 직전 `airflow-scheduler` 일시 stop | 700MB 회수, Day 9 OOM 방지 |
| `parsing_processes=1, dag_dir_list_interval=300` | scheduler 메모리 절감 |

추정 메모리 budget: 상시 ~9.8GB / Day 9 Spark 동시 (scheduler stop 후) 15.8~17.8GB / 80% 임계 = 19.2GB 안.

#### 일정 영향

추가 작업량 ≈ 1.5일 (Day 5 셋업 0.5일 + Day 5~6 buffer DAG 1일). Day 5 fallback 트리거 추가 (§9-1).

#### 면접 카운터 답변

§8-2 표 참조 ("왜 Airflow 또 쓰나요?", "1번이랑 뭐가 다른가요?" 답변에 통합).

## 6. Phase 1A — Data Platform Core (Day 1~10)

### 6-1. 일정

| Day | 작업 | 산출물 |
|---|---|---|
| 1 | Oracle Cloud ARM VM + docker-compose (Kafka KRaft, Postgres, MinIO, Lakekeeper) | 인프라 기동 |
| 2 | 도시데이터 + 지하철 도착정보 producer → Kafka 토픽 2개 | Bronze 토픽 흐름 |
| 3 | PyFlink Bronze → Silver (스키마 정규화, 핫스팟 region 매핑) → Iceberg via Lakekeeper | Silver 테이블 |
| 4 | PyFlink Silver → Gold (`fact_hotspot_congestion_5min`) + DuckDB 검증 + **데이터 신선도 SLO 측정 코드** | Gold + SLO 메트릭 |
| 5 | dbt-core 도입, Silver→Gold 일부 dbt 이관, dbt tests 5~10개, **Airflow 셋업 (LocalExecutor + SQLite, ~700MB)** + **`dbt_full_run` DAG** (TaskGroup + 의존성 + SLA + on_failure_callback), GitHub Actions CI. **Airflow 도입 직후 free 메모리 측정 (80% = 19.2GB 임계 확인)** | CI green + DQ 리포트 + Airflow 첫 DAG |
| 5~6 buffer | **`backfill_silver_from_bronze` DAG** (dynamic task mapping + Params + 멱등 MERGE INTO + dry_run 모드) + **`iceberg_maintenance` DAG 골격** | 백필 + 유지보수 DAG |
| 6 | Postgres `places` + Debezium connector, `place.master.cdc.v1` → PyFlink → `dim_place` (SCD Type 2 골격) | CDC 동작 데모 |
| 7 | Next.js 단일 페이지 + Mapbox/Leaflet, 핫스팟 120개 혼잡도 색상, Cloudflare Pages 배포 | 도메인 붙은 데모 |
| 8 | "지금 한가하고 영업 중인 카페" 데모 화면 1개 (영업시간은 공공 인허가 정적 데이터) | 사용자 화면 |
| 9 | **Spark batch** Iceberg MERGE INTO 멱등성 검증 + Compaction(`rewrite_data_files`) + 비용 측정. **`iceberg_maintenance` DAG 본격 운영** (Spark MERGE INTO 를 DAG 로 트리거 + XCom before/after 메트릭 + max_active_tis_per_dag=3) | 1번 미해결 이슈 closure 증거 + Airflow 본진 활용 |
| 10 | 아키텍처 다이어그램 + README + **`slo_daily_report` DAG** (BranchPythonOperator) + **포트폴리오 1차 작성 (Phase 1A 단독, 6~7p — Airflow 페이지 포함)** | **체크포인트 1: 포트폴리오 제출 가능** |

### 6-2. 데이터 신선도 SLO 정의

- **정의**: 공공 도시데이터 API 응답 `tm` 시각 → Iceberg Gold 테이블 도달까지의 wall-clock 시간
- **목표**: P95 < 7분 (1번의 15분 대비 50%+ 개선)
- **측정 방식**: producer 가 Kafka 메시지에 `api_response_ts` 헤더 첨부 → Flink Gold sink 가 `gold_arrival_ts` 기록 → 두 값 차이의 분포를 자체 Python 스크립트로 일일 리포트

### 6-3. Phase 1A 단독 포트폴리오 (6~7p)

1. 표지 + 핵심 성과 (도메인 URL, SLO P95, 비용)
2. 아키텍처 다이어그램 (이종 소스 4 → 버스 → 컨슈머 패턴, Airflow 3계층 분리 원칙 포함)
3. 1번과의 차별화 표 + 학습 곡선 서사 (Airflow 사용 패턴 진화 포함)
4. 데이터 신선도 SLO 측정 결과 + dbt tests
5. 트러블슈팅: Spark MERGE INTO 멱등성 + Compaction (1번 미해결 closure)
6. **Airflow 본진 4 DAG 운영** (DAG 의존성 / dynamic task mapping 백필 / BranchPythonOperator / 메모리 mitigation)
7. 운영 비용 분석 + Phase 1B/2 로드맵

## 7. Phase 1B — 익명 실서비스 통합 (Day 11~14)

### 7-1. 일정

| Day | 작업 | 산출물 |
|---|---|---|
| 11 | **익명 사용자 행동 producer** — 브라우저 → Cloudflare Pages Functions(Edge API) → HTTPS → Oracle Cloud의 HTTP receiver → Kafka `user.events.v1`. 익명 ID는 쿠키로 발급. | 행동 토픽 흐름 |
| 12 | **D1 + 북마크** — D1에 `bookmarks(anon_id, region_id, created_at)` 테이블, Next.js 북마크 UI | 사용자 기능 1개 |
| 13 | **Web Push 알림** — 서비스워커 + push subscription을 D1에 저장 + Workers Cron이 DuckDB로 Iceberg 쿼리 → "북마크된 동네가 한가해짐" 감지 시 푸시 발신 | 알림 동작 |
| 14 | `user.events.v1` 을 PyFlink로 처리해 Iceberg `fact_user_event` 적재 + **포트폴리오 강화 버전 작성 (1A+1B, 8~10p)** | **체크포인트 2: 강화 버전 제출** |

### 7-2. Edge API → Kafka REST Proxy 패턴 (처음부터 채택)

Cloudflare Pages Functions / Workers는 TCP 직접 연결이 제한적이므로, **처음부터** 다음 경로로 설계:

```
[브라우저]
  ↓ POST /v1/events {events:[...]}
[Cloudflare Pages Functions (Edge API)]
  ↓ HTTPS POST /v1/events  (Cloudflare Tunnel 경유)
[Oracle Cloud VM의 HTTP receiver (FastAPI 등)]
  ↓ kafka producer
[Kafka]
```

- Edge API는 Cloudflare 토큰 검증 + 익명 ID 쿠키 검증 후 그대로 전달
- HTTP receiver는 Tunnel 내부에서만 노출 (직접 IP 노출 안 함)
- **직접 TCP from Workers 시도하지 않음** (디버깅 비용 회피)

### 7-3. 익명 ID 거버넌스

- 첫 방문 시 Edge API가 무작위 UUID 쿠키 발급 (`anon_id`)
- 1년 만료
- IP 저장하지 않음, IP는 로그에만 (Cloudflare 자체 로그)
- `/privacy` 페이지에 1줄짜리 처리방침 명시: "익명 쿠키만 수집, 로그인·이메일·IP 영구저장 없음"
- D1에는 사용자 메타만, 행동 로그는 Kafka → Iceberg

### 7-4. Web Push 알림 + Fallback

- 정상 경로: 서비스워커 등록 + VAPID 키로 push subscription 생성 → D1 저장 → Workers Cron(5분 주기) → DuckDB로 Iceberg 쿼리 → "북마크된 동네 혼잡도가 임계값 미만 + 영업 중" 조건 충족 시 web-push 라이브러리로 발신
- **Day 13 fallback**: Web Push 단계에서 막히면 **메인 지도에 "내 북마크 동네 현황" 위젯**을 띄워 한가해지면 화면에 배지 표시. 푸시는 Phase 2로 미룸.

### 7-5. 1A+1B 통합 포트폴리오 (9~11p)

위 Phase 1A 7p + 다음 추가:
8. 익명 실서비스 아키텍처 (Edge API + REST Proxy + D1 + Web Push)
9. `user.events.v1` 처리 + 익명 ID 거버넌스
10. Phase 1B 트러블슈팅 (Web Push / D1 / Edge API → Kafka 등)
11. 본인 운영 실서비스 + 그 서비스의 데이터 플랫폼 통합 어필 + Phase 2 로드맵

## 8. 차별화 서사 (면접 어필)

### 8-1. Top 4 메시지

1. **Streaming 진정성** — 1번이 사실상 micro-batch였음을 직시하고, 본 프로젝트에선 Kafka → PyFlink → Iceberg 의 진짜 streaming 파이프라인 + 데이터 신선도 P95 < 7분 SLO 측정.
2. **미해결 closure** — 1번 페이지 9·11의 "Dynamic Partition Overwrite 예정 / Compaction 도입 예정" 두 미해결 이슈를 본 프로젝트 Day 9에서 Spark MERGE INTO + `rewrite_data_files` 로 직접 해결.
3. **실데이터 + 도메인 확장** — 시뮬레이션/Kaggle 한계를 공공 실시간 API + CDC + **본인이 만든 익명 실서비스의 행동 로그** 로 메우고, 유저 행동 도메인에서 공공·공간 도메인으로 확장.
4. **Airflow 본진 사용 패턴 진화** — 1번에서 cron 대용으로만 썼던 Airflow 를 본 프로젝트에선 본진 4 DAG (DAG 의존성 그래프 / dynamic task mapping 백필 / BranchPythonOperator 분기 / SLA / TaskGroup / XCom / on_failure_callback) 으로 직접 운영. **streaming = Flink, polling = cron, batch ops = Airflow** 3계층 분리 원칙. **도구 활용도의 학습 곡선 자체가 서사**.

### 8-2. 면접 카운터 응답

| 면접관 카운터 | 응답 |
|---|---|
| "1번이랑 뭐가 다른가요?" | streaming 엔진(Flink), CDC(Debezium), 변환 레이어(dbt), CI(GitHub Actions), 카탈로그(Lakekeeper), 쿼리 엔진(DuckDB), **워크플로우 사용 패턴(Airflow cron 대용 → 본진 4 DAG)** — **7개 영역이 다름**. |
| "왜 Kafka를 또 썼나요?" | 1번에선 Connect 적재 통로로만 썼고, 본 프로젝트는 streaming 처리 엔진 입력. **같은 도구의 다른 사용 패턴.** |
| "왜 Flink인가요? Spark Structured Streaming은요?" | 1번에서 Spark batch를 익혔고, streaming은 새로 배우는 게 학습 곡선상 자연스러움. 신입 풀에서 Flink는 희소. |
| "Spark는 왜 또 끼어 있나요?" | 1번에서 익힌 도구로 1번의 미해결 이슈를 해결한 일관성. **엔진을 용도별로 분리** (streaming = Flink, batch = Spark). |
| "Kafka 정말 필요했나요?" | Phase 1A 단독이면 Polling + Postgres로도 가능했음. **이종 소스 4종 통합 + 컨슈머 2개 (Flink + Cron) + 학습 목표** 가 정당화 사유. **단일 토픽이었으면 안 썼을 겁니다.** |
| "왜 3-node 가 아닌 single-node 인가요?" | 1번에서 Kafka 3-node + Connect 2-node를 EC2 위에 운영해봤음. 본 프로젝트는 Oracle Cloud 24GB + 1인 운영 + Spark batch 일시 기동을 고려했을 때 3-node 면 Day 9 OOM 위험. 데이터 손실 허용도도 공공 API 5분 polling 자연 복구 + CDC Debezium offset 보장으로 HA 비즈니스 정당화 약함. **KRaft single-node 로 의식적 단순화 + SPOF 는 limitation 으로 인정.** Production SLA 였다면 3-node + RF=3 + ISR=2. |
| "Airflow 또 쓰셨네요?" | 1번에선 15분 batch trigger (cron 대용) 로만 썼고, 사실 그 자리는 systemd timer / cron 으로 충분했습니다. 본 프로젝트에선 의식적으로 본진 기능을 다뤘습니다 — **`dbt_full_run`** 에서 staging→marts TaskGroup + test 실패 시 marts 자동 skip, **`iceberg_maintenance`** 에서 병렬 Spark submit + XCom 메트릭 비교, **`backfill_silver_from_bronze`** 에서 dynamic task mapping + 멱등 MERGE INTO, **`slo_daily_report`** 에서 BranchPythonOperator 분기. **같은 도구를 cron 대용에서 본진 4 DAG 로 활용도 진화.** |
| "왜 Dagster / Prefect 안 쓰셨어요?" | 검토했습니다. Dagster 가 dbt asset 통합 측면에서 더 모던한 건 맞지만, **본 프로젝트의 1차 학습 목표가 "Airflow 본진 운영"** 이었습니다 (1번에서 cron 대용으로만 써서 깊이가 부족했음). Dagster 는 Phase 2 W4~5 에 dbt asset 기반 orchestration 으로 도입할 계획이고, 그땐 도구 활용 깊이가 다른 단계가 됩니다. |
| "왜 batch ops 만 Airflow 인가요?" | **streaming = Flink, polling = cron, batch ops = Airflow** 3계층 분리 원칙입니다. Airflow 는 long-running streaming job 의 lifecycle 을 관리하기에 부적합하고, 5분 polling 도 cron 으로 충분합니다. **Airflow 가 진짜 풀어야 할 문제 (DAG 의존성·재시도·SLA·백필) 만** Airflow 에 줬습니다. |
| "비용은요?" | 월 $0~$2. 1번의 75% 절감 서사의 연속. |
| "실데이터 다뤄봤나요?" | Phase 1B 익명 실서비스의 행동 로그. **본인이 그 데이터의 producer + consumer.** |
| "Claude Code 활용은?" | 도구로서 적극 활용하되, 의사결정·아키텍처·SLO 정의·trade-off 분석은 본인이 주도. 브레인스토밍 시점부터 본인 판단 명시. |

## 9. 리스크 / Fallback (CLAUDE.md §13 보강)

### 9-1. Phase 1A 트리거 (Day 0~10)

| Day | 트리거 | Fallback |
|---|---|---|
| 0 | 서울 OpenAPI 키 미신청 | 즉시 신청 + Day 1~2 fixture |
| 1 | Kafka KRaft 셋업 4시간 초과 | Redpanda single-node 우회. 차별화 톤 다운. |
| 3 | PyFlink Bronze→Silver 안 됨 | Spark Structured Streaming 전환. Option α 차별화 약화는 솔직히 기술. |
| 진행 중 | Lakekeeper 디버깅 2시간 초과 | JdbcCatalog (Postgres backed) 우회. 도입 시도 + 마주친 이슈를 포트폴리오에 솔직히 기술. |
| 5 | Airflow 셋업 4시간 초과 (LocalExecutor / SQLite metadata 이슈, DAG 파싱 실패 등) | 1단계 fallback: `dbt_full_run` 만 GitHub Actions schedule + cron 으로 우회 후 Day 5~6 buffer 에 재시도. 2단계 fallback (Day 6에도 실패): Airflow 미도입으로 결정 + 약점 #11 미커버를 포트폴리오에 솔직히 기술. |
| 5 | Airflow 도입 후 메모리 80% (19.2GB) 초과 | 즉시 `airflow-scheduler` 일시 stop + DAG 야간 실행으로 재배치. 그래도 초과 시 `dbt_full_run` 만 유지하고 나머지 3 DAG 는 cron 으로 fallback. |
| 6 | Debezium 셋업 4시간 초과 | Postgres outbox + 폴링 producer로 단순화. CDC 어필 일부만 유지. |
| 7 | Next.js + Cloudflare Pages 배포 안 됨 | Streamlit + ngrok. 포트폴리오는 스크린샷 + 코드. |
| 9 | Spark 셋업 안 됨 | PyIceberg 또는 dbt MERGE 매크로로 멱등성 검증 (`iceberg_maintenance` DAG 의 SparkSubmitOperator → BashOperator 로 교체). |
| 9 | Day 9 Spark 기동 시 OOM | `airflow-scheduler` 즉시 stop (700MB 회수) → Spark 작업 완료 후 재기동. |
| 9 | 시간 부족 | Day 10 포트폴리오 작성 절대 사수. |

### 9-2. Phase 1B 트리거 (Day 11~14)

| Day | 트리거 | Fallback |
|---|---|---|
| 11 | Edge API → Kafka 전송 안 됨 | 2단 우회: (a) Postgres `events_inbox` + Debezium 으로 우회, (b) HTTP receiver 위치 변경. |
| 12 | D1 연동 막힘 | Postgres `bookmarks` 테이블로 대체. Cloudflare Tunnel로 Pages Functions가 접근. |
| 13 | Web Push 안 됨 | 모니터링 페이지로 대체 — 메인 지도에 "내 북마크 동네 현황" 위젯 + 한가해지면 배지. |
| 14 | 시간 부족 | Phase 1A 5~6p만 제출. Phase 1B는 "Day 11~13까지 완료, 14일에 정리 중"으로 솔직히 명시. |

### 9-3. 일반 리스크

- **메모리 충돌**: Kafka KRaft + Flink + **Airflow (LocalExecutor + SQLite, ~700MB)** 상시, Spark는 Day 9 일시 기동. Day 1에 free 메모리 측정 스크립트 작성, **Day 5 Airflow 도입 직후 재측정**. 80% (19.2GB) 초과 시 알림. Airflow DAG 들은 야간(02~05시) 실행으로 streaming peak 회피, **Day 9 Spark 기동 직전 `docker compose stop airflow-scheduler` 로 일시 회수** 가 운영 원칙.
- **Edge API 토큰 보안**: `user.events.v1` 발행 권한이 외부 노출되면 spam 가능. Edge API에서 발행하는 토큰은 secret으로 관리, Kafka는 Tunnel 내부에서만 수신.
- **익명 ID 거버넌스**: `/privacy` 1줄 처리방침 페이지 필수.
- **VAPID 키 관리**: GitHub commit 금지. Cloudflare Workers secret + Oracle Cloud `.env`.
- **D1 5GB 한도**: D1엔 사용자 메타만. 행동 로그는 절대 D1에 넣지 않음.
- **Oracle Cloud Always Free 회수**: 90일 미사용 회수 → PAYG 업그레이드(과금 없음)로 회피.
- **공공 API rate limit**: 캐싱 + 백오프.
- **카카오/네이버 스크래핑**: 절대 금지. 가게 정보는 공공 인허가 + (Phase 2) Google Places + UGC.

## 10. Day 0 사전 준비 체크리스트

| # | 항목 | 비용 | 비고 |
|---|---|---|---|
| 1 | 서울 열린데이터 광장 API 키 신청 | $0 | ~1 영업일. 즉시 신청. |
| 2 | 서울 열린데이터광장 실시간 지하철 도착정보 API 키 | $0 | ~1~3 영업일. |
| 3 | 공공데이터 인허가 정보(일반음식점·휴게음식점) 다운로드 | $0 | 즉시. Day 6 정적 적재용. |
| 4 | Oracle Cloud 계정 + PAYG 업그레이드 | $0 | ARM Ampere A1 가용 지역 확인. |
| 5 | Cloudflare 계정 (Pages + D1 + Workers + Tunnel 활성화) | $0 | |
| 6 | GitHub repo 생성 (public 추천) | $0 | .gitignore + README 골격. |
| 7 | 도메인 결정 | $0 또는 ~$10/년 | 기본 `*.pages.dev` 무료. |
| 8 | Docker Desktop / colima | $0 | latest. |
| 9 | Python 3.11+ (uv 또는 poetry) | $0 | |
| 10 | Node 20+ (pnpm 권장) | $0 | |
| 11 | VAPID 키 페어 생성 | $0 | `npx web-push generate-vapid-keys`. Day 13용. |
| 12 | 1번 docker-compose.yml 사본 위치 메모 | - | Day 1 재활용. |
| 13 | 1번 Spark MERGE INTO 코드 위치 메모 | - | Day 9 재활용. |
| 14 | 신규 GitHub repo 이름 결정 | - | 예: `seoul-realtime-platform`. |
| 15 | 도메인 서브 이름 결정 | - | 예: `seoul-citydata.pages.dev`. |
| 16 | 새 프로젝트 영문 코드명 결정 | - | 포트폴리오·커밋·다이어그램 일관성. |

## 11. CLAUDE.md 정정 사항 (23개 항목 + 단일 출처 원칙)

본 design.md 승인 후 한 번에 반영. 주요 항목:

1. §3·§5: Redpanda → Kafka KRaft single-node
2. §3·§5: 스트림 프로세서 = PyFlink 메인 + Spark batch 보조 (Day 9)
3. §10: 1번 정정 — "Kafka→S3→Spark batch→Iceberg, streaming 처리 없음"
4. §11 차별화 표: "스트림 처리" 행을 "스트리밍 엔진 / 배치 엔진" 두 행으로 분리
5. §0-1: Phase 1 정의 = 14일, Phase 1A (1~10) + Phase 1B (11~14)
6. §4·§5: `user.events.v1` 을 P2 → P1B로 이동
7. §5: Cloudflare D1 + Pages Functions + Workers Cron + Web Push 추가
8. §6: 사용자 화면 #4·#5 익명 버전을 P1B로 이동
9. §8: Day 11~14 일정 추가
10. §9: 포트폴리오 분량 = Phase 1A 5~6p / 1A+1B 8~10p, 페이지 구조 명시
11. §13: 트리거 9개 추가/보강 (Phase 1A) + 4개 신규 (Phase 1B) + 일반 리스크 5종 추가
12. §13: "Phase 1B fallback 핵심 원칙: 1A 사수, 1B 부분 완료라도 솔직히 기술" 추가
13. §3 메시징 행에 "Kafka 정당화 사유" 컬럼 추가
14. §11에 "Kafka 사용 패턴" 행 추가 (적재 통로 vs 메시지 버스)
15. §12 차별화 포인트에 면접 카운터 응답 표 추가
16. §9 포트폴리오 아키텍처 페이지에 "이종 소스 N → 1 버스 → 컨슈머 M" 다이어그램 명시
17. §16에 Databricks 미니 프로젝트 옵션 (Day 14 이후, 추후 결정) 추가
18. §13에 Databricks 무료 트라이얼은 본 프로젝트 외부 트랙임을 명시
19. §15 새 세션 체크리스트에 §17 Day 0 체크리스트 참조 추가
20. 신규 §17 추가: Day 0 사전 준비 체크리스트 (16개 항목)
21. §5 Phase 1B 인프라에 "Edge API → HTTPS receiver (Oracle Cloud) → Kafka" 경로 명시
22. §10 서사 연속성에 "1번이 micro-batch였음을 직시" 한 문단 추가
23. §2 약점 표에 "1번이 사실상 micro-batch였음" 정정 + 본 프로젝트의 streaming + 익명 실서비스 + 도메인 다변화 대응 명시
**단일 출처 원칙**: 위 1~23 항목 반영 후, CLAUDE.md의 §6·§8·§11·§12·§13의 세부 사항은 design.md 본문을 참조하도록 짧게 링크 처리한다. design.md 가 Phase 1의 단일 출처(single source of truth) 가 되고, CLAUDE.md 는 프로젝트 전체 컨텍스트(Phase 1·2 전반, 사용자 배경, 의사결정 원칙)만 유지한다.

> **참고**: 위 정정 사항은 implementation 단계와 별개로, design.md 승인 직후 별도 commit으로 일괄 반영한다.

## 12. 이중 체크포인트와 제출 전략

- **체크포인트 1 (Day 10)**: Phase 1A 5~6p 단독 제출 가능. §13 fallback이 발동돼 일정이 밀려도 이 시점이 1차 안전망.
- **체크포인트 2 (Day 14)**: Phase 1A+1B 8~10p 강화 버전 제출.
- **둘 다 못 맞춘다면** (예: 일정이 Day 13까지 늘어진 경우): Phase 1A 5~6p로 1차 제출하고, Phase 1B는 "진행 중 (Day 11~13까지 완료)"로 솔직히 기술. 이후 W1에 1B를 마무리해서 강화 버전으로 갱신.

## 13. 다음 단계

1. **사용자가 본 design.md 검토** (변경 요청 시 재작성 후 재검토).
2. 승인되면 **`writing-plans` 스킬로 이행**해 implementation plan 작성.
3. CLAUDE.md 29개 정정 사항을 design.md 승인 직후 일괄 반영 (별도 commit).
4. Day 0 체크리스트 항목 사용자 직접 진행 (Day 1 시작 전까지).
5. implementation plan 승인 후 Day 1 작업 시작.
