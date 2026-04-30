# 서울 도시데이터 플랫폼 (Project Brief)

> 신입 데이터 엔지니어 포트폴리오 보강을 위한 신규 프로젝트.
> 새 세션에서 Claude Code가 이 디렉토리에서 시작될 때, **이 문서를 먼저 끝까지 읽고** 작업을 이어갈 것.
> 결정 사항은 본 문서 기준으로 진행하며, 변경 시 본 문서를 업데이트할 것.
>
> **Phase 1 (1A+1B 14일)의 상세 청사진은 `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` 가 단일 출처.** 본 문서는 그 위의 전역 컨텍스트(Phase 1·2 정책, 사용자 배경, 의사결정 원칙) 만 다룬다.

---

## 0. 한 줄 요약

서울시 공공 실시간 데이터(도시데이터·지하철 혼잡도)와 Postgres CDC, 익명 사용자 행동 로그를 **Kafka 메시지 버스**로 통합하고 **PyFlink streaming + Spark batch + Iceberg(Lakekeeper) + dbt + GitHub Actions** 로 처리한다. 운영 비용은 월 $0~$2.

**Phase 1 (14일) = 1A (Day 1~10, 데이터 플랫폼 코어) + 1B (Day 11~14, 익명 실서비스 통합) → Phase 2 (8주, 본격 확장)**. Phase 1A 단독으로도 포트폴리오 5~6페이지 분량 제출 가능, 1A+1B 통합 시 8~10페이지.

---

## 0-1. Phase 정책 (가장 중요)

| 항목 | **Phase 1A (Day 1~10)** | **Phase 1B (Day 11~14)** | Phase 2 (확장, 8주) |
|------|--------------------------|--------------------------|---------------------|
| 목표 | 데이터 플랫폼 코어 + 공개 데모 URL | 익명 실서비스 통합 (사용자 행동 토픽 + 북마크 + Web Push) | 본격 풀스택 (실서비스 외부 공개·UGC 별점·Google Places) |
| 데이터 소스 | 서울 도시데이터 + 지하철 혼잡도 + 공공 인허가(정적) + Postgres CDC | + **`user.events.v1`** (익명 사용자 행동) | + Google Places + UGC 별점 + 버스 위치 |
| 메시징 | **Kafka KRaft single-node** | (동일) | (동일) |
| 스트림 처리 | **PyFlink (메인) + Spark batch (Day 9 멱등성 검증 보조)** | (동일) | PyFlink 확정 |
| 데이터 품질 | dbt tests + pytest | (동일) | + Great Expectations |
| BI / 시각화 | DuckDB notebook + Next.js 메인 지도 | + 익명 북마크 + Web Push 알림 | + Apache Superset + 동네/가게 상세 |
| 쿼리 엔진 | DuckDB | (동일) | + Trino single-node |
| 인프라 자동화 | docker-compose + 수동 README | + Cloudflare D1 + Pages Functions + Workers Cron | + Terraform IaC |
| 모니터링 | Flink Web UI + 자체 Python SLO 스크립트 | (동일) | + Grafana Cloud Free |
| SLO | 데이터 신선도 P95 < 7분 | (동일) | + 사용자 클릭 latency / CDC 정합성 / API 비용 |
| 포트폴리오 분량 | 5~6페이지 (1A 단독 제출 가능) | 8~10페이지 (1A+1B 강화 버전) | 12~15페이지 (전체 통합) |

**철칙**

- Phase 1A 끝(Day 10)에서 1차 포트폴리오 제출 가능 = **이중 안전망**. Phase 1B는 가능한 한 Day 14까지 마무리.
- Phase 1B 의 "실서비스" 는 **익명 쿠키 기반만**. 회원 가입 / 로그인 / 본격 인증은 Phase 2.
- Phase 1 데드라인이 위협받으면 즉시 §13 일반 리스크 + design.md §9 fallback 트리거를 적용한다.

---

## 1. 사용자 컨텍스트

- 신입 데이터 엔지니어 (0~3년차) 지원 중. 서류 탈락 반복 → 포트폴리오 보강이 본 프로젝트의 직접 동기.
- 사용자가 본 프로젝트로 명시한 3가지 목표:
  1. **Kafka 실시간 데이터 경험을 더 쌓는다.**
  2. **작게나마 실서비스를 직접 만들어본다.**
  3. **비용을 최대한 적게 쓴다.**
- 기존 포트폴리오 2건 보유.
  - **레시핑 (2025.07~10)** — 6인 팀, DE 단독. Self-hosted Lakehouse. 처리 흐름은 **Kafka → Kafka Connect (S3 Sink) → S3 → Spark batch (15분 주기, Airflow) → Iceberg → Trino → Superset.** **streaming 처리 자체는 없음** (Kafka는 적재 통로, Spark는 batch). 75% 인프라비 절감, 99.31% 정합성, 96.84% I/O 절감.
  - **E-commerce 유저 행동 분석 (2025.02~04)** — 1인. **Kaggle CSV → S3 → Snowflake (COPY/MERGE, 주간 batch) → Superset.** 48.7% 쿼리 스캔 감소, 45.6% 스키마 크기 감소.

## 2. 기존 포트폴리오의 약점과 본 프로젝트의 대응

| # | 기존 약점 | 본 프로젝트의 대응 | 커버 단계 |
|---|----------|------------------|-----------|
| 1 | 두 프로젝트 도메인 동일 (둘 다 유저 행동 분석) | 공공·공간 데이터 + CDC + 익명 행동 = 3종 이종 소스로 다양화 | P1A+1B |
| 2 | **1번이 사실상 micro-batch (Kafka는 적재 통로, 처리는 15분 Spark batch)** | Kafka → **PyFlink streaming** → Iceberg, 데이터 신선도 P95 < 7분 SLO | P1A |
| 3 | 실데이터 부재 (시뮬레이션, Kaggle) | 공공 실시간 API (P1A) + **익명 실서비스 행동 로그** (P1B) → 본인이 그 데이터의 producer + consumer | P1A+1B |
| 4 | CDC 패턴 부재 | Postgres → Debezium → Kafka → PyFlink → `dim_place` (SCD Type 2 골격) | P1A |
| 5 | dbt + CI/CD for Data 부재 | dbt-core (Silver→Gold) + GitHub Actions (PyFlink/dbt PR 검증) | P1A |
| 6 | 데이터 품질 자동화 부재 | dbt tests + pytest (P1) → + Great Expectations (P2) | P1A+P2 |
| 7 | Catalog/Lineage 부재 | Lakekeeper REST Catalog + dbt-docs | P1A |
| 8 | 미해결 이슈가 "예정"으로 끝남 (1번 페이지 9·11) | Day 9 **Spark batch MERGE INTO + `rewrite_data_files`** 로 1번의 멱등성/Compaction 미해결 closure | P1A |
| 9 | IaC 부재 | docker-compose (P1) → Terraform (P2) | P1A+P2 |
| 10 | 테스트 코드 부재 | pytest로 transform 단위 테스트 | P1A |

→ **Phase 1A 단독으로 10개 약점 중 8개를 커버.** Phase 1B 추가 시 #1·#3 강화. Phase 1A 단독 포트폴리오로도 충분히 임팩트.

## 3. 핵심 의사결정

| 항목 | 결정 | 정당화 사유 |
|------|------|------|
| 서비스 컨셉 | 서울 실시간 지역 혼잡도 + 익명 북마크/알림 (P1B) → 혼잡도 기반 지역 추천 (P2) | Kafka 정당화 + 비용 0원 가능 |
| 운영 비용 목표 | 월 $0~$2 | Oracle Cloud Always Free + Cloudflare 무료 + 공공 무료 API |
| 메시징 | **Kafka KRaft single-node** (Redpanda 아님, ZooKeeper 없음) | 이종 소스 4종 통합 + 컨슈머 2개(Flink + Workers Cron) + Replay + Flink exactly-once + 학습 목표. **단일 토픽이었으면 안 썼을 것**. **single-node 사유**: 1번이 3-node였으나 본 프로젝트는 24GB + Day 9 Spark 일시 기동 OOM 회피 + HA 비즈니스 정당화 약함(공공 API polling 자연 복구·CDC offset 보장) → 의식적 단순화, SPOF 는 limitation 으로 인정 |
| 스트림 처리 | **PyFlink (메인)** + **Spark batch (Day 9 보조)** | streaming = Flink (1번에 없던 영역), batch = Spark (1번에서 익힌 도구로 1번 미해결 이슈 closure) — **엔진을 용도별로 분리** |
| 카탈로그 | Lakekeeper REST Catalog (fallback: JdbcCatalog) | 1번의 Hive Metastore와 차별화 + 메모리 절감 |
| 쿼리 엔진 | DuckDB 우선, Trino는 P2 옵션 | 메모리·운영 부담 최소 |
| 변환 / CI | dbt-core + GitHub Actions | 1·2번 모두 부재였던 영역 |
| Edge API → Kafka | **REST Proxy 패턴** (Cloudflare Pages Functions → HTTPS → Oracle Cloud HTTP receiver → Kafka) | Workers의 TCP 직접 연결 제약 회피, 디버깅 비용 최소화 |
| 사용자 메타 저장소 | Cloudflare D1 (북마크, push subscription만) | 5GB 무료. **행동 로그는 절대 D1에 넣지 않음 — Kafka → Iceberg 직행** |
| 알림 | Web Push (VAPID) + Workers Cron | 외부 서비스 의존 없음, 비용 0원 |
| 가게 정보 출처 | **카카오/네이버 스크래핑 절대 금지** → 공공 인허가(P1) + Google Places API(P2) + 자체 UGC(P2) | ToS·법적 리스크 회피, 데이터 거버넌스 감각 어필 |
| Databricks | 본 프로젝트엔 미도입 | CE는 streaming 24/7 불가, Free Trial 14일 후 과금. Day 14 이후 별도 미니 프로젝트로 검토 가능 |

## 4. 데이터 소스 (모두 무료·합법)

| 소스 | 용도 | 갱신 주기 | 비용 | 단계 |
|------|------|----------|------|-------|
| 서울시 실시간 도시데이터 API (`SeoulRtd`) | 핫스팟 120곳의 혼잡도 단계, 도로 소통, 따릉이, 날씨 | 5분 | $0 | **P1A** |
| 서울교통공사 실시간 지하철 혼잡도 API | 역사·칸별 혼잡도 | 30초~1분 | $0 | **P1A** |
| 공공데이터 인허가 정보 | 가게 마스터 (이름, 주소, 업종, 영업시간) | 일 1회 배치 (P1은 정적 1회 적재) | $0 | **P1A** |
| Postgres `places` 테이블 (자체 OLTP) | CDC 데모용 가게 마스터 | 실시간 (Debezium) | $0 | **P1A** |
| **익명 사용자 행동 로그** (브라우저 → Edge API → HTTP receiver → Kafka) | 클릭, 지도 이동, 북마크, 알림 구독 | 실시간 | $0 | **P1B** |
| 서울 버스 위치 정보 API | 노선별 버스 위치 | 1분 | $0 | P2 |
| Google Places API | 별점, 영업시간 보강 | 캐시 + 증분 갱신 | 월 $200 무료 크레딧 내 | P2 |
| UGC 별점 (자체 입력) | 사용자 직접 입력 | 실시간 | $0 | P2 |

## 5. 아키텍처 (요약)

> 상세 다이어그램·토픽 명세·데이터 레이어는 **design.md §4** 단일 출처 참조.

### 인프라 레이어 (Phase 1·2 종합)

| 구성 요소 | Phase 1A | Phase 1B 추가 | Phase 2 추가 |
|-----------|----------|---------------|--------------|
| 컴퓨트 | Oracle Cloud Always Free VM (ARM Ampere A1, 4 vCPU / 24GB RAM) | (동일) | (동일) |
| 객체 스토리지 | Oracle Object Storage 10GB Free (또는 로컬 MinIO) | (동일) | (동일) |
| 프론트엔드 | Cloudflare Pages (Next.js 메인 지도) | + 북마크 UI, 서비스워커 | + 동네/가게 상세 페이지 |
| Edge / API | (P1A 없음) | **Cloudflare Pages Functions** (Edge API) + Oracle Cloud HTTP receiver (REST Proxy) + Cloudflare Workers Cron (알림 발신) | (동일) |
| 사용자 메타 저장소 | (P1A 없음) | **Cloudflare D1** (북마크, push subscription) | (동일) |
| 외부 노출 | Cloudflare Tunnel | (동일) | (동일) |
| 모니터링 | Flink Web UI + 자체 Python SLO 스크립트 | (동일) | + Grafana Cloud Free |

### 데이터 스택 (Phase 1·2 종합)

| 영역 | Phase 1A | Phase 1B 추가 | Phase 2 추가 |
|------|----------|---------------|--------------|
| 메시징 | **Kafka KRaft single-node** | (동일) | (동일) |
| Kafka 토픽 | `seoul.hotspot.congestion.v1`, `seoul.transit.subway.v1`, `place.master.cdc.v1` | + **`user.events.v1`** | + `seoul.transit.bus.v1`, `place.gmap.snapshot.v1`, `user.review.v1` |
| CDC | Debezium (`places` 1테이블) | (동일) | + 추가 테이블 |
| OLTP | Postgres (가게 마스터) | + (Day 11 fallback용 `events_inbox` — 선택) | + 사용자, UGC |
| 스트림 처리 | **PyFlink (메인)** | (동일) | (동일) |
| 배치 처리 | **Spark batch (Day 9 멱등성 검증)** | (동일) | (동일) |
| 레이크하우스 | Apache Iceberg + Lakekeeper REST Catalog (fallback: JdbcCatalog) | (동일) | (동일) |
| 분석 엔진 | DuckDB (Iceberg 직접 쿼리) | (동일) | + Trino single-node |
| 변환 레이어 | dbt-core (Silver→Gold) | (동일) | (동일) |
| 데이터 품질 | dbt tests + pytest | (동일) | + Great Expectations |
| BI | DuckDB notebook 스크린샷 + Next.js 지도 | + 북마크 위젯 + Web Push | + Apache Superset |
| CI/CD | GitHub Actions (dbt + PyFlink lint/test) | (동일) | (동일) |

### Medallion 레이어

- **Bronze**: raw 이벤트 (스키마 검증만)
- **Silver**: 정규화, 행정구역/핫스팟 region join, 좌표 정제, 중복 제거
- **Gold**: `fact_hotspot_congestion_5min` (P1A) / `dim_region` (P1A) / `dim_place` (P1A 단일 출처 SCD2 골격, P2 다출처 머지) / `fact_place_status_hourly` (P1A 골격, P2 본격) / **`fact_user_event` (P1B)**

## 6. 사용자가 보는 화면

| # | 화면 | 단계 |
|---|------|-------|
| 1 | **메인 지도** — 핫스팟 120곳 혼잡도 색상 | **P1A** |
| 2 | **"지금 한가하고 영업 중인 곳"** — 혼잡도 + 영업시간 결합 데모 1개 | **P1A** |
| 3 | **익명 동네 북마크** — 쿠키 기반 anon_id, D1 저장 | **P1B** |
| 4 | **북마크 동네 한가해지면 Web Push 알림** (fallback: 모니터링 페이지 배지) | **P1B** |
| 5 | **동네 상세** — 평균 패턴, 도착 가능한 대중교통, 영업 중인 가게 | P2 |
| 6 | **가게 상세** — Google 별점 + 자체 UGC 별점, 영업시간, "지금 영업 중" | P2 |
| 7 | **메인 지도에 실시간 버스 흐름 오버레이** | P2 |

## 7. SLO

| SLO | 정의 | 단계 |
|-----|------|-------|
| **데이터 신선도 (P1A 핵심)** | 공공 도시데이터 API 응답 `tm` → Iceberg Gold 도달 P95 < 7분 | **P1A** (측정 방식은 design.md §6-2) |
| 익명 사용자 클릭 → Gold 도달 | P95 < 5초 (best-effort 측정) | P1B (정식 측정은 P2) |
| CDC 정합성 | Postgres 변경 → Iceberg 반영 P95 < 10초 | P1A best-effort, P2 정식 |
| 외부 유료 API 비용 | Google Places 월 호출이 무료 크레딧($200)의 X% 이내 | P2 |

## 8. Phase 1 일정

> Phase 1A (Day 1~10) + Phase 1B (Day 11~14) **상세 Day별 일정은 design.md §6-1·§7-1 참조.**

### Phase 2 일정 (확장, W1~W8)

Phase 1 완료 후 진행. Phase 1A·1B에 들어간 항목 제외 잔여:

| 주차 | 작업 |
|------|------|
| W1 | 본인 운영 실서비스 회원 가입/로그인 도입 (익명 → 식별 사용자), Cloudflare Workers Edge API 본격 |
| W2 | UGC 별점 입력, `user.review.v1` 토픽 추가 |
| W3 | 버스 위치 토픽 추가, 지역 추천 점수 모델 고도화 |
| W4 | Superset 대시보드, SLO 4종 전체 측정 |
| W5 | Great Expectations 도입, dbt-docs lineage 자동 배포 |
| W6 | Google Places 캐싱·증분 파이프라인, `dim_place` 다출처 머지 |
| W7 | Trino 옵션 도입, Terraform IaC 작성, Grafana Cloud 연결 |
| W8 | OKKY/Reddit/Disquiet 외부 공개, 실유저 행동 데이터 인사이트 1~2건 |

## 9. 포트폴리오 페이지 구성

> 상세 페이지 구조는 **design.md §6-3 (Phase 1A 단독, 5~6p) / §7-5 (1A+1B 통합, 8~10p)** 참조.

| 시점 | 분량 | 핵심 메시지 |
|------|------|------------|
| **Phase 1A 완료 (Day 10)** | 5~6페이지 | streaming 진정성 + 1번 미해결 closure + 공공 실데이터 도메인 확장 |
| **Phase 1A+1B 완료 (Day 14)** | 8~10페이지 | + **본인 운영 익명 실서비스 + 그 서비스의 데이터 플랫폼 동시 보유** |
| **Phase 1+2 통합 (8주 후)** | 12~15페이지 | + 실사용자 데이터 인사이트 + 추가 SLO 3종 |

## 10. 기존 포트폴리오와의 연결 (서사 연속성)

- **1번이 사실상 micro-batch였음을 직시한 학습 곡선**: 1번에서 Kafka는 Connect로 S3 적재 통로였고, 처리는 15분 주기 Spark batch였음. 본 프로젝트는 그 한계를 인지하고 **Kafka → PyFlink streaming → Iceberg** 구조로 streaming 영역을 새로 채움. 1번에서 익힌 Spark·Iceberg 경험은 Day 9 **Spark MERGE INTO 멱등성 검증**에 재활용해 1번의 미해결 이슈를 직접 closure.
- 1번 페이지 9·11의 "Dynamic Partition Overwrite **예정**" / "Compaction **도입 예정**" → 본 프로젝트 Day 9에서 **Iceberg MERGE INTO + dedup key + `rewrite_data_files`** 로 해결.
- 1번에서 SCD Type 1 선택 → 본 프로젝트 Phase 1A 에서 `dim_place` SCD Type 2 골격, Phase 2 에서 영업시간/별점 변경 이력 추적으로 본격화.
- 2번 프로젝트(E-commerce)를 본 프로젝트로 대체 가능. 최종 포트폴리오 = **레시핑 + 본 프로젝트 (P1A 단독 또는 P1A+1B 통합) 2개**.

## 11. 기술 스택 차별화 표

| 영역 | 1번 (레시핑) | 2번 (E-commerce) | **본 프로젝트 P1A** | **+ P1B 추가** | **+ P2 추가** |
|------|-------------|----------------|---------------------|---------------|---------------|
| 메시징 | Kafka 3-node | - | **Kafka KRaft single-node** | (동일) | (동일) |
| **Kafka 사용 패턴** | Connect로 S3 적재만 (통로) | - | **Flink consumer streaming** (메시지 버스) | + 컨슈머 2개 (Flink + Workers Cron) | (동일) |
| **스트리밍 엔진** | (없음) | (없음) | **PyFlink** | (동일) | (동일) |
| **배치 엔진** | Spark (S3→Iceberg 정제) | Snowflake SQL | **Spark batch (Day 9 멱등성)** | (동일) | (동일) |
| 카탈로그 | Hive Metastore | - | Lakekeeper REST Catalog | (동일) | (동일) |
| 쿼리 엔진 | Trino | Snowflake | DuckDB | (동일) | + Trino single-node |
| 변환 | Spark SQL (수동) | Snowflake SQL | dbt-core | (동일) | (동일) |
| DQ | dropDuplicates 수동 | MERGE INTO | dbt tests + pytest | (동일) | + Great Expectations |
| **데이터 출처** | 시뮬레이터 (가짜) | Kaggle (정적) | 공공 실시간 API + CDC | + **익명 실서비스 행동 로그** | + UGC + Google Places |
| CDC | 없음 | 없음 | Debezium | (동일) | (동일) |
| CI/CD | 없음 | 없음 | GitHub Actions | (동일) | (동일) |
| 실서비스 | 시뮬레이터로 대체 | Kaggle | 공개 데모 URL | + **본인 운영 익명 실서비스** | + 회원 가입/UGC |
| IaC | 없음 | 없음 | docker-compose | (동일) | + Terraform |

## 12. 차별화 포인트 (면접 어필)

> 면접 카운터 응답 8종은 **design.md §8-2** 단일 출처 참조.

- **Phase 1A 단독으로도** 신입 풀에서 보기 드문 조합: 진짜 Kafka streaming(Flink) + 공공 실시간 API + CDC + Lakekeeper + dbt + GitHub Actions + Iceberg MERGE INTO 멱등성 closure
- **Phase 1A+1B 통합 시 희소성**: "직접 운영하는 익명 실서비스 + 그 서비스의 데이터 플랫폼" 동시 보유 — 신입 풀에서 거의 안 보임
- **공공·공간 데이터 도메인** = 한국 시장 (SK·KT·네이버·카카오모빌리티·티맵·당근 등)에 강한 어필
- **이종 소스 통합 패턴**: P1A 3개(공공 API 2 + CDC) → P1B 4개 (+ 익명 행동) → P2 6+개. 메시지 버스로서의 Kafka 정당화.
- **비용 0원 운영** 자체가 FinOps 사고의 증거 (1번 75% 절감 서사 연속)
- **합법 출처만 사용** = 데이터 거버넌스 감각 어필 (스크래핑 금지, 익명 ID 거버넌스, /privacy 페이지)
- **Phase 분리 + 이중 체크포인트** 자체가 우선순위 판단 능력의 증거

## 13. 일반 리스크 / 운영 원칙

> Day별 fallback 트리거 14종은 **design.md §9-1·§9-2** 단일 출처 참조. 본 절은 일반 원칙만.

- **카카오맵·네이버지도 스크래핑 절대 금지**. ToS 위반 + 법적 분쟁 사례 다수.
- **개인정보 회피** — 행동 이벤트는 익명 ID(쿠키)만, IP 영구저장 없음, `/privacy` 1줄 처리방침 페이지 필수.
- **Oracle Cloud Always Free 회수 정책** — 90일 미사용 시 회수. 카드 등록 후 PAYG 업그레이드(과금 없음)로 회수 방지.
- **공공 API rate limit** — 토큰 키마다 한도 있음. 캐싱·백오프 필수.
- **Google Places API 비용** (P2) — 캐싱 전략 필수 (TTL 7~30일).
- **Kafka KRaft single-node 한계** — 무중단 보장 어려움. "비용 vs 가용성" Trade-off로 명시.
- **메모리 충돌 주의**: Kafka + Flink 상시 가동 + Spark는 Day 9 일시 기동 원칙 (24GB 한계).
- **Edge API 토큰 보안**: `user.events.v1` 발행 권한 secret 관리. Kafka는 Tunnel 내부에서만 수신.
- **D1 5GB 한도**: 사용자 메타만 (북마크/구독). 행동 로그는 절대 D1에 넣지 않음.
- **VAPID 키 / 환경변수**: GitHub commit 금지. `.gitignore` 에 이미 포함.
- **Phase 1B fallback 핵심 원칙**: 1A는 사수, 1B는 부분 완료라도 솔직히 기술 (Day 14 시간 부족 시 Phase 1A만 단독 제출 + 1B는 "진행 중"으로 명시).

## 14. 작업 환경

- **실제 코드/문서 위치**: `/Users/aryijq/Documents/01_DE_project/seoul-citydata-platform/` (본 디렉토리)
- **옵시디언 볼트 노출**: `/Users/aryijq/Documents/obsidian-vault/01 Projects/01-06 서울 도시데이터 플랫폼` (심볼릭 링크)
- 옵시디언에서 작성한 업무 일지·메모도 본 디렉토리에 함께 쌓임

## 15. 새 세션 시작 시 체크리스트 (Claude Code 용)

새 세션에서 본 디렉토리에 진입했을 때:

- [ ] 본 CLAUDE.md 전체를 끝까지 읽기
- [ ] **Phase 1 작업 중이면 `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` 도 함께 읽기** (Phase 1 단일 출처)
- [ ] **현재 어느 단계 / 어느 Day 인지 사용자에게 확인** (P1A Day X / P1B Day X / P2 Wx)
- [ ] `git log --oneline -20` 으로 최근 진행 상황 파악
- [ ] `git status` 로 현재 작업 중이던 변경 사항 확인
- [ ] 프로젝트 루트의 `README.md` 가 있다면 함께 확인
- [ ] design.md §9 fallback 트리거 조건이 활성화되었는지 확인
- [ ] §17 Day 0 사전 준비 항목이 모두 완료됐는지 확인 (Day 1 시작 전)
- [ ] 결정 사항 변경이 발생하면 본 문서의 해당 섹션 또는 design.md를 업데이트할 것
- [ ] 한국어 존댓말로 응답할 것 (사용자 전역 규칙)

## 16. 관련 메모리·문서 참조

- **Phase 1 단일 출처**: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`
- 사용자 전역 규칙: `~/.claude/CLAUDE.md`
- 옵시디언 볼트 메모리 인덱스: `~/.claude/projects/-Users-aryijq-Documents-obsidian-vault/memory/MEMORY.md`
- 1번 포트폴리오 코드: `/Users/aryijq/Documents/01_DE_project/reciping-data-pipeline/`
- 포트폴리오 PDF: `/Users/aryijq/Documents/obsidian-vault/01 Projects/01-04 이력서/전상택 Data Engineer 포트폴리오.pdf`
- (옵션) Day 14 이후 Databricks 미니 프로젝트 — 추후 결정

## 17. Day 0 사전 준비 체크리스트

> 상세 16개 항목은 **design.md §10** 참조. 핵심 요약만:

- 즉시 신청 (영업일 소요): 서울 OpenAPI 키 / 지하철 혼잡도 API 키 / 공공데이터 인허가 정보 다운로드
- 계정 셋업: Oracle Cloud (PAYG 업그레이드 필수) / Cloudflare (Pages·D1·Workers·Tunnel) / GitHub repo
- 로컬 환경: Docker Desktop or colima / Python 3.11+ / Node 20+
- 미리 만들어두기: VAPID 키 페어 / 1번 docker-compose.yml 사본 위치 메모 / 1번 Spark MERGE INTO 코드 위치 메모
- 결정만 필요: GitHub repo 이름 / 도메인 서브 / 영문 코드명
