# 서울 도시데이터 플랫폼 (Project Brief)

> 신입 데이터 엔지니어 포트폴리오 보강을 위한 신규 프로젝트.
> 새 세션에서 Claude Code가 이 디렉토리에서 시작될 때, **이 문서를 먼저 끝까지 읽고** 작업을 이어갈 것.
> 결정 사항은 본 문서 기준으로 진행하며, 변경 시 본 문서를 업데이트할 것.

---

## 0. 한 줄 요약

서울시 공공 실시간 데이터(도시데이터·지하철 혼잡도)를 Kafka로 통합 처리하여, **"실시간 지역 혼잡도 + 혼잡도 기반 지역 추천"** 서비스를 구축한다. 운영 비용은 월 $0~$2 를 목표로 한다.

**본 프로젝트는 Phase 1(10일, 즉시 실행) → Phase 2(확장, 향후 8주)로 분리해 진행한다.** Phase 1만으로도 포트폴리오 1장 분량의 제출 가능 수준에 도달하는 것을 목표로 한다.

---

## 0-1. Phase 정책 (가장 중요)

| 항목 | **Phase 1 (10일, 즉시 실행)** | Phase 2 (확장, 향후 8주) |
|------|--------------------------------|---------------------------|
| 목표 | 포트폴리오 제출 가능한 end-to-end 동작 | 실서비스 통합 + 풀스택 데이터 플랫폼 |
| 데이터 소스 | 서울시 도시데이터 API + 지하철 혼잡도 + 공공 인허가(정적) + Postgres CDC(`places` 1테이블) | + 본인 서비스 사용자 행동 + UGC 별점 + Google Places + 버스 위치 |
| 스트림 처리 | PyFlink (막히면 Spark Structured Streaming으로 fallback) | PyFlink 확정 |
| 데이터 품질 | dbt tests | + Great Expectations |
| BI / 시각화 | DuckDB notebook + Next.js 지도 1페이지 | + Apache Superset |
| 쿼리 엔진 | DuckDB | + Trino single-node |
| 인프라 자동화 | docker-compose + 수동 README 가이드 | Terraform IaC |
| 모니터링 | Flink Web UI + 자체 Python SLO 측정 스크립트 | + Grafana Cloud |
| 사용자 화면 | 메인 지도 + "지금 한가하고 영업 중" 데모 1개 | + 동네 상세 + 가게 상세 + 알림 |
| SLO | 데이터 신선도 1개 측정 | 4개 전체 |
| 포트폴리오 분량 | 1장 (Phase 1 단독 제출 가능) | 5~6장 (Phase 1+2 통합) |

**철칙**

- Phase 1을 끝내고 포트폴리오에 **먼저** 제출. Phase 2는 그 이후 부가 확장.
- Phase 1 스코프 안에서는 "본인 운영 실서비스" 운영을 시작하지 않는다. 사용자 행동 토픽이 필요하면 시뮬레이터로 대체하고, 포트폴리오에는 "Phase 2에서 추가 예정"으로 명시.
- Phase 1 데드라인이 위협받으면 즉시 §13의 fallback 트리거를 적용한다.

---

## 1. 사용자 컨텍스트

- 신입 데이터 엔지니어 (0~3년차) 지원 중. 서류 탈락 반복 → 포트폴리오 보강이 본 프로젝트의 직접 동기.
- 기존 포트폴리오 2건 보유.
  - **레시핑 (2025.07~10)** — 6인 팀, DE 단독. Self-hosted Lakehouse (Kafka, Spark, Iceberg, Trino, Hive Metastore, Airflow, Superset). 75% 인프라비 절감, 99.31% 정합성, 96.84% I/O 절감.
  - **E-commerce 유저 행동 분석 (2025.02~04)** — 1인. Snowflake + Star Schema. 48.7% 쿼리 스캔 감소, 45.6% 스키마 크기 감소.

## 2. 기존 포트폴리오의 약점과 본 프로젝트의 대응

| # | 기존 약점 | 본 프로젝트의 대응 | 커버 Phase |
|---|----------|------------------|-----------|
| 1 | 두 프로젝트 도메인 동일 (둘 다 유저 행동 분석) | 공공·공간 데이터 도메인으로 다양화 | P1 |
| 2 | 실데이터 부재 (시뮬레이션, Kaggle) | 공공 실시간 API (P1) → + 실서비스 사용자 행동 (P2) | P1+P2 |
| 3 | CDC 패턴 부재 | Postgres → Debezium → Kafka | P1 |
| 4 | dbt 부재 | dbt-core 도입 (Silver→Gold 변환) | P1 |
| 5 | 데이터 품질 자동화 부재 | dbt tests (P1) → + Great Expectations (P2) | P1+P2 |
| 6 | CI/CD for Data 부재 | GitHub Actions로 PyFlink/dbt PR 검증 | P1 |
| 7 | Catalog/Lineage 부재 | Lakekeeper REST Catalog + dbt-docs | P1 |
| 8 | IaC 부재 | Terraform으로 Oracle Cloud 리소스 정의 | P2 |
| 9 | 테스트 코드 부재 | pytest로 transform 단위 테스트 | P1 |

→ **Phase 1만으로 9개 약점 중 7개를 커버.** 즉 Phase 1 단독 포트폴리오로도 충분히 임팩트가 있다.

## 3. 핵심 의사결정

| 항목 | 결정 | 근거 |
|------|------|------|
| 서비스 컨셉 | 서울 실시간 지역 혼잡도 + 혼잡도 기반 지역 추천 (P2에서 본인 운영 실서비스 통합) | Kafka 정당화 + 비용 0원 가능 |
| 운영 비용 목표 | 월 $0~$2 | Oracle Cloud Always Free + Cloudflare 무료 + 공공 무료 API |
| 실시간성의 원천 | 공공 API 5분~1분 폴링 + (P2) 사용자 행동 push + Postgres CDC | 진짜 push 스트림(HN/거래소)보다 약하지만, 공공·공간 데이터 차별화로 보완 |
| 가게 정보 출처 | **카카오/네이버 스크래핑 절대 금지** → 공공 인허가(P1) + Google Places API + 자체 UGC(P2) | ToS·법적 리스크 회피, 데이터 거버넌스 감각 어필 |
| 스트림 프로세서 | **PyFlink** (1번이 Spark Streaming이므로 차별화) | 부담 시 Spark Structured Streaming으로 fallback 가능 (§13 트리거 참조) |
| 메시징 | Redpanda single-node (Kafka 호환, ZooKeeper 불필요) | 1인 운영 + 비용 0원 |
| 카탈로그 | Lakekeeper REST Catalog | 1번의 Hive Metastore와 차별화 + 메모리 절감 |
| 쿼리 엔진 | DuckDB 우선, 필요 시 Trino single-node (P2) | 메모리·운영 부담 최소 |

## 4. 데이터 소스 (모두 무료·합법)

| 소스 | 용도 | 갱신 주기 | 비용 | Phase |
|------|------|----------|------|-------|
| 서울시 실시간 도시데이터 API (`SeoulRtd`) | 핫스팟 120곳의 혼잡도 단계, 도로 소통, 따릉이, 날씨 | 5분 | $0 | **P1** |
| 서울교통공사 실시간 지하철 혼잡도 API | 역사·칸별 혼잡도 | 30초~1분 | $0 | **P1** |
| 공공데이터 인허가 정보 | 가게 마스터 (이름, 주소, 업종, 영업시간) | 일 1회 배치 (P1은 정적 1회 적재) | $0 | **P1** |
| Postgres `places` 테이블 (자체 OLTP) | CDC 데모용 가게 마스터 | 실시간 (Debezium) | $0 | **P1** |
| 서울 버스 위치 정보 API | 노선별 버스 위치 | 1분 | $0 | P2 |
| Google Places API | 별점, 영업시간 보강 | 캐시 + 증분 갱신 | 월 $200 무료 크레딧 내 | P2 |
| 본인 서비스 사용자 행동 | 클릭, 검색, 스크롤, 북마크 | 실시간 | $0 | P2 |
| 본인 서비스 UGC 별점 | 사용자 직접 입력 | 실시간 | $0 | P2 |

## 5. 아키텍처

### 인프라

| 구성 요소 | Phase 1 | Phase 2 추가 |
|-----------|---------|--------------|
| 컴퓨트 | Oracle Cloud Always Free VM (ARM Ampere A1, 4 vCPU / 24GB RAM) | (동일) |
| 객체 스토리지 | Oracle Object Storage 10GB Free (또는 로컬 MinIO) | (동일) |
| 프론트엔드 | Cloudflare Pages (Next.js 단일 페이지) | + 동네/가게 상세 페이지 |
| Edge API | (P1은 Next.js API route로 갈음) | Cloudflare Workers |
| 외부 노출 | Cloudflare Tunnel | (동일) |
| 모니터링 | Flink Web UI + 자체 Python SLO 스크립트 | + Grafana Cloud Free |

### 데이터 스택

| 영역 | Phase 1 | Phase 2 추가 |
|------|---------|--------------|
| 메시징 | Redpanda single-node | (동일) |
| CDC | Debezium (Kafka Connect) — `places` 1테이블 | + 추가 테이블 |
| OLTP | Postgres (가게 마스터) | + 사용자, UGC |
| 스트림 처리 | PyFlink (fallback: Spark Structured Streaming) | (동일) |
| 레이크하우스 | Apache Iceberg + Lakekeeper REST Catalog (fallback: JdbcCatalog) | (동일) |
| 분석 엔진 | DuckDB (Iceberg 직접 쿼리) | + Trino single-node |
| 변환 레이어 | dbt-core (Silver→Gold) | (동일) |
| 데이터 품질 | dbt tests + pytest | + Great Expectations |
| BI | DuckDB notebook 스크린샷 | + Apache Superset |
| CI/CD | GitHub Actions (dbt + PyFlink lint/test) | (동일) |

### Kafka 토픽

```
# Phase 1
seoul.hotspot.congestion.v1   (5분, JSON)   서울시 도시데이터 API
seoul.transit.subway.v1       (1분, JSON)   지하철 혼잡도
place.master.cdc.v1           (Debezium)    Postgres 가게 마스터 변경

# Phase 2
seoul.transit.bus.v1          (1분, JSON)   버스 위치
place.gmap.snapshot.v1        (이벤트)      Google Places 캐시 갱신
user.events.v1                (실시간)      사용자 행동
user.review.v1                (실시간)      UGC 별점
```

### 데이터 레이어 (Medallion)

- **Bronze**: raw 이벤트 (스키마 검증만)
- **Silver**: 정규화, 행정구역/핫스팟 region join, 좌표 정제, 중복 제거
- **Gold**:
  - `fact_hotspot_congestion_5min` (P1)
  - `dim_region` (자치구 / 행정동 / 핫스팟) (P1)
  - `dim_place` — P1은 공공 인허가 단일 출처 + SCD Type 2 골격, P2에서 Google + UGC 머지 + 출처 컬럼 분리
  - `fact_place_status_hourly` (영업·혼잡) (P1 최소 골격, P2 본격 활용)
  - `fact_user_event` (P2)

## 6. 사용자가 보는 화면

| # | 화면 | Phase |
|---|------|-------|
| 1 | **메인 지도** — 핫스팟 120곳 혼잡도 색상 | **P1** |
| 2 | **"지금 한가하고 영업 중인 곳"** — 혼잡도 + 영업시간 결합 데모 1개 | **P1** |
| 3 | **동네 상세** — 평균 패턴, 도착 가능한 대중교통, 영업 중인 가게 | P2 |
| 4 | **가게 상세** — Google 별점 + 자체 UGC 별점 (출처 분리), 영업시간, "지금 영업 중" | P2 |
| 5 | **알림** — 즐겨찾는 동네가 한가해질 때 푸시 | P2 (선택) |
| 6 | **메인 지도에 실시간 버스 흐름 오버레이** | P2 |

## 7. SLO

| SLO | 정의 | Phase |
|-----|------|-------|
| **데이터 신선도 (P1 핵심)** | 공공 도시데이터 API 응답 시각 → Gold 도달까지 P95 < 7분 | **P1** |
| 사용자 클릭 → Gold 도달 | P95 < 5초 | P2 |
| CDC 정합성 | Postgres 변경 → Iceberg 반영 P95 < 10초 | P2 (P1에선 best-effort 측정만) |
| 외부 유료 API 비용 | Google Places 월 호출 비용 무료 크레딧($200)의 X% 이내 | P2 |

## 8. Phase 1 일정 (10일, Day 1~10)

전제: 하루 8~10시간 집중. 1번 포트폴리오(레시핑)의 docker-compose / Spark 코드 재활용 가능.

| Day | 작업 | 산출물 |
|-----|------|--------|
| 1 | Oracle Cloud ARM VM 셋업, docker-compose (Redpanda, Postgres, MinIO, Lakekeeper), 서울시 OpenAPI 키 발급 | 인프라 기동 |
| 2 | 서울 도시데이터 API + 지하철 혼잡도 producer (Python) → Redpanda 2개 토픽 | Bronze 토픽 흐름 |
| 3 | PyFlink job: Bronze → Silver (스키마 정규화, 핫스팟 region 매핑) → Iceberg via Lakekeeper | Silver 테이블 조회 가능 |
| 4 | PyFlink Silver → Gold (`fact_hotspot_congestion_5min`), DuckDB로 Iceberg 직접 쿼리 검증, **데이터 신선도 SLO 측정 코드** | Gold + SLO 메트릭 1개 |
| 5 | dbt-core 도입, Silver→Gold 일부 dbt로 이관, dbt tests 5~10개, GitHub Actions로 dbt CI | CI green + DQ 리포트 |
| 6 | Postgres `places` + Debezium connector, `place.master.cdc.v1` → PyFlink → `dim_place` (SCD Type 2 골격) | CDC 동작 데모 |
| 7 | Next.js 단일 페이지 + Mapbox/Leaflet, 핫스팟 120개 혼잡도 색상, Cloudflare Pages 배포 | 도메인 붙은 데모 |
| 8 | "지금 한가하고 영업 중인 카페" 데모 화면 1개 (영업시간은 공공 인허가 정적 데이터 사용) | 사용자 화면 |
| 9 | 멱등성 테스트 (Iceberg MERGE INTO + dedup key), Compaction, 운영 비용 측정 | 1번 포트폴리오 미해결 이슈 해결 증거 |
| 10 | 아키텍처 다이어그램, README, 포트폴리오 1장 작성, 인사이트 1개 (예: "강남역 평일 18-20시 혼잡 패턴") | **포트폴리오 제출 가능** |

### Phase 2 일정 (확장, W1~W8)

Phase 1 완료 후 진행. 원안 6~8주 일정에서 Phase 1에 들어간 항목을 제외한 잔여:

| 주차 | 작업 |
|------|------|
| W1 | 본인 운영 실서비스 Next.js 프론트 + Cloudflare Workers Edge API |
| W2 | `user.events.v1` / `user.review.v1` 추가, 사용자 행동 스트림 본격화 |
| W3 | 버스 위치 토픽 추가, 지역 추천 점수 모델 고도화 |
| W4 | Superset 대시보드, SLO 4종 전체 측정 |
| W5 | Great Expectations 도입, dbt-docs lineage 자동 배포 |
| W6 | Google Places 캐싱·증분 파이프라인, UGC 별점 입력, `dim_place` 다출처 머지 |
| W7 | Trino 옵션 도입, Terraform IaC 작성, Grafana Cloud 연결 |
| W8 | OKKY/Reddit/Disquiet 외부 공개, 실유저 행동 데이터 인사이트 1~2건 |

## 9. 포트폴리오 페이지 구성

### Phase 1 단독 (10일 후 즉시 제출, 1장)

```
[제목] 서울 실시간 지역 혼잡도 데이터 플랫폼 (Phase 1)

1. 서비스 소개 + 아키텍처 다이어그램 + 도메인 URL
2. 데이터 흐름: 공공 API → Redpanda → PyFlink → Iceberg(Lakekeeper) → DuckDB → 지도 UI
3. 차별화 포인트:
   - 1번 포트폴리오의 멱등성 미해결 이슈 → Iceberg MERGE INTO로 해결
   - 1번의 Hive Metastore → Lakekeeper REST Catalog로 차별화
   - dbt + GitHub Actions CI 도입
   - CDC 패턴 (Debezium) 도입
4. 측정 결과: 데이터 신선도 SLO P95
5. 운영 비용: 월 $0~$2
6. Phase 2 로드맵 명시 (실서비스 통합, UGC, Google Places, 추가 SLO)
```

→ Phase 1 단독 제출 시에도 면접관에게 "10일에 이만큼 + 다음 8주 계획이 명확" = **계획 수립 + 우선순위 판단 능력**으로 어필.

### Phase 1+2 통합 (5~6장)

1. 서비스 소개 + 아키텍처 (도메인 URL 명시)
2. 스택 선택 Trade-off (Redpanda vs Kafka, PyFlink vs Spark, Lakekeeper vs HMS, DuckDB vs Trino)
3. 이중 스트림 설계 (CDC + 행동 이벤트 + 공공 API), 1번 프로젝트의 멱등성 미해결 이슈 해결
4. 데이터 신선도 SLO 측정 결과 (4종)
5. 실사용자 데이터 인사이트 1~2개
6. 운영 비용 분석 (월 $0 운영의 Trade-off)

## 10. 기존 포트폴리오와의 연결 (서사 연속성)

- 1번 프로젝트(레시핑) 9페이지의 **"Dynamic Partition Overwrite로 멱등성 해결 예정"** 미해결 이슈 → 본 프로젝트 Phase 1에서 Iceberg MERGE INTO + 적절한 dedup key로 해결
- 1번에서 SCD Type 1 선택 → 본 프로젝트 Phase 1에서 `dim_place` SCD Type 2 골격, Phase 2에서 영업시간/별점 변경 이력 추적으로 본격화
- 2번 프로젝트(E-commerce)를 본 프로젝트로 대체 가능. 최종 포트폴리오는 **레시핑 + 본 프로젝트(P1 또는 P1+P2) 2개**로 정리

## 11. 기술 스택 차별화 표

| 영역 | 1번 (레시핑) | 2번 (E-commerce) | **본 프로젝트 P1** | **본 프로젝트 P2 추가** |
|------|-------------|----------------|--------------------|------------------------|
| 메시징 | Kafka 3-node | - | Redpanda single-node | (동일) |
| 스트림 처리 | Spark Structured Streaming | - | PyFlink | (동일) |
| 카탈로그 | Hive Metastore | - | Lakekeeper REST Catalog | (동일) |
| 쿼리 엔진 | Trino | Snowflake | DuckDB | + Trino single-node |
| 변환 | Spark SQL | Snowflake SQL | dbt-core | (동일) |
| DQ | dropDuplicates 수동 | MERGE INTO | dbt tests + pytest | + Great Expectations |
| 데이터 출처 | 시뮬레이터 | Kaggle | 공공 실시간 API + CDC | + 실사용자 + UGC + Google Places |
| CDC | 없음 | 없음 | Debezium | (동일) |
| CI/CD | 없음 | 없음 | GitHub Actions | (동일) |
| IaC | 없음 | 없음 | docker-compose | + Terraform |

## 12. 차별화 포인트 (면접 어필)

- **Phase 1 단독으로도** 신입 풀에서 보기 드문 조합: 공공 실시간 API + CDC + Lakekeeper + dbt + GitHub Actions + Iceberg MERGE INTO
- Phase 2 완성 시 **"직접 운영하는 실서비스 + 그 서비스의 데이터 플랫폼"** 동시 보유 사례 (희소성)
- **공공데이터 + 공간 데이터** = 한국 시장 (SK, KT, 네이버, 카카오모빌리티, 티맵 등)에 강한 어필
- 이종 소스 통합: P1 3개(공공 API 2 + CDC) → P2 6개 이상
- **비용 0원 운영** 자체가 FinOps 사고의 증거
- **합법 출처만 사용** = 데이터 거버넌스 감각 어필
- **Phase 분리 자체가 우선순위 판단 능력의 증거**

## 13. 주의점 / 리스크 / Fallback 트리거

### 일반 리스크

- **카카오맵·네이버지도 스크래핑 절대 금지**. ToS 위반 + 법적 분쟁 사례 다수. 가게 정보는 공공 인허가 + Google Places API + UGC만 사용
- **개인정보 회피** — 행동 이벤트는 익명 ID(쿠키)만, 로그인 없이
- **Oracle Cloud Always Free 회수 정책** — 90일 미사용 시 회수. 카드 등록 후 PAYG 업그레이드(과금 없음)로 회수 방지
- **공공 API rate limit** — 토큰 키마다 한도 있음. 캐싱·백오프 필요
- **Google Places API 비용** (P2) — 캐싱 전략 필수 (TTL 7~30일, 사용자가 본 가게 우선 갱신)
- **Redpanda single-node 한계** — 무중단 보장 어려움. "비용 vs 가용성" Trade-off로 명시

### Phase 1 데드라인 보호용 Fallback 트리거 (10일 사수)

| 트리거 조건 | Fallback 액션 |
|-------------|---------------|
| Day 0 (오늘): 서울시 OpenAPI 키 미신청 | **즉시 신청.** 승인 늦으면 Day 1~2를 fixture로 진행 |
| Day 3 끝까지 PyFlink Bronze→Silver가 Iceberg에 안 들어감 | **즉시 Spark Structured Streaming으로 전환.** 1번 프로젝트 코드 재활용. 차별화는 Lakekeeper + DuckDB + dbt + Debezium 조합으로 유지 |
| Lakekeeper REST Catalog 디버깅 2시간 초과 | **JdbcCatalog (SQLite/Postgres backed)로 우회.** 포트폴리오에는 "Lakekeeper 도입 시도 + 마주친 이슈" 솔직하게 기술 |
| Day 7 끝까지 Next.js 배포 안 됨 | **Streamlit + ngrok**으로 시각화 대체. 포트폴리오는 스크린샷 + 코드 위주 |
| Day 9 끝까지 시간 부족 | Day 10은 **포트폴리오 작성 절대 사수.** 코드 미완성 부분은 Phase 2로 명시적으로 밀기 |

## 14. 작업 환경

- **실제 코드/문서 위치**: `/Users/aryijq/Documents/01_DE_project/seoul-citydata-platform/` (본 디렉토리)
- **옵시디언 볼트 노출**: `/Users/aryijq/Documents/obsidian-vault/01 Projects/01-06 서울 도시데이터 플랫폼` (심볼릭 링크)
- 옵시디언에서 작성한 업무 일지·메모도 본 디렉토리에 함께 쌓임

## 15. 새 세션 시작 시 체크리스트 (Claude Code 용)

새 세션에서 본 디렉토리에 진입했을 때:

- [ ] 본 CLAUDE.md 전체를 끝까지 읽기
- [ ] **현재 어느 Phase / 어느 Day(P1) 또는 어느 W(P2)인지 사용자에게 확인**
- [ ] `git log --oneline -20` 으로 최근 진행 상황 파악
- [ ] `git status` 로 현재 작업 중이던 변경 사항 확인
- [ ] 프로젝트 루트의 `README.md`, `docs/` 가 있다면 함께 확인
- [ ] §13 fallback 트리거 조건이 활성화되었는지 확인
- [ ] 결정 사항 변경이 발생하면 본 문서의 해당 섹션을 업데이트할 것
- [ ] 한국어 존댓말로 응답할 것 (사용자 전역 규칙)

## 16. 관련 메모리·문서 참조

- 사용자 전역 규칙: `~/.claude/CLAUDE.md`
- 옵시디언 볼트 메모리 인덱스: `~/.claude/projects/-Users-aryijq-Documents-obsidian-vault/memory/MEMORY.md`
- 1번 포트폴리오 코드: `/Users/aryijq/Documents/01_DE_project/reciping-data-pipeline/`
- 포트폴리오 PDF: `/Users/aryijq/Documents/obsidian-vault/01 Projects/01-04 이력서/전상택 Data Engineer 포트폴리오.pdf`
