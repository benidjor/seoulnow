# Phase 1B Week 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Phase 1A (Day 1-10) 완료 (`phase-1a-v1` tag, main HEAD 56522a9) 직후 Phase 1B 8일 (Day 11-18, 4일 → 8일 확장 결정 2026-05-14) 일정 — 익명 사용자 행동 토픽 + Cloudflare Edge API + D1 북마크 + Web Push + `fact_user_event` PyFlink 적재 + 회원가입 + 외부 가게 정보 (카카오/네이버) + 혼잡도 분류 mart + Apache Superset 대시보드 + Trino single-node + 강화 리포트 v2 (Phase 1A v1 7p → v2 12-14p).

**Architecture:** Phase 1A 의 Kafka KRaft single-node + PyFlink streaming (host process) + Iceberg Lakekeeper REST Catalog + dbt-core + Airflow LocalExecutor (본진 4 DAG) + GitHub Actions 스택 위에 Cloudflare Pages Functions Edge API + Cloudflare D1 + Workers Cron + VAPID Web Push + Postgres `users` + `places_external` + Apache Superset + Trino single-node 추가. spec §7 (Phase 1B 단일 출처) + `[[project-identity-correction]]` (5가지 의도) + `[[deferred-items-post-day10]]` §1 (카카오/네이버 결정) 단일 출처.

**Tech Stack:** Cloudflare Pages Functions (Edge API), FastAPI (HTTP receiver on Oracle Cloud), Cloudflare D1 (SQLite 5GB Free), Web Push (VAPID), Cloudflare Workers Cron, PyFlink (`user.events.v1` 처리), Iceberg `fact_user_event`, Postgres `users` + `places_external`, 카카오 로컬 API / 네이버 검색 API (Day 16 결정), dbt mart `congestion_grade_5min`, Apache Superset, Trino single-node, dbt-trino, Markdown 강화 리포트 v2.

---

## File Structure (Week 3 신규 / 수정 분만)

```
seoul-citydata-platform/
├── frontend/cloudflare-pages-functions/
│   ├── api/v1/events/POST.ts          (Day 11 신규 — Edge API receiver)
│   ├── api/bookmarks/                  (Day 12 신규 — D1 read/write)
│   ├── api/push/subscribe.ts           (Day 13 신규 — Web Push subscription)
│   ├── api/auth/signup.ts              (Day 15 신규 — 회원가입 endpoint)
│   └── lib/cookie-anon.ts              (Day 11 신규 — anon_id UUID 쿠키)
├── frontend/next-app/
│   ├── pages/privacy.tsx               (Day 13 신규 — 처리방침)
│   ├── components/BookmarkButton.tsx   (Day 12 신규 — 북마크 UI)
│   ├── components/PushSubscribe.tsx    (Day 13 신규 — Web Push subscribe)
│   ├── components/SignupForm.tsx       (Day 15 신규 — 회원가입 form)
│   └── public/sw.js                    (Day 13 신규 — service worker)
├── infra/http-receiver/                (Day 11 신규 — FastAPI receiver)
│   ├── Dockerfile
│   ├── app.py                          (Kafka producer)
│   └── requirements.txt
├── infra/workers/                      (Day 13 신규 — Cloudflare Workers Cron)
│   ├── alert-sender.ts                 (Web Push 발신)
│   └── wrangler.toml
├── infra/cloudflare-d1/                (Day 12 신규)
│   └── schema.sql                      (bookmarks + push_subscriptions)
├── infra/superset/                     (Day 17 신규 — profile=superset)
│   ├── Dockerfile
│   ├── superset_config.py
│   └── README.md
├── infra/trino/                        (Day 18 신규 — profile=trino)
│   ├── etc/config.properties
│   ├── etc/catalog/iceberg.properties
│   └── README.md
├── infra/postgres/migrations/
│   ├── 004_users.sql                   (Day 15 신규 — users 테이블)
│   └── 005_places_external.sql         (Day 16 신규 — 외부 가게 정보)
├── src/flink_jobs/
│   └── user_events_to_silver.py        (Day 14 신규 — user.events.v1 → silver)
├── src/scrapers/                       (Day 16 신규)
│   ├── kakao_local.py                  (카카오 로컬 API)
│   └── naver_local.py                  (네이버 검색 API or 스크래퍼)
├── dbt/seoul/models/marts/
│   ├── congestion_grade_5min.sql       (Day 17 신규 — 혼잡도 분류 mart)
│   ├── fact_user_event_hourly.sql      (Day 14 신규)
│   └── chill_open_now.sql              (Day 16 수정 — places_external join)
├── airflow/dags/
│   └── dbt_full_run.py                 (Day 14 수정 — fact_user_event_hourly 추가)
├── docker-compose.yml                  (Day 11/17/18 수정 — receiver + superset + trino)
└── docs/
    ├── runbook/day-11-edge-api.md
    ├── runbook/day-12-bookmark.md
    ├── runbook/day-13-web-push.md
    ├── runbook/day-14-user-event-flink.md
    ├── runbook/day-15-signup.md
    ├── runbook/day-16-external-place.md
    ├── runbook/day-17-grade-superset.md
    ├── runbook/day-18-trino-report.md
    ├── topics/user-events-v1-schema.md (Day 11 신규)
    ├── decisions/2026-05-DD-external-place-source.md (Day 16 신규)
    └── portfolio/phase1b_v2.md         (Day 18 신규 — 강화 리포트 12-14p)
```

---

## Conventions (Phase 1A Week 1 + Week 2 와 동일)

- TDD pure 함수 우선 / 인프라 작업은 검증 단계 명시.
- commit 한글 / type-scope 영어 / `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` trailer 의무.
- branch `phase-1b/day-<n>-<short-desc>` 패턴.
- 메모리 mitigation: Day 17 Superset + Day 18 Trino 기동 시 Phase 1A Day 9 Spark 패턴 reuse — `docker compose stop airflow-scheduler` (700MB 회수) + 야간 실행 (02-05 KST).
- Phase 1A 의 PyFlink mini-cluster (host process) + Iceberg Lakekeeper + 본진 Airflow 4 DAG + dbt + GitHub Actions 모두 그대로 reuse.

---

## Day 11 — `user.events.v1` 토픽 + Cloudflare Edge API + Oracle Cloud HTTP receiver

**목표 (spec §7-1, §7-2):** 브라우저 → Cloudflare Pages Functions Edge API → Cloudflare Tunnel → Oracle Cloud HTTP receiver (FastAPI) → Kafka `user.events.v1` 토픽 발행. 익명 ID 쿠키 UUID 발급 (1년 만료, IP 영구저장 X).

### Task 11.1: Cloudflare Pages Functions Edge API + anon_id 쿠키

**Files:** `frontend/cloudflare-pages-functions/api/v1/events/POST.ts` 신규, `lib/cookie-anon.ts` 신규.

**골격:** Cloudflare Edge runtime + Web Crypto `randomUUID()` + cookie set/get (`anon_id`, 1년 만료, Secure / HttpOnly / SameSite=Lax) + Tunnel HTTPS POST to HTTP receiver.

**검증:** `curl -X POST https://seoul-citydata.pages.dev/api/v1/events -H 'Content-Type: application/json' -d '{"event_type":"map_view","page":{"path":"/"}}'` → 200 응답 + `Set-Cookie: anon_id=<uuid>` 헤더.

**fallback (spec §9-2 Day 11):** (a) Postgres `events_inbox` + Debezium 으로 우회, (b) HTTP receiver 위치 변경.

### Task 11.2: Oracle Cloud HTTP receiver (FastAPI) + Kafka producer

**Files:** `infra/http-receiver/Dockerfile`, `app.py`, `requirements.txt` 신규. `docker-compose.yml` 에 receiver 서비스 추가 (network internal, Tunnel 통과만).

**골격:** FastAPI + Cloudflare Tunnel internal route (외부 IP 노출 X) + Kafka producer (`user.events.v1` 토픽, key=`anon_id`, acks=all).

**검증:** producer 로컬 발행 → `docker exec scp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user.events.v1 --from-beginning --max-messages 1` 1건 확인.

### Task 11.3: `user.events.v1` 토픽 생성 + event schema 명세

**Files:** `docker-compose.yml` topic init script 갱신 + `docs/topics/user-events-v1-schema.md` 신규 (`[[deferred-items-post-day10]]` §3 schema SoT).

**Schema** (deferred-items-post-day10 §3 SoT):
- `event_id` uuid (멱등 key)
- `event_ts` ISO8601
- `ingest_ts` Kafka header (receiver 가 채움)
- `anon_id` (쿠키, 항상 채워짐)
- `user_id` (null Phase 1B Day 11-14, Day 15 회원가입 후 채워짐) **= forward compatibility 핵심**
- `session_id` (탭 단위 또는 30분 idle)
- `event_type` enum: `map_view | hotspot_click | district_filter | bookmark_add | bookmark_remove | push_subscribe | push_unsubscribe | privacy_view | signup_complete`
- `page` {path, referrer}
- `client` {ua_hash salted, viewport}
- `properties` JSON (event_type 별 가변 schema)

**개인정보 회피**: IP 영구저장 X / ua_hash salted (salt 는 Cloudflare Workers secret) / `/privacy` 페이지 의무.

**검증:** schema 가 JSON Schema 으로 명세 + 단위 테스트 1건 (event_id uuid 검증 + event_type enum 검증).

**상세 step / 코드** = Day 11 진입 직전 plan-update commit 으로 별도 작성 (Phase 1A Week 2 의 점진적 패턴 reuse).

---

## Day 12 — Cloudflare D1 + 익명 북마크 UI

**목표 (spec §7-1):** D1 (SQLite 5GB Free) 에 `bookmarks(anon_id, region_id, created_at)` 테이블 신규 + Next.js 북마크 버튼 UI + D1 read/write API endpoint.

### Task 12.1: D1 schema + Wrangler binding

**Files:** `infra/cloudflare-d1/schema.sql` 신규 + `wrangler.toml` D1 binding 추가.

**Schema:**

```sql
CREATE TABLE bookmarks (
  anon_id TEXT NOT NULL,
  region_id TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (anon_id, region_id)
);
CREATE INDEX idx_bookmarks_anon ON bookmarks(anon_id);
```

**검증:** `wrangler d1 execute scp-d1 --file=infra/cloudflare-d1/schema.sql` → 테이블 생성 확인.

### Task 12.2: Next.js BookmarkButton 컴포넌트

**Files:** `frontend/next-app/components/BookmarkButton.tsx` + Pages Functions `/api/bookmarks/POST.ts` + `/api/bookmarks/[anon_id]/GET.ts`.

**골격:** hotspot 클릭 → POST `/api/bookmarks` (anon_id cookie + region_id body) → D1 insert. 메인 지도에서 북마크된 region 별표 표시.

**검증:** 브라우저에서 hotspot 클릭 → 북마크 저장 + 새로고침 시 별표 유지 확인.

### Task 12.3: D1 read API endpoint + Workers Cron 연결 준비

**Files:** Pages Functions `/api/bookmarks/[anon_id]/GET.ts`.

**골격:** D1 query `SELECT region_id FROM bookmarks WHERE anon_id = ?` → 응답 array. Day 13 Workers Cron 이 동일 query reuse.

**검증:** D1 query 응답 + Next.js 컴포넌트 display + Workers Cron prep query 동작 확인.

**fallback (spec §9-2 Day 12):** Postgres `bookmarks` 테이블로 대체 + Cloudflare Tunnel 로 Pages Functions 가 접근.

---

## Day 13 — Web Push + `/privacy` + Cloudflare Workers Cron 알림 발신

**목표 (spec §7-1, §7-4):** Service Worker + VAPID Web Push subscription → D1 저장 → Cloudflare Workers Cron (5분 주기) DuckDB → Iceberg 쿼리 → 북마크 동네 한가하면 Web Push 발신.

### Task 13.1: `/privacy` 페이지 + service worker 등록

**Files:** `frontend/next-app/pages/privacy.tsx` + `public/sw.js`.

**골격:** 1줄짜리 처리방침 페이지 ("익명 쿠키만 수집, 로그인·이메일·IP 영구저장 없음. Day 15 회원가입 후 email + password hash 저장. 행동 로그는 Kafka → Iceberg 직행 (D1 미저장).") + sw.js 등록.

**검증:** `/privacy` 접근 가능 + sw.js 등록 확인 (DevTools Application 패널).

### Task 13.2: Web Push subscription + D1 저장

**Files:** `frontend/next-app/components/PushSubscribe.tsx` + Pages Functions `/api/push/subscribe/POST.ts` + D1 schema 갱신 (`push_subscriptions` 테이블 신규).

**Schema:**

```sql
CREATE TABLE push_subscriptions (
  anon_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  p256dh TEXT NOT NULL,
  auth TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (anon_id, endpoint)
);
```

**검증:** 사용자 동의 후 D1 `push_subscriptions` 테이블 row insert 확인.

### Task 13.3: Cloudflare Workers Cron `alert-sender`

**Files:** `infra/workers/alert-sender.ts` + `wrangler.toml` cron schedule (`*/5 * * * *`).

**골격:** D1 query (active subscriptions) + DuckDB query (Iceberg `fact_hotspot_congestion_5min` 의 북마크 동네 평균 < 임계값) + `web-push` 라이브러리 발신.

**검증:** 임계값 미만일 때 Web Push 알림 수신 확인.

**fallback (spec §9-2 Day 13):** 메인 지도에 "내 북마크 동네 현황" 위젯 + 한가해지면 배지 표시 (Web Push 안 됨 시).

---

## Day 14 — `fact_user_event` PyFlink 적재 + dbt mart

**목표 (spec §7-1):** PyFlink 가 `user.events.v1` 토픽 consume → bronze `user_events_raw` (Iceberg) → silver `user_events` (validation + ua_hash salted + IP drop) → gold `fact_user_event` 적재. dbt mart 1건 (`fact_user_event_hourly`). Airflow `dbt_full_run` DAG TaskGroup 갱신.

### Task 14.1: PyFlink `user_events_to_silver.py` 신규

**Files:** `src/flink_jobs/user_events_to_silver.py` 신규 + Iceberg DDL `silver.user_events` (Lakekeeper REST 등록).

**골격:** Kafka source (`user.events.v1`) → JSON parse → validation (event_type enum + event_id uuid) → ua_hash salted hashing → silver Iceberg sink. forward compatibility 의 `user_id` 컬럼 nullable.

**검증:** `kafka-console-producer.sh --topic user.events.v1` 으로 1건 발행 → silver Iceberg 적재 확인 (DuckDB).

### Task 14.2: dbt mart `fact_user_event_hourly`

**Files:** `dbt/seoul/models/marts/fact_user_event_hourly.sql` 신규 (dbt-duckdb adapter Lakekeeper source 자동 read 안 됨 시 python model 변환, Day 6 Task 6.4 deviation D 패턴 reuse).

**골격:** event_type 별 hourly count + 자치구 단위 group by + dbt test (event_type not_null + count > 0 + user_id null 비율 모니터링).

**검증:** dbt run + dbt test PASS.

### Task 14.3: `dbt_full_run` DAG 에 `fact_user_event_hourly` 추가

**Files:** `airflow/dags/dbt_full_run.py` TaskGroup 갱신 (marts 그룹에 새 model 추가).

**검증:** Airflow UI 에서 manual trigger SUCCESS + XCom 결과 확인.

---

## Day 15 — 회원가입 + Postgres `users` 테이블 (Phase 2 W1 → Phase 1B 당김, 사용자 결정 2026-05-14)

**목표 (사용자 결정 2026-05-14):** 익명 `anon_id` → 식별 사용자 `user_id` 전환. Postgres `users` 테이블 신규 + 회원가입 form (email + password hash bcrypt) + `user.events.v1` 의 `user_id` 필드 채움 (forward compatibility 발휘).

### Task 15.1: Postgres `users` 테이블 migration

**Files:** `infra/postgres/migrations/004_users.sql` 신규 + Debezium connector 갱신 (`users` 도 CDC 대상, 새 토픽 `place.users.cdc.v1`).

**Schema:**

```sql
CREATE TABLE users (
  user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  anon_id TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_users_anon ON users(anon_id);
```

**검증:** `psql -c "\d users"` → 컬럼 확인 + Debezium 새 토픽 `place.users.cdc.v1` 발행 + Kafka consumer 1건 수신 확인.

### Task 15.2: Next.js SignupForm + API route

**Files:** `frontend/next-app/components/SignupForm.tsx` + Pages Functions `/api/auth/signup/POST.ts`.

**골격:** email + password 입력 → bcrypt hash (cost 12) → Postgres insert. 회원가입 후 cookie 에 `user_id` 추가 (anon_id 와 병기). 로그인은 Phase 2 W1 으로 미룸 (본 task 는 signup 만, 로그인 X).

**검증:** 브라우저에서 회원가입 진행 → Postgres row insert + 쿠키 user_id 설정 확인.

### Task 15.3: `user.events.v1` 의 `user_id` 채움 + Edge API 갱신

**Files:** `frontend/cloudflare-pages-functions/api/v1/events/POST.ts` 갱신 (cookie 에서 user_id 읽기).

**골격:** event payload 의 `user_id` = cookie.user_id ?? null. 익명 시점은 null 유지, 회원가입 후는 user_id 채워짐.

**검증:** 회원가입 후 발행되는 event 의 `user_id` 가 null 아닌 값 확인 + 익명 시점은 그대로 null.

---

## Day 16 — 카카오 / 네이버 외부 가게 정보 (영업시간 + 별점)

**목표 (`[[deferred-items-post-day10]]` §1 SoT, 사용자 결정 2026-05-14):** Google Places API 의 한국 데이터 정확도 한계 → 카카오 로컬 API 또는 네이버 검색 API 또는 스크래핑으로 카페·술집 영업시간 + 별점 도입. `places_external` Postgres 테이블 + dbt mart join. ToS 위반 risk 인지 + 채용 기대 낮음 입장 명시 (메모리 SoT).

### Task 16.1: 데이터 source 결정 + 의사결정 문서

**Files:** `docs/decisions/2026-05-DD-external-place-source.md` 신규.

**결정 항목** (Day 16 시작 시 사용자 결정):
- API 키 발급 가능 여부 (카카오 개발자 무료 vs 네이버 검색 API 무료 한도 vs 스크래핑)
- rate limit (카카오 = 일 300k 무료, 네이버 = 일 25k 무료)
- 스크래핑 ToS risk 인지 (네이버 소송 사례 다수, 카카오 cease-and-desist)
- 우선순위 = (1) 카카오 로컬 API → (2) 네이버 검색 API → (3) 스크래핑 (최후 옵션)

### Task 16.2: `places_external` 테이블 + scraper/fetcher

**Files:** `infra/postgres/migrations/005_places_external.sql` + `src/scrapers/kakao_local.py` + `src/scrapers/naver_local.py`.

**Schema:**

```sql
CREATE TABLE places_external (
  place_id TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('kakao','naver')),
  opening_hours JSONB,
  rating NUMERIC(3,1),
  last_fetched_at TIMESTAMP DEFAULT NOW(),
  raw_payload JSONB,
  PRIMARY KEY (place_id, source)
);
```

**골격:** Day 8 적재된 공공 인허가 places 의 `place_name + address` 로 카카오/네이버 검색 → opening_hours + rating 추출 → Postgres upsert. rate limit 안에서 batch (100 places / 5min).

**검증:** 100 row 적재 + 영업시간 정확도 sample 10건 수동 확인.

### Task 16.3: dbt mart `chill_open_now` 의 외부 영업시간 join

**Files:** `dbt/seoul/models/marts/chill_open_now.sql` 갱신.

**골격:** 기존 `chill_open_now` mart 가 공공 인허가 영업시간만 사용 → `places_external` (외부 source) join 추가. 외부 영업시간 우선 사용 + 공공 인허가 fallback. 별점 컬럼 추가.

**검증:** `/chill` 페이지에서 외부 별점 + 정확한 영업시간 표시 확인.

**fallback:** API 키 발급 안 되면 스크래핑 (ToS risk 인지). 또는 Day 16 작업 1일 → 0.5일 축소 + 별점만 도입.

---

## Day 17 — 혼잡도 분류 dbt mart (0.5d) + Apache Superset 대시보드 (0.5d)

**목표 (사용자 결정 2026-05-14, 옵션 a):** `fact_hotspot_congestion_5min.avg_congest_score` → 자치구 단위 등급 분류 (`여유` / `보통` / `약간 붐빔` / `붐빔`) mart 신규 + Apache Superset 단일 노드 기동 + 혼잡도 등급 색상 dashboard.

### Task 17.1: dbt mart `congestion_grade_5min` (0.5d)

**Files:** `dbt/seoul/models/marts/congestion_grade_5min.sql` 신규.

**골격:** `avg_congest_score` 임계값 매핑 — `< 25 = 여유`, `25-50 = 보통`, `50-75 = 약간 붐빔`, `>= 75 = 붐빔`. 자치구 단위 group by + 5min window. dbt test (등급 enum 검증 + 자치구 not_null).

**검증:** dbt run + dbt test PASS + DuckDB 쿼리 sample 결과 확인 (강남구 / 마포구 / 영등포구 3개 자치구 등급 분포).

### Task 17.2: Apache Superset 단일 노드 기동 (0.5d)

**Files:** `infra/superset/Dockerfile` + `superset_config.py` + `docker-compose.yml` profile=`superset` 추가 + `infra/superset/README.md`.

**골격:** Superset + DuckDB connector (Iceberg 직접 쿼리). 단일 dashboard: 자치구별 혼잡도 등급 heatmap (5min refresh) + `chill_open_now` mart count by 자치구.

**메모리 mitigation:** Day 17 작업 시작 직전 `docker compose stop airflow-scheduler` (700MB 회수) + Superset 작업 종료 후 재기동. Phase 1A Day 9 Spark 패턴 reuse.

**검증:** `http://localhost:8088` 접속 + dashboard 화면 정상 표시 + 5min refresh 확인.

**fallback:** Superset 셋업 4h 초과 시 Day 18 로 이월 + 강화 리포트에 "Superset 진행 중" 명시.

---

## Day 18 — Trino single-node (0.5d) + 강화 리포트 v2 (0.5d)

**목표 (사용자 결정 2026-05-14):** Trino single-node 기동 (Iceberg connector) + dbt-trino profile 추가 + 강화 리포트 v2 작성 (Phase 1A v1 의 7p → Phase 1A+1B v2 의 12-14p).

### Task 18.1: Trino single-node 기동 + Iceberg connector (0.5d)

**Files:** `infra/trino/etc/config.properties` + `etc/catalog/iceberg.properties` + `docker-compose.yml` profile=`trino` + `infra/trino/README.md`.

**골격:** Trino 단일 node (coordinator + worker 동일 process) + Iceberg REST catalog 연결 (Lakekeeper) + dbt-trino profile 추가 (기존 dbt-duckdb 와 병존).

**Config 예시:**

```properties
# infra/trino/etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog
iceberg.rest-catalog.warehouse=seoul
```

**메모리 mitigation:** Day 18 Trino 기동 시 Day 17 Superset 과 동일 패턴 (airflow-scheduler 일시 stop).

**검증:** `trino://localhost:8080` query `SELECT * FROM iceberg.gold.fact_hotspot_congestion_5min LIMIT 5` 성공 + dbt-trino profile run 1건 PASS.

### Task 18.2: 강화 리포트 v2 작성 (0.5d)

**Files:** `docs/portfolio/phase1b_v2.md` 신규 (Phase 1A v1 의 7p → Phase 1A+1B v2 의 12-14p).

**구조:**

- **p1-p7** = Phase 1A v1 의 모든 page (그대로 reuse, Phase 1A 종료 시점 결과 + Day 11 이후 평시 SLO 결과 §4.5 보강)
- **p8** = Phase 1B 익명 실서비스 아키텍처 (Edge API + REST Proxy + D1 + Web Push, Day 11-13 산출물)
- **p9** = `user.events.v1` 처리 + 익명 ID 거버넌스 (Day 14 + `/privacy`)
- **p10** = Phase 1B 트러블슈팅 (Web Push / D1 / Edge API → Kafka 등 Day 11-13 실측 이슈)
- **p11** = 회원가입 단계적 진화 (Day 15 anon_id → user_id forward compatibility 발휘)
- **p12** = 외부 가게 정보 도입 (카카오/네이버, Day 16) + ToS risk 입장
- **p13** = 혼잡도 분류 mart + Superset 대시보드 (Day 17)
- **p14** = Trino single-node + 운영 비용 갱신 (Day 18) + Phase 2 로드맵

**검증:** markdown 본문 12-14p 분량 + 모든 SLO / 비용 실측치 반영 + spec §7-5 의도 일치.

**fallback:** Day 18 시간 부족 시 강화 리포트는 Phase 1A v1 + Phase 1B Day 11-17 산출물 short summary 만 추가 (Phase 1B "진행 중 (Day 18 마감 중)" 솔직히 명시).

---

## 후속 작업 — 각 Day 진입 직전 plan-update commit

- 각 Day 의 task 상세 step (코드 본문 / DDL / API route / Wrangler config / Superset config / Trino config / dbt SQL) = 각 Day 진입 직전 plan-update commit 으로 작성. 골격만 박힌 본 plan SoT 는 Phase 1A Week 2 의 점진적 작성 패턴과 동일 (`[[airflow-decision]]` SoT).
- Day 11 진입 직전 plan-update = Task 11.1/11.2/11.3 의 TypeScript / Python / docker-compose 본문 작성.
- 이후 매 Day 진입 시점에 동일 패턴.
- spec §7 의 Day 11-14 일정 표 → Day 11-18 일정 표 갱신은 본 plan 머지 후 별도 spec PR 로 처리 (PR ε scope 외).

---

## Self-Review (writing-plans 스킬 §Self-Review)

### 1. Spec 커버리지 매핑

| Spec 항목 | 본 plan 의 task |
|---|---|
| spec §7-1 Day 11 — `user.events.v1` + Edge API | Task 11.1 / 11.2 / 11.3 |
| spec §7-1 Day 12 — D1 + 북마크 | Task 12.1 / 12.2 / 12.3 |
| spec §7-1 Day 13 — Web Push + Workers Cron | Task 13.1 / 13.2 / 13.3 |
| spec §7-1 Day 14 — `fact_user_event` PyFlink + 강화 리포트 | Task 14.1 / 14.2 / 14.3 + Day 18 Task 18.2 (강화 리포트 v2) |
| 사용자 결정 2026-05-14 — 회원가입 + users (Phase 2 W1 → Phase 1B 당김) | Day 15 Task 15.1 / 15.2 / 15.3 |
| 사용자 결정 2026-05-14 — 카카오/네이버 외부 정보 (Phase 2 W6 Google Places 대신) | Day 16 Task 16.1 / 16.2 / 16.3 |
| 사용자 결정 2026-05-14, 옵션 a — 혼잡도 분류 mart | Day 17 Task 17.1 |
| 사용자 결정 2026-05-14 — Superset + Trino 까지 Day 18 안 마무리 | Day 17 Task 17.2 + Day 18 Task 18.1 |
| spec §7-5 강화 리포트 (1A+1B 8-10p → 12-14p 확장) | Day 18 Task 18.2 |
| spec §7-2 Edge API → Kafka REST Proxy 패턴 | Task 11.1 + 11.2 |
| spec §7-3 익명 ID 거버넌스 | Task 11.1 + 11.3 + Task 13.1 (`/privacy`) |
| spec §7-4 Web Push + fallback | Task 13.1 / 13.2 / 13.3 |

### 2. Placeholder 스캔

- 각 task 의 상세 코드 step 은 "Day 진입 직전 plan-update commit 별도 작성" 명시 (Phase 1A Week 2 의 점진적 작성 패턴 일치).
- "TBD" / "implement later" → 0건. 모든 task 의 골격 (Files / 골격 / 검증 / fallback) 명확.
- 두 가지 예외 (Phase 1A Week 2 와 동일 패턴):
  1. Day 11-18 의 코드 본문 / 코드 step = 각 Day 진입 직전 plan-update commit 으로 작성. plan-update 패턴이 `[[airflow-decision]]` 메모리 SoT 와 일치.
  2. Task 16.1 의 데이터 source 결정 = Day 16 시작 시 결정 (API 키 발급 결과 + rate limit 실측 기반). Decision doc 양식만 박힘.

### 3. 타입 / 명명 일관성

- `anon_id` (cookie UUID) — Task 11.1 (Edge API 발급) + Task 11.3 (event schema) + Task 12.1 (D1 bookmarks) + Task 13.2 (D1 push_subscriptions) + Task 15.1 (Postgres users.anon_id 링크) 모두 동일 type / 명명.
- `user_id` (uuid, null Phase 1B Day 11-14, Day 15 회원가입 후 채워짐) — Task 11.3 (event schema) + Task 15.1 (Postgres users PK) + Task 15.3 (Edge API event payload) 모두 동일.
- `places_external.source` enum — `kakao` / `naver` (Task 16.1 결정 + Task 16.2 schema CHECK 제약).
- 혼잡도 등급 enum — `여유` / `보통` / `약간 붐빔` / `붐빔` (Task 17.1 dbt mart 임계값).
- Iceberg 카탈로그 alias `ice` + warehouse db `seoul` — Phase 1A 와 동일 (Day 14 user_events 도 동일).
- `event_type` enum (9 값) — Task 11.3 schema + Task 14.1 PyFlink validation + Task 14.2 dbt test 동일.

### 4. Out of Scope (Phase 2)

- 외부 공개·홍보 (OKKY/Reddit/Disquiet) = P2 W8
- UGC 별점 (사용자 직접 입력) = P2 W2
- 버스 위치 토픽 = P2 W3
- Great Expectations = P2 W5
- Terraform IaC = P2 W7
- Grafana Cloud Free = P2 W7
- 로그인 (signup 완료 후 다시 로그인) = P2 W1 (본 plan Day 15 는 signup 만)
- Google Places API 도입 = P2 W6 → 본 plan Day 16 의 카카오/네이버로 대체

### 5. Fallback 트리거 (spec §9-2 + 본 plan 신설)

| Day | 트리거 | Fallback |
|---|---|---|
| 11 | Edge API → Kafka 안 됨 | Postgres `events_inbox` + Debezium |
| 12 | D1 막힘 | Postgres `bookmarks` + Tunnel |
| 13 | Web Push 안 됨 | 모니터링 페이지 위젯 + 배지 |
| 14 | PyFlink `user_events_to_silver` 안 됨 | bronze 직접 적재만 + silver/gold 는 Phase 2 |
| 15 | 회원가입 schema 충돌 | Phase 2 W1 로 다시 이월 (Day 15 cancel, Day 11-14 산출물만 유지) |
| 16 | 카카오 API 키 안 됨 | 네이버 검색 API 또는 스크래핑 (ToS risk 인지) |
| 17 | Superset 셋업 4h 초과 | Day 18 로 이월 + 분류 mart 만 유지 |
| 18 | Trino 또는 강화 리포트 못 끝남 | Phase 1A v1 + Phase 1B Day 11-17 산출물 short summary 만 |

### 6. Phase 1A v1 §4.5 SLO 평시 결과 재측정 commit (옵션 D 후속)

- 본 측정 시점 (2026-05-14 02:06 KST) + 24h 후 (`slo_daily_report` DAG 자동 첫 실행, schedule `0 9 * * *`) 결과 인용한 별도 commit 으로 phase1a_v1.md §4.5 표 갱신.
- 적용 시점 = Day 14 fact_user_event PyFlink 적재 완료 후 또는 Day 18 강화 리포트 v2 작성 시점에 일괄 처리.

---

**Plan 작성 완료. Day 11 진입 직전 plan-update commit (Task 11.1/11.2/11.3 상세 step) 부터 시작.**
