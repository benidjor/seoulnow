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

> Day 11 본문 = plan-update commit 으로 상세 step 박힘 (Phase 1A Week 2 Task 9.3 / 10.3 점진적 작성 패턴 reuse, `[[airflow-decision]]` SoT). Task 11.1 (Edge API) + Task 11.2 (FastAPI receiver) + Task 11.3 (topic schema) 의 코드 골격 / TDD step / 검증 명령 / fallback 모두 명시.

### Task 11.1: Cloudflare Pages Functions Edge API + anon_id 쿠키

**Files:**
- Create: `frontend/cloudflare-pages-functions/api/v1/events/POST.ts` — Edge API entry, anon_id 쿠키 발급 / 검증 + Tunnel HTTPS forward
- Create: `frontend/cloudflare-pages-functions/lib/cookie-anon.ts` — `getOrCreateAnonId(request, response)` helper. Web Crypto `randomUUID()` + Set-Cookie 헤더
- Create: `frontend/cloudflare-pages-functions/lib/event-validator.ts` — 클라이언트 전달 payload 의 `event_type` enum / 필수 필드 검증 (Task 11.3 의 schema 와 정합)
- Modify: `frontend/next-app/lib/events-client.ts` — 브라우저 측 fetch wrapper (`postEvents(events: Event[])`)

**Goal:** 브라우저가 `POST /api/v1/events` 1회 → Edge API 가 (a) cookie 에 anon_id 있으면 reuse / 없으면 UUID 발급 (b) payload 의 event_type / required field 검증 (c) Tunnel HTTPS 로 `https://receiver.internal/v1/events` POST forward (Bearer token 헤더 = Cloudflare Workers secret) (d) `Set-Cookie: anon_id=<uuid>; Max-Age=31536000; Secure; HttpOnly; SameSite=Lax` 응답.

**TypeScript 골격 (Cloudflare Pages Functions context):**

```typescript
// frontend/cloudflare-pages-functions/api/v1/events/POST.ts
import { getOrCreateAnonId } from "../../../lib/cookie-anon";
import { validateEvents } from "../../../lib/event-validator";

interface Env {
  RECEIVER_URL: string;       // https://receiver.internal/v1/events (Tunnel 내부)
  RECEIVER_TOKEN: string;     // Bearer 토큰 (Workers secret)
  ANON_UA_SALT: string;       // ua_hash salt (Workers secret)
}

export const onRequestPost: PagesFunction<Env> = async ({ request, env }) => {
  const response = new Response();
  const anonId = getOrCreateAnonId(request, response);

  const payload = await request.json<{ events: unknown[] }>();
  const validated = validateEvents(payload.events ?? [], { anonId, salt: env.ANON_UA_SALT });

  const forward = await fetch(env.RECEIVER_URL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${env.RECEIVER_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ events: validated }),
  });

  if (!forward.ok) {
    return new Response(JSON.stringify({ error: "forward_failed" }), { status: 502, headers: response.headers });
  }
  return new Response(JSON.stringify({ accepted: validated.length }), { status: 200, headers: response.headers });
};
```

```typescript
// frontend/cloudflare-pages-functions/lib/cookie-anon.ts
const ANON_COOKIE = "anon_id";
const ONE_YEAR_S = 60 * 60 * 24 * 365;

export function getOrCreateAnonId(request: Request, response: Response): string {
  const cookie = request.headers.get("Cookie") ?? "";
  const match = cookie.match(/anon_id=([0-9a-f-]{36})/);
  if (match) return match[1];

  const anonId = crypto.randomUUID();
  response.headers.append(
    "Set-Cookie",
    `${ANON_COOKIE}=${anonId}; Max-Age=${ONE_YEAR_S}; Path=/; Secure; HttpOnly; SameSite=Lax`,
  );
  return anonId;
}
```

**검증:**
- `curl -X POST https://seoul-citydata.pages.dev/api/v1/events -H 'Content-Type: application/json' -d '{"events":[{"event_type":"map_view","event_ts":"2026-05-15T01:00:00Z","page":{"path":"/"}}]}' -i` → 200 응답 + `Set-Cookie: anon_id=<uuid>` 헤더 (첫 요청) + `{"accepted":1}` body
- 두 번째 호출 시 cookie reuse 확인 (`Set-Cookie` 헤더 부재 또는 동일 anon_id)
- Cloudflare Pages preview deployment 1회 정상 build (Wrangler `npx wrangler pages dev` 로컬 동작 검증)

**fallback (spec §9-2 Day 11):**
- (a) Cloudflare Tunnel 셋업 4h 초과 → Postgres `events_inbox` 테이블 + Debezium 토픽으로 우회. Edge API 가 Pages Functions 가 아니라 Next.js API route 로 받고 Postgres 에 insert
- (b) HTTP receiver 위치 변경 — Oracle Cloud 가 아닌 동일 VM 의 receiver container (Tunnel 없이 internal docker network 로만 노출, Pages Functions 가 직접 접근 불가 → 회피 안 1 채택)

### Task 11.2: Oracle Cloud HTTP receiver (FastAPI) + Kafka producer

**Files:**
- Create: `infra/http-receiver/Dockerfile` — `python:3.11-slim` 베이스, uv 의존성 install
- Create: `infra/http-receiver/app.py` — FastAPI app + aiokafka producer + Bearer token 검증
- Create: `infra/http-receiver/requirements.txt` — `fastapi==0.115.*`, `uvicorn[standard]==0.32.*`, `aiokafka==0.12.*`, `pydantic==2.*`
- Create: `tests/integration/test_http_receiver.py` — TestClient + 모킹 Kafka producer (3 case: 정상 / 401 / 422)
- Modify: `docker-compose.yml` — `http-receiver` 서비스 추가 (profile=`receiver`, network=`scp_net`, depends_on=kafka, 외부 노출 X)

**Goal:** FastAPI receiver 가 `POST /v1/events` 수신 → (a) `Authorization: Bearer <token>` 검증 (불일치 401) (b) payload pydantic 검증 (422) (c) 각 event 를 Kafka `user.events.v1` 토픽으로 발행 (`key = event.anon_id`, `acks=all`, header `ingest_ts` 채움) (d) `{"published": <n>}` 응답.

**Python 골격:**

```python
# infra/http-receiver/app.py
from __future__ import annotations
import os
import json
import uuid
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
RECEIVER_TOKEN = os.environ["RECEIVER_TOKEN"]
TOPIC = "user.events.v1"

producer: AIOKafkaProducer | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        enable_idempotence=True,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    await producer.start()
    yield
    await producer.stop()

app = FastAPI(lifespan=lifespan)

class IncomingEvent(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_ts: datetime
    anon_id: uuid.UUID
    user_id: uuid.UUID | None = None
    session_id: str | None = None
    event_type: str
    page: dict | None = None
    client: dict | None = None
    properties: dict | None = None

class EventBatch(BaseModel):
    events: list[IncomingEvent]

@app.post("/v1/events")
async def post_events(batch: EventBatch, authorization: str = Header(...)) -> dict:
    if authorization != f"Bearer {RECEIVER_TOKEN}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token")
    if producer is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "producer not ready")

    ingest_ts = datetime.now(timezone.utc).isoformat()
    for ev in batch.events:
        await producer.send_and_wait(
            TOPIC,
            value=ev.model_dump(mode="json"),
            key=str(ev.anon_id),
            headers=[("ingest_ts", ingest_ts.encode("utf-8"))],
        )
    return {"published": len(batch.events)}
```

```dockerfile
# infra/http-receiver/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8400
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8400"]
```

**docker-compose.yml 추가 서비스:**

```yaml
http-receiver:
  build: ./infra/http-receiver
  container_name: scp-http-receiver
  profiles: ["receiver"]
  networks: [scp_net]
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    RECEIVER_TOKEN: ${RECEIVER_TOKEN}
  depends_on:
    kafka: { condition: service_healthy }
```

**TDD 단계 (3 case, pytest + TestClient):**
- Step 1: 실패 테스트 작성
  - `test_post_events_unauthorized()` — Bearer 토큰 불일치 → 401
  - `test_post_events_invalid_payload()` — `event_ts` 누락 → 422
  - `test_post_events_publishes_to_kafka()` — 정상 payload → 200 + 모킹 producer `send_and_wait` 1회 호출 + ingest_ts header 부착 확인
- Step 2: 테스트 fail 확인 (`pytest tests/integration/test_http_receiver.py -v`)
- Step 3: `app.py` 본문 작성 (위 골격)
- Step 4: 테스트 PASS 확인 (3 PASS)
- Step 5: `docker compose --profile receiver up -d http-receiver` 후 호스트에서 `curl -X POST http://localhost:8400/v1/events -H 'Authorization: Bearer <token>'` smoke 1회

**검증 명령:**
- `pytest tests/integration/test_http_receiver.py -v` → 3 PASS
- producer 로컬 발행 후 `docker exec scp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user.events.v1 --from-beginning --max-messages 1` 1건 수신 + JSON payload 의 `event_type` / `anon_id` / `event_ts` 필드 확인
- `docker compose --profile receiver logs http-receiver | grep "Started server process"` 정상 boot 확인

**fallback:** aiokafka 의존성 build 실패 시 `confluent-kafka-python` 으로 교체 (레시핑에서 사용한 라이브러리, ClassLoader 충돌 없음).

### Task 11.3: `user.events.v1` 토픽 생성 + event schema 명세

**Files:**
- Modify: `infra/kafka/create_topics.sh` — `user.events.v1` 토픽 init (`--partitions 6 --replication-factor 1 --config retention.ms=2592000000`, 30일 retention)
- Create: `docs/topics/user-events-v1-schema.md` — JSON Schema + 필드별 의도 + 개인정보 회피 정책 + Task 11.1/11.2 연결 (`[[deferred-items-post-day10]]` §3 SoT)
- Create: `infra/http-receiver/schemas/user_events_v1.json` — JSON Schema 본문 (FastAPI pydantic 모델과 1:1 매핑)
- Create: `tests/unit/test_user_events_schema.py` — JSON Schema 검증 2 case (event_id uuid + event_type enum)

**Schema 본문** (deferred-items-post-day10 §3 SoT, JSON Schema Draft 2020-12):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://seoul-citydata.pages.dev/schemas/user_events_v1.json",
  "title": "user.events.v1",
  "type": "object",
  "required": ["event_id", "event_ts", "anon_id", "event_type"],
  "properties": {
    "event_id": { "type": "string", "format": "uuid" },
    "event_ts": { "type": "string", "format": "date-time" },
    "ingest_ts": { "type": "string", "format": "date-time", "description": "receiver 가 Kafka header 로 채움" },
    "anon_id": { "type": "string", "format": "uuid" },
    "user_id": { "type": ["string", "null"], "format": "uuid", "description": "Day 15 회원가입 후 채워짐, forward compatibility" },
    "session_id": { "type": ["string", "null"], "maxLength": 64 },
    "event_type": {
      "type": "string",
      "enum": [
        "map_view", "hotspot_click", "district_filter",
        "bookmark_add", "bookmark_remove",
        "push_subscribe", "push_unsubscribe",
        "privacy_view", "signup_complete"
      ]
    },
    "page": {
      "type": "object",
      "properties": {
        "path": { "type": "string", "maxLength": 256 },
        "referrer": { "type": ["string", "null"], "maxLength": 256 }
      }
    },
    "client": {
      "type": "object",
      "properties": {
        "ua_hash": { "type": "string", "description": "salted SHA-256, IP 영구저장 X" },
        "viewport": { "type": "object", "properties": { "w": {"type": "integer"}, "h": {"type": "integer"} } }
      }
    },
    "properties": { "type": ["object", "null"], "description": "event_type 별 가변 schema" }
  },
  "additionalProperties": false
}
```

**개인정보 회피 (spec §7-3):** IP 영구저장 X / `ua_hash` = SHA-256(ua + salt) (salt = Cloudflare Workers secret `ANON_UA_SALT`) / `/privacy` 페이지 의무 (Day 13 Task 13.1).

**TDD 단계 (2 case + topic init smoke):**
- Step 1: 실패 테스트 작성
  - `test_event_id_must_be_uuid()` — `event_id` 가 uuid 형식 아니면 ValidationError
  - `test_event_type_must_be_in_enum()` — `event_type` 이 enum 외 값이면 ValidationError
- Step 2: 테스트 fail 확인
- Step 3: `schemas/user_events_v1.json` 본문 + `jsonschema` 검증 helper 작성
- Step 4: 테스트 PASS 확인 (2 PASS)
- Step 5: `./infra/kafka/create_topics.sh` 재실행 → `user.events.v1` 토픽 생성 (`docker exec scp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic user.events.v1` 으로 partitions=6 + retention 확인)

**검증 명령:**
- `pytest tests/unit/test_user_events_schema.py -v` → 2 PASS
- `docker exec scp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic user.events.v1` → 6 partitions / retention.ms=2592000000 확인
- Task 11.1 + 11.2 의 end-to-end smoke (브라우저 1회 → Edge API → receiver → Kafka 1 row) 후 `docs/topics/user-events-v1-schema.md` 의 schema 와 실 메시지 payload 일치 확인

---

### Day 11 종료 게이트 (3 task 통합)

- [ ] Task 11.1 — `curl` smoke 200 + Set-Cookie 헤더 + 두 번째 호출 reuse
- [ ] Task 11.2 — pytest 3 PASS + Kafka consumer 1건 수신
- [ ] Task 11.3 — pytest 2 PASS + 토픽 partitions=6 + 30일 retention
- [ ] end-to-end smoke — 브라우저 → Pages Functions → Tunnel → receiver → Kafka 1 row + payload schema 일치
- [ ] `docs/runbook/day-11-edge-api.md` 작성 (운영 절차 + 환경 편차 + Cloudflare Tunnel secret 발급 단계)

---

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

**골격:** `avg_congest_score` 임계값 매핑 (실측 enum 1-4 기준 + Day 8 `chill_open_now` 정합) —

| `avg_congest_score` 구간 | 등급 |
|---|---|
| `<= 1.5` | `여유` |
| `(1.5, 2.0]` | `보통` |
| `(2.0, 3.0]` | `약간 붐빔` |
| `> 3.0` | `붐빔` |

자치구 단위 group by + 5min window. dbt test (등급 enum 검증 + 자치구 not_null).

**임계값 SoT 정정 (2026-05-17, plan-update PR):** 본 plan 의 직전 임계값 가정 (`<25 / 25-50 / 50-75 / >=75`, 1-100 스케일) 은 silver 실측과 불일치. 정정 근거 3건:

1. **실측 `congest_level_score` enum** — `src/flink_jobs/lib/transforms.py:10-15` 의 `CONGEST_LEVEL_MAP = {"여유": 1, "보통": 2, "약간 붐빔": 3, "붐빔": 4}` + NULL 처리 0. `dbt/seoul/models/marts/schema.yml` 의 `accepted_values: [0, 1, 2, 3, 4]` SoT 일치.
2. **mart `avg_congest_score` 실측 범위** — `src/flink_jobs/silver_to_gold.py:132` `AVG(CAST(congest_level_score AS DOUBLE))` + staging `> 0` filter → 실측 1.0-4.0 (fractional, 자치구 평균).
3. **Day 8 `chill_open_now` 정합** — `dbt/seoul/models/marts/schema.yml` 의 `chill_open_now.avg_congest_score <= 2` (한가 후보 임계값) 이미 정착. Option B 의 `여유 ⊥ 보통` 경계 = `1.5`, `보통 ⊥ 약간 붐빔` 경계 = `2.0` 으로 chill 의 `<= 2` 경계와 일치 → 한가 = `여유` ∪ `보통` 으로 등급 정의 정합.

**검증:** dbt run + dbt test PASS + DuckDB 쿼리 sample 결과 확인 (강남구 / 마포구 / 영등포구 3개 자치구 등급 분포). `accepted_values: ["여유", "보통", "약간 붐빔", "붐빔"]` enum test + 4개 등급 모두 1건 이상 분포 검증.

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
