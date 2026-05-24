# Kafka 토픽 `user.events.v1` — 익명 사용자 행동 이벤트

> Phase 1B Day 11 (Task 11.2 / 11.3) 신규. 익명 쿠키 기반 사용자 행동 로그를
> Kafka 메시지 버스로 인입한다. **D1 에 저장하지 않고 Kafka → Iceberg 직행**
> (CLAUDE.md §3 사용자 메타 저장소 정책).
>
> 계약 단일 출처(JSON Schema) = [`infra/http-receiver/schemas/user_events_v1.json`](../../infra/http-receiver/schemas/user_events_v1.json).
> event schema 안의 원 출처 = `[[deferred-items-post-day10]]` §3.

## 1. 토픽 설정

| 항목 | 값 | 사유 |
|------|----|----|
| topic | `user.events.v1` | spec §4 Kafka 토픽 표 |
| partitions | 6 | 행동 이벤트 트래픽 + 컨슈머 병렬도 여유 |
| replication-factor | 1 | KRaft single-node (가용성 trade-off, CLAUDE.md §3) |
| retention.ms | 2,592,000,000 (30일) | replay / 백필 여유 |
| compression.type | lz4 | `create_topics.sh` 공통 기본값 |
| key | `anon_id` | 동일 익명 사용자 이벤트 순서 보장 (파티션 고정) |

토픽 생성 = [`infra/kafka/create_topics.sh`](../../infra/kafka/create_topics.sh) (멱등 — 이미 존재 시 skip).

## 2. 필드 명세

JSON Schema Draft 2020-12. `additionalProperties: false` (미정의 필드 거부).

| 필드 | 타입 | 필수 | 의도 |
|------|------|------|------|
| `event_id` | string (uuid) | ✅ | 이벤트 멱등 키. consumer dedup 기준. |
| `event_ts` | string (date-time) | ✅ | 브라우저 발생 시각 (ISO 8601). |
| `ingest_ts` | string (date-time) | — | **receiver 가 Kafka header 로 채움** (서버 수신 시각). payload 가 아닌 header. |
| `anon_id` | string (uuid) | ✅ | 익명 쿠키 ID. Kafka key. |
| `user_id` | string (uuid) \| null | — | Day 15 회원가입 후 채워짐. **forward compatibility 핵심** — 익명 → 식별 전환. |
| `session_id` | string(≤64) \| null | — | 탭 또는 30분 idle 단위 세션. |
| `event_type` | enum | ✅ | 아래 §3 enum. |
| `page` | object | — | `{ path(≤256), referrer(≤256)\|null }`. |
| `client` | object | — | `{ ua_hash, viewport{w,h} }`. `ua_hash` = salted SHA-256. |
| `properties` | object \| null | — | `event_type` 별 가변 schema. |

## 3. `event_type` enum (9종)

| 값 | 의미 | 도입 |
|----|------|------|
| `map_view` | 메인 지도 노출 | Day 11 |
| `hotspot_click` | 핫스팟 마커 클릭 | Day 11 |
| `district_filter` | 자치구 필터 변경 | Day 11 |
| `bookmark_add` / `bookmark_remove` | 북마크 추가/삭제 | Day 12 |
| `push_subscribe` / `push_unsubscribe` | Web Push 구독/해지 | Day 13 |
| `privacy_view` | `/privacy` 페이지 조회 | Day 13 |
| `signup_complete` | 회원가입 완료 (익명→식별 전환) | Day 15 |

## 4. 개인정보 회피 (spec §7-3 / CLAUDE.md §13)

- **IP 영구저장 X.** receiver 는 IP 를 로그/payload 에 남기지 않는다.
- `client.ua_hash` = `SHA-256(user_agent + salt)`. salt = Cloudflare Workers secret `ANON_UA_SALT`. 원문 UA 미저장.
- 식별 정보(이메일·전화 등) 일절 수집 안 함. `user_id` 는 Day 15 자체 발급 UUID 일 뿐 PII 아님.
- `/privacy` 1줄 처리방침 페이지 의무 (Day 13 Task 13.1).

## 5. 파이프라인 연결

```
브라우저 → Edge API (Pages Functions, Task 11.1)
        → HTTP receiver (FastAPI, Task 11.2) → Kafka user.events.v1 (Task 11.3)
        → [Day 14+] PyFlink/Spark → Iceberg Gold fact_user_event
```

- **Task 11.1** (다른 PR): Edge API 가 `anon_id` 쿠키 발급 + 이벤트 배치를 receiver 로 forward.
- **Task 11.2** (본 PR): receiver 가 Bearer 토큰 검증 → pydantic 검증 → `key=anon_id`, header `ingest_ts` 로 Kafka 발행.
- **Task 11.3** (본 PR): 본 토픽 + 스키마 계약.

receiver 의 pydantic `IncomingEvent` 모델은 본 JSON Schema 와 1:1 필드 매핑이다
(런타임 검증 = pydantic, 계약/문서/단위테스트 = JSON Schema).
