/**
 * 익명 행동 이벤트 배치 검증 + Edge stamp (Task 11.3 schema 와 정합).
 *
 * 클라이언트는 `{ event_ts, event_type, page?, client?, properties? }` 만 보낸다.
 * Edge 가 anon_id / event_id / client.ua_hash 를 찍는다 (design.md §7-3,
 * schemas/user_events_v1.json SoT). 본 함수는 순수/동기이며 ua_hash 는 route
 * handler 가 crypto.subtle 로 계산해 `uaHash` 로 주입한다.
 *
 * reject 조건: event_type 이 enum 밖이거나 event_ts 가 누락된 경우 (+ 객체가
 * 아닌 항목). 유효 이벤트는 receiver `POST /v1/events` 에 그대로 forward 된다.
 */

/** schemas/user_events_v1.json `event_type.enum` SoT 와 1:1. */
const EVENT_TYPES = new Set<string>([
  "map_view",
  "hotspot_click",
  "district_filter",
  "bookmark_add",
  "bookmark_remove",
  "push_subscribe",
  "push_unsubscribe",
  "privacy_view",
  "signup_complete",
]);

export interface StampContext {
  /** 쿠키에서 읽거나 새로 발급한 anon_id. */
  anonId: string;
  /** salt 가 있을 때 route 가 계산한 SHA-256 hex. 없으면 null → ua_hash 생략. */
  uaHash: string | null;
}

export interface ValidateResult {
  valid: Record<string, unknown>[];
  rejected: number;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function validateEvents(raw: unknown[], ctx: StampContext): ValidateResult {
  const valid: Record<string, unknown>[] = [];
  let rejected = 0;

  for (const item of raw) {
    if (!isPlainObject(item)) {
      rejected += 1;
      continue;
    }

    const eventType = item.event_type;
    const eventTs = item.event_ts;
    if (typeof eventType !== "string" || !EVENT_TYPES.has(eventType)) {
      rejected += 1;
      continue;
    }
    if (typeof eventTs !== "string" || eventTs.length === 0) {
      rejected += 1;
      continue;
    }

    const stamped: Record<string, unknown> = {
      event_id:
        typeof item.event_id === "string" && item.event_id.length > 0
          ? item.event_id
          : crypto.randomUUID(),
      event_ts: eventTs,
      anon_id: ctx.anonId,
      event_type: eventType,
    };

    if (isPlainObject(item.page)) stamped.page = item.page;

    const rawClient = isPlainObject(item.client) ? item.client : null;
    if (rawClient || ctx.uaHash) {
      const client: Record<string, unknown> = { ...(rawClient ?? {}) };
      if (ctx.uaHash) client.ua_hash = ctx.uaHash;
      stamped.client = client;
    }

    if ("properties" in item) stamped.properties = item.properties;

    valid.push(stamped);
  }

  return { valid, rejected };
}
