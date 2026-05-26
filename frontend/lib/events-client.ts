/**
 * 익명 사용자 행동 이벤트 발행 클라이언트 (`user.events.v1`).
 *
 * 브라우저는 이벤트 payload 만 만들어 `POST /api/v1/events` 로 보낸다.
 * `anon_id` / `event_id` 는 보내지 않는다 — Edge API(Cloudflare Pages Functions)
 * 가 쿠키 기반으로 찍는다 (spec §7-2/§7-3, [[deferred-items-post-day10]] §3).
 *
 * 전송은 페이지 이탈(언로드) 중에도 안전하도록 `navigator.sendBeacon` 을
 * 우선 사용하고, 미지원·실패 시 `fetch(keepalive: true)` 로 fallback 한다.
 * 발행은 best-effort — 실패가 화면(UX)을 막지 않는다.
 */

const EVENTS_ENDPOINT = "/api/v1/events";

/** `user.events.v1` event_type enum (deferred-items-post-day10 §3 SoT). */
export type EventType =
  | "map_view"
  | "hotspot_click"
  | "district_filter"
  | "bookmark_add"
  | "bookmark_remove"
  | "push_subscribe"
  | "push_unsubscribe"
  | "privacy_view"
  | "signup_complete";

/**
 * 브라우저가 보내는 이벤트 payload. `anon_id` / `event_id` / `ingest_ts` 는
 * Edge·receiver 가 채우므로 여기에 포함하지 않는다.
 */
export interface ClientEvent {
  event_ts: string;
  event_type: EventType;
  page?: { path: string; referrer?: string };
  client?: { viewport?: { w: number; h: number } };
  properties?: Record<string, unknown>;
}

/**
 * 현재 브라우저 컨텍스트(시각·경로·뷰포트)로 `ClientEvent` 를 만든다.
 * 서버 렌더 등 `window` 가 없는 환경에서는 page/client 를 생략한다.
 */
export function buildClientEvent(
  eventType: EventType,
  properties?: Record<string, unknown>,
): ClientEvent {
  const event: ClientEvent = {
    event_ts: new Date().toISOString(),
    event_type: eventType,
  };

  if (typeof window !== "undefined") {
    const referrer = typeof document !== "undefined" ? document.referrer : "";
    event.page = {
      path: window.location.pathname,
      ...(referrer ? { referrer } : {}),
    };
    event.client = {
      viewport: { w: window.innerWidth, h: window.innerHeight },
    };
  }

  if (properties) event.properties = properties;
  return event;
}

/**
 * 이벤트 배열을 `POST /api/v1/events` 로 전송한다 (body `{ events }`).
 * 빈 배열은 무시. sendBeacon 우선, 미지원·반환 false 시 fetch fallback.
 */
export function postEvents(events: ClientEvent[]): void {
  if (events.length === 0) return;

  const body = JSON.stringify({ events });

  if (
    typeof navigator !== "undefined" &&
    typeof navigator.sendBeacon === "function"
  ) {
    const blob = new Blob([body], { type: "application/json" });
    if (navigator.sendBeacon(EVENTS_ENDPOINT, blob)) return;
    // sendBeacon 이 false (queue full 등) → fetch 로 fallback
  }

  void fetch(EVENTS_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    keepalive: true,
  }).catch(() => {
    /* best-effort: 발행 실패가 UX 를 막지 않는다 */
  });
}
