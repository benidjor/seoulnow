import { describe, expect, it } from "vitest";
import { validateEvents } from "@/lib/event-validator";

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ANON = "11111111-2222-4333-8444-555566667777";

describe("validateEvents", () => {
  it("유효 이벤트 → anon_id / event_id stamp + rejected 0", () => {
    const raw = [{ event_ts: "2026-05-25T10:00:00Z", event_type: "map_view" }];
    const { valid, rejected } = validateEvents(raw, { anonId: ANON, uaHash: null });
    expect(rejected).toBe(0);
    expect(valid).toHaveLength(1);
    expect(valid[0].anon_id).toBe(ANON);
    expect(valid[0].event_type).toBe("map_view");
    expect(valid[0].event_ts).toBe("2026-05-25T10:00:00Z");
    expect(valid[0].event_id).toMatch(UUID_RE);
  });

  it("uaHash null → client.ua_hash 생략", () => {
    const raw = [
      { event_ts: "2026-05-25T10:00:00Z", event_type: "map_view", client: { viewport: { w: 390, h: 844 } } },
    ];
    const { valid } = validateEvents(raw, { anonId: ANON, uaHash: null });
    const client = valid[0].client as Record<string, unknown> | undefined;
    expect(client).toBeDefined();
    expect(client?.viewport).toEqual({ w: 390, h: 844 });
    expect(client?.ua_hash).toBeUndefined();
  });

  it("uaHash 주입 → client.ua_hash 설정 + viewport 보존", () => {
    const raw = [
      { event_ts: "2026-05-25T10:00:00Z", event_type: "hotspot_click", client: { viewport: { w: 390, h: 844 } } },
    ];
    const { valid } = validateEvents(raw, { anonId: ANON, uaHash: "deadbeef" });
    const client = valid[0].client as Record<string, unknown>;
    expect(client.ua_hash).toBe("deadbeef");
    expect(client.viewport).toEqual({ w: 390, h: 844 });
  });

  it("event_type enum 밖 → reject", () => {
    const raw = [{ event_ts: "2026-05-25T10:00:00Z", event_type: "evil_event" }];
    const { valid, rejected } = validateEvents(raw, { anonId: ANON, uaHash: null });
    expect(valid).toHaveLength(0);
    expect(rejected).toBe(1);
  });

  it("event_ts 누락 → reject", () => {
    const raw = [{ event_type: "map_view" }];
    const { valid, rejected } = validateEvents(raw, { anonId: ANON, uaHash: null });
    expect(valid).toHaveLength(0);
    expect(rejected).toBe(1);
  });

  it("객체가 아닌 항목 → reject", () => {
    const raw = ["nope", 42, null];
    const { valid, rejected } = validateEvents(raw, { anonId: ANON, uaHash: null });
    expect(valid).toHaveLength(0);
    expect(rejected).toBe(3);
  });

  it("혼합 배치 → 유효만 통과 + reject 카운트", () => {
    const raw = [
      { event_ts: "2026-05-25T10:00:00Z", event_type: "map_view" },
      { event_ts: "2026-05-25T10:00:01Z", event_type: "bad" },
      { event_type: "hotspot_click" },
      { event_ts: "2026-05-25T10:00:02Z", event_type: "district_filter" },
    ];
    const { valid, rejected } = validateEvents(raw, { anonId: ANON, uaHash: null });
    expect(valid).toHaveLength(2);
    expect(rejected).toBe(2);
    expect(valid.map((e) => e.event_type)).toEqual(["map_view", "district_filter"]);
  });

  it("page / properties 통과 + event_id 없으면 생성", () => {
    const raw = [
      {
        event_ts: "2026-05-25T10:00:00Z",
        event_type: "district_filter",
        page: { path: "/", referrer: null },
        properties: { gu: "Mapo" },
      },
    ];
    const { valid } = validateEvents(raw, { anonId: ANON, uaHash: null });
    expect(valid[0].page).toEqual({ path: "/", referrer: null });
    expect(valid[0].properties).toEqual({ gu: "Mapo" });
    expect(valid[0].event_id).toMatch(UUID_RE);
  });

  it("properties 가 null 이어도 통과", () => {
    const raw = [
      { event_ts: "2026-05-25T10:00:00Z", event_type: "map_view", properties: null },
    ];
    const { valid, rejected } = validateEvents(raw, { anonId: ANON, uaHash: null });
    expect(rejected).toBe(0);
    expect(valid[0].properties).toBeNull();
  });
});
