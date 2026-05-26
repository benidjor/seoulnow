import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildClientEvent,
  postEvents,
  type ClientEvent,
} from "@/lib/events-client";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

// jsdom 의 Blob 은 .text() 미구현 + node Response 와 호환 안 됨 → FileReader 로 읽는다
function readBlobText(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = () => reject(reader.error);
    reader.readAsText(blob);
  });
}

describe("buildClientEvent", () => {
  it("event_type + 현재 시각(ISO) + page.path + viewport 를 채운다", () => {
    const event = buildClientEvent("map_view");

    expect(event.event_type).toBe("map_view");
    // event_ts 는 round-trip 가능한 ISO8601 문자열
    expect(event.event_ts).toBe(new Date(event.event_ts).toISOString());
    expect(event.page?.path).toBe("/");
    expect(event.client?.viewport).toEqual({
      w: window.innerWidth,
      h: window.innerHeight,
    });
  });

  it("properties 를 넘기면 그대로 담는다", () => {
    const event = buildClientEvent("district_filter", { district: "강남구" });
    expect(event.properties).toEqual({ district: "강남구" });
  });

  it("anon_id / event_id 는 절대 담지 않는다 (Edge 가 찍음)", () => {
    const event = buildClientEvent("hotspot_click", { district: "강남구" });
    expect(event).not.toHaveProperty("anon_id");
    expect(event).not.toHaveProperty("event_id");
  });
});

describe("postEvents", () => {
  it("navigator.sendBeacon 으로 /api/v1/events 에 {events} body 를 보낸다", async () => {
    const beacon = vi.fn(() => true);
    vi.stubGlobal("navigator", { sendBeacon: beacon });
    const events: ClientEvent[] = [
      { event_ts: "2026-05-25T00:00:00.000Z", event_type: "map_view" },
    ];

    postEvents(events);

    expect(beacon).toHaveBeenCalledTimes(1);
    const [url, body] = beacon.mock.calls[0];
    expect(url).toBe("/api/v1/events");
    expect(body).toBeInstanceOf(Blob);
    expect((body as Blob).type).toBe("application/json");
    const text = await readBlobText(body as Blob);
    expect(JSON.parse(text)).toEqual({ events });
  });

  it("sendBeacon 미지원이면 fetch(keepalive:true) 로 fallback", () => {
    vi.stubGlobal("navigator", {});
    const fetchMock = vi.fn(async () => new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const events: ClientEvent[] = [
      { event_ts: "2026-05-25T00:00:00.000Z", event_type: "map_view" },
    ];

    postEvents(events);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/api/v1/events");
    expect(init.method).toBe("POST");
    expect(init.keepalive).toBe(true);
    expect(JSON.parse(init.body as string)).toEqual({ events });
  });

  it("sendBeacon 이 false 를 반환하면 fetch 로 fallback", () => {
    vi.stubGlobal("navigator", { sendBeacon: vi.fn(() => false) });
    const fetchMock = vi.fn(async () => new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    postEvents([{ event_ts: "t", event_type: "map_view" }]);

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("빈 배열이면 아무 것도 전송하지 않는다", () => {
    const beacon = vi.fn(() => true);
    const fetchMock = vi.fn();
    vi.stubGlobal("navigator", { sendBeacon: beacon });
    vi.stubGlobal("fetch", fetchMock);

    postEvents([]);

    expect(beacon).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("fetch fallback 이 reject 해도 throw 하지 않는다 (best-effort)", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("network");
      }),
    );

    expect(() =>
      postEvents([{ event_ts: "t", event_type: "map_view" }]),
    ).not.toThrow();
    // 거부된 promise 의 .catch 가 처리되도록 microtask flush (unhandled rejection 방지)
    await Promise.resolve();
  });
});
