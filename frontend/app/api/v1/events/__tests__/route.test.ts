import { afterEach, describe, expect, it, vi } from "vitest";
import { POST } from "@/app/api/v1/events/route";

const VALID_BODY = {
  events: [{ event_ts: "2026-05-25T10:00:00Z", event_type: "map_view" }],
};

function postReq(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request("https://seoulnow.live/api/v1/events", {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

function mockFetch(impl: (url: string, init: RequestInit) => Promise<Response>) {
  const fn = vi.fn(impl);
  vi.stubGlobal("fetch", fn);
  return fn;
}

async function sha256Hex(input: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("POST /api/v1/events", () => {
  it("env 미설정 → 503 + x-degraded-reason", async () => {
    vi.stubEnv("EVENTS_RECEIVER_BASE", "");
    vi.stubEnv("RECEIVER_TOKEN", "");
    const res = await POST(postReq(VALID_BODY));
    expect(res.status).toBe(503);
    expect(res.headers.get("x-degraded-reason")).toBe("events_receiver_unconfigured");
  });

  it("정상 forward → 200 accepted + Bearer + /v1/events + Set-Cookie(신규)", async () => {
    vi.stubEnv("EVENTS_RECEIVER_BASE", "https://recv.example");
    vi.stubEnv("RECEIVER_TOKEN", "secret-token");
    const fetchFn = mockFetch(async () =>
      new Response(JSON.stringify({ published: 1 }), { status: 200 }),
    );

    const res = await POST(postReq(VALID_BODY));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ accepted: 1 });
    expect(res.headers.get("set-cookie")).toContain("anon_id=");

    const [url, init] = fetchFn.mock.calls[0];
    expect(url).toBe("https://recv.example/v1/events");
    expect(init.method).toBe("POST");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer secret-token");
    const sent = JSON.parse(init.body as string);
    expect(sent.events).toHaveLength(1);
    expect(sent.events[0].anon_id).toMatch(/^[0-9a-f-]{36}$/i);
  });

  it("기존 anon_id 쿠키 → Set-Cookie 없음 + 동일 anon_id forward", async () => {
    vi.stubEnv("EVENTS_RECEIVER_BASE", "https://recv.example");
    vi.stubEnv("RECEIVER_TOKEN", "secret-token");
    const existing = "11111111-2222-4333-8444-555566667777";
    const fetchFn = mockFetch(async () =>
      new Response(JSON.stringify({ published: 1 }), { status: 200 }),
    );

    const res = await POST(postReq(VALID_BODY, { cookie: `anon_id=${existing}` }));
    expect(res.headers.get("set-cookie")).toBeNull();
    const sent = JSON.parse(fetchFn.mock.calls[0][1].body as string);
    expect(sent.events[0].anon_id).toBe(existing);
  });

  it("ANON_UA_SALT 설정 → client.ua_hash = SHA-256(UA + salt)", async () => {
    vi.stubEnv("EVENTS_RECEIVER_BASE", "https://recv.example");
    vi.stubEnv("RECEIVER_TOKEN", "secret-token");
    vi.stubEnv("ANON_UA_SALT", "pepper");
    const fetchFn = mockFetch(async () =>
      new Response(JSON.stringify({ published: 1 }), { status: 200 }),
    );

    const ua = "Mozilla/5.0 (test)";
    await POST(postReq(VALID_BODY, { "user-agent": ua }));
    const sent = JSON.parse(fetchFn.mock.calls[0][1].body as string);
    expect(sent.events[0].client.ua_hash).toBe(await sha256Hex(ua + "pepper"));
  });

  it("upstream 실패 → 502 + x-degraded-reason", async () => {
    vi.stubEnv("EVENTS_RECEIVER_BASE", "https://recv.example");
    vi.stubEnv("RECEIVER_TOKEN", "secret-token");
    mockFetch(async () => new Response("nope", { status: 500 }));

    const res = await POST(postReq(VALID_BODY));
    expect(res.status).toBe(502);
    expect(res.headers.get("x-degraded-reason")).toBe("upstream_500");
  });

  it("upstream abort → 504 timeout", async () => {
    vi.stubEnv("EVENTS_RECEIVER_BASE", "https://recv.example");
    vi.stubEnv("RECEIVER_TOKEN", "secret-token");
    mockFetch(async () => {
      throw Object.assign(new Error("aborted"), { name: "AbortError" });
    });

    const res = await POST(postReq(VALID_BODY));
    expect(res.status).toBe(504);
    expect(res.headers.get("x-degraded-reason")).toBe("timeout");
  });

  it("전부 무효 배치 → upstream 호출 없이 accepted 0 + Set-Cookie 발급", async () => {
    vi.stubEnv("EVENTS_RECEIVER_BASE", "https://recv.example");
    vi.stubEnv("RECEIVER_TOKEN", "secret-token");
    const fetchFn = mockFetch(async () =>
      new Response(JSON.stringify({ published: 0 }), { status: 200 }),
    );

    const res = await POST(postReq({ events: [{ event_type: "bad" }] }));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ accepted: 0 });
    expect(res.headers.get("set-cookie")).toContain("anon_id=");
    expect(fetchFn).not.toHaveBeenCalled();
  });
});
