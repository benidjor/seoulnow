import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchApiJson } from "@/lib/api-client";

afterEach(() => {
  vi.unstubAllGlobals();
});

function mockFetch(impl: () => Promise<Response>) {
  vi.stubGlobal("fetch", vi.fn(impl));
}

describe("fetchApiJson", () => {
  it("200 OK → degraded false + data 반환", async () => {
    mockFetch(async () =>
      new Response(JSON.stringify({ items: [1, 2] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const result = await fetchApiJson<{ items: number[] }>("/x", { items: [] });
    expect(result.degraded).toBe(false);
    expect(result.reason).toBeNull();
    expect(result.data.items).toEqual([1, 2]);
  });

  it("503 + x-degraded-reason → degraded true + reason 헤더", async () => {
    mockFetch(async () =>
      new Response(JSON.stringify({ items: [] }), {
        status: 503,
        headers: {
          "content-type": "application/json",
          "x-degraded-reason": "chill_api_base_missing",
        },
      }),
    );
    const result = await fetchApiJson<{ items: number[] }>("/x", { items: [] });
    expect(result.degraded).toBe(true);
    expect(result.reason).toBe("chill_api_base_missing");
  });

  it("network throw → degraded true + fallback 데이터", async () => {
    mockFetch(async () => {
      throw new Error("boom");
    });
    const result = await fetchApiJson<{ items: number[] }>("/x", { items: [] });
    expect(result.degraded).toBe(true);
    expect(result.reason).toBe("boom");
    expect(result.data.items).toEqual([]);
  });

  it("JSON parse 실패 → fallback 데이터 + degraded false (200인 경우)", async () => {
    mockFetch(async () =>
      new Response("not json", { status: 200, headers: { "content-type": "text/plain" } }),
    );
    const result = await fetchApiJson<{ items: number[] }>("/x", { items: [99] });
    expect(result.data.items).toEqual([99]);
  });
});
