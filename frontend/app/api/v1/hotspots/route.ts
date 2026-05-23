import { NextResponse } from "next/server";
import type { HotspotDistrict } from "@/lib/hotspots-client";

export const runtime = "edge";

interface ClientResponse {
  items: HotspotDistrict[];
}

interface UpstreamResponse {
  items: HotspotDistrict[];
  count: number;
}

const UPSTREAM_TIMEOUT_MS = 4000;

/**
 * 전체 자치구 혼잡도 Edge API — 기존 FastAPI `/api/hotspots` (전체 등급) forward.
 * 지도 색상 mapping 의 정식 source. chill-open-now (한가+영업 가게 마커) 와 분리.
 *
 * env: CHILL_API_BASE (Tunnel base), CHILL_API_TOKEN (옵션 Bearer)
 */
export async function GET(): Promise<NextResponse<ClientResponse>> {
  const base = process.env.CHILL_API_BASE;
  const token = process.env.CHILL_API_TOKEN;

  if (!base) {
    return NextResponse.json(
      { items: [] },
      { status: 503, headers: { "x-degraded-reason": "chill_api_base_missing" } },
    );
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);

  try {
    const headers: Record<string, string> = { Accept: "application/json" };
    if (token) headers.Authorization = `Bearer ${token}`;

    const upstream = await fetch(`${base.replace(/\/$/, "")}/api/hotspots`, {
      headers,
      signal: controller.signal,
    });
    clearTimeout(timer);

    if (!upstream.ok) {
      return NextResponse.json(
        { items: [] },
        { status: 502, headers: { "x-degraded-reason": `upstream_${upstream.status}` } },
      );
    }

    const data = (await upstream.json()) as UpstreamResponse;
    return NextResponse.json(
      { items: data.items ?? [] },
      { headers: { "cache-control": "public, max-age=60, s-maxage=60" } },
    );
  } catch (e) {
    clearTimeout(timer);
    const reason =
      e instanceof Error && e.name === "AbortError" ? "timeout" : "fetch_failed";
    return NextResponse.json(
      { items: [] },
      { status: 504, headers: { "x-degraded-reason": reason } },
    );
  }
}
