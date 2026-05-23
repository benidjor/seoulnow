import { NextResponse } from "next/server";
import type { ChillOpenPlace } from "@/lib/chill-open-client";

export const runtime = "edge";

interface ClientResponse {
  snapshot_ts: string | null;
  places: ChillOpenPlace[];
}

// 기존 FastAPI (`src/api/routes/chill_open.py`, Day 8 Task 8.2) 응답 shape.
// DuckDB 로 Iceberg gold mart 직접 query + is_open_now filter 적용한 결과.
interface UpstreamResponse {
  items: Array<ChillOpenPlace & { is_open_now?: boolean }>;
  count: number;
  current_hour: number;
}

const UPSTREAM_TIMEOUT_MS = 4000;

/**
 * Option 5 (FastAPI 직결 + Tunnel) — Edge API 가 기존 FastAPI `/api/chill-open`
 * 으로 forward + 응답 schema 를 frontend `{snapshot_ts, places}` 로 변환.
 *
 * env:
 *   CHILL_API_BASE  — Tunnel 노출 FastAPI base (예: https://api.seoulnow.live)
 *   CHILL_API_TOKEN — (옵션) Bearer 토큰. Tunnel 노출 보호용. 미설정 시 헤더 생략
 */
export async function GET(): Promise<NextResponse<ClientResponse>> {
  const base = process.env.CHILL_API_BASE;
  const token = process.env.CHILL_API_TOKEN;

  if (!base) {
    return NextResponse.json(
      { snapshot_ts: null, places: [] },
      { status: 503, headers: { "x-degraded-reason": "chill_api_base_missing" } },
    );
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);

  try {
    const headers: Record<string, string> = { Accept: "application/json" };
    if (token) headers.Authorization = `Bearer ${token}`;

    const upstream = await fetch(`${base.replace(/\/$/, "")}/api/chill-open`, {
      headers,
      signal: controller.signal,
    });
    clearTimeout(timer);

    if (!upstream.ok) {
      return NextResponse.json(
        { snapshot_ts: null, places: [] },
        { status: 502, headers: { "x-degraded-reason": `upstream_${upstream.status}` } },
      );
    }

    const data = (await upstream.json()) as UpstreamResponse;
    const body: ClientResponse = {
      // FastAPI 는 live mart read 라 별도 snapshot_ts 없음 → fetch 시각으로 합성.
      snapshot_ts: new Date().toISOString(),
      places: data.items ?? [],
    };
    return NextResponse.json(body, {
      headers: { "cache-control": "public, max-age=60, s-maxage=60" },
    });
  } catch (e) {
    clearTimeout(timer);
    const reason =
      e instanceof Error && e.name === "AbortError" ? "timeout" : "fetch_failed";
    return NextResponse.json(
      { snapshot_ts: null, places: [] },
      { status: 504, headers: { "x-degraded-reason": reason } },
    );
  }
}
