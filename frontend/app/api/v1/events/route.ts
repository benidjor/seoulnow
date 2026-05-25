import { NextResponse } from "next/server";
import { getOrCreateAnonId } from "@/lib/cookie-anon";
import { validateEvents } from "@/lib/event-validator";

export const runtime = "edge";

const UPSTREAM_TIMEOUT_MS = 4000;

/**
 * 익명 행동 이벤트 수신 Edge API (REST Proxy 패턴, design.md §7-2).
 *
 * 브라우저 → POST /api/v1/events {events:[...]} → (anon_id 쿠키 + ua_hash stamp +
 * 검증) → Oracle Cloud HTTP receiver `POST /v1/events` (Cloudflare Tunnel 경유)
 * → Kafka user.events.v1.
 *
 * env (Pages 에 설정, 여기선 읽기만):
 *   EVENTS_RECEIVER_BASE — Tunnel 노출 receiver base (예: https://recv.seoulnow.live)
 *   RECEIVER_TOKEN       — receiver Bearer 토큰
 *   ANON_UA_SALT         — (옵션) ua_hash salt. 미설정 시 ua_hash 생략
 */
export async function POST(req: Request): Promise<NextResponse> {
  const base = process.env.EVENTS_RECEIVER_BASE;
  const token = process.env.RECEIVER_TOKEN;
  const salt = process.env.ANON_UA_SALT;

  if (!base || !token) {
    return NextResponse.json(
      { accepted: 0 },
      { status: 503, headers: { "x-degraded-reason": "events_receiver_unconfigured" } },
    );
  }

  let rawEvents: unknown[];
  try {
    const body = (await req.json()) as { events?: unknown };
    rawEvents = Array.isArray(body?.events) ? body.events : [];
  } catch {
    return NextResponse.json(
      { accepted: 0 },
      { status: 400, headers: { "x-degraded-reason": "invalid_json" } },
    );
  }

  const { anonId, setCookie } = getOrCreateAnonId(req.headers.get("cookie"));

  let uaHash: string | null = null;
  if (salt) {
    uaHash = await sha256Hex((req.headers.get("user-agent") ?? "") + salt);
  }

  const { valid } = validateEvents(rawEvents, { anonId, uaHash });

  // 유효 이벤트가 없으면 receiver 를 호출하지 않고 즉시 응답 (신규 쿠키는 발급).
  if (valid.length === 0) {
    return jsonWithCookie({ accepted: 0 }, 200, setCookie);
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    const upstream = await fetch(`${base.replace(/\/$/, "")}/v1/events`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ events: valid }),
      signal: controller.signal,
    });
    clearTimeout(timer);

    if (!upstream.ok) {
      return NextResponse.json(
        { accepted: 0 },
        { status: 502, headers: { "x-degraded-reason": `upstream_${upstream.status}` } },
      );
    }

    const data = (await upstream.json().catch(() => ({}))) as { published?: number };
    const accepted = typeof data.published === "number" ? data.published : valid.length;
    return jsonWithCookie({ accepted }, 200, setCookie);
  } catch (e) {
    clearTimeout(timer);
    const reason =
      e instanceof Error && e.name === "AbortError" ? "timeout" : "fetch_failed";
    return NextResponse.json(
      { accepted: 0 },
      { status: reason === "timeout" ? 504 : 502, headers: { "x-degraded-reason": reason } },
    );
  }
}

function jsonWithCookie(
  body: { accepted: number },
  status: number,
  setCookie: string | null,
): NextResponse {
  const res = NextResponse.json(body, { status });
  if (setCookie) res.headers.set("set-cookie", setCookie);
  return res;
}

async function sha256Hex(input: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
