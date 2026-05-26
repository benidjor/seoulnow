/**
 * 익명 ID 쿠키 거버넌스 (design.md §7-3).
 *
 * 첫 방문 시 무작위 UUID `anon_id` 쿠키를 1년 만료로 발급하고, 이후 방문은
 * 기존 쿠키를 재사용한다. IP 등 식별 정보는 저장하지 않는다.
 *
 * 순수 함수 — Request/Response 에 의존하지 않고 쿠키 헤더 문자열만 입력받아
 * 결과(anon_id + 필요 시 Set-Cookie 문자열)를 반환한다. route handler 가
 * Set-Cookie 를 응답 헤더에 실어 준다.
 */

export interface AnonIdResult {
  /** 재사용 또는 신규 발급된 anon_id (UUID v4 형식). */
  anonId: string;
  /** 신규 발급 시 응답에 실을 Set-Cookie 문자열. 재사용 시 null. */
  setCookie: string | null;
}

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const COOKIE_MAX_AGE_SEC = 31_536_000; // 1년

function parseAnonId(cookieHeader: string | null): string | null {
  if (!cookieHeader) return null;
  for (const part of cookieHeader.split(";")) {
    const eq = part.indexOf("=");
    if (eq === -1) continue;
    const key = part.slice(0, eq).trim();
    if (key !== "anon_id") continue;
    const value = part.slice(eq + 1).trim();
    return UUID_RE.test(value) ? value : null;
  }
  return null;
}

export function getOrCreateAnonId(cookieHeader: string | null): AnonIdResult {
  const existing = parseAnonId(cookieHeader);
  if (existing) {
    return { anonId: existing, setCookie: null };
  }

  const anonId = crypto.randomUUID();
  const setCookie = `anon_id=${anonId}; Max-Age=${COOKIE_MAX_AGE_SEC}; Path=/; Secure; HttpOnly; SameSite=Lax`;
  return { anonId, setCookie };
}
