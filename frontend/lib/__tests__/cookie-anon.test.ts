import { describe, expect, it } from "vitest";
import { getOrCreateAnonId } from "@/lib/cookie-anon";

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

describe("getOrCreateAnonId", () => {
  it("쿠키 헤더 없음 → 새 UUID 발급 + Set-Cookie 문자열", () => {
    const { anonId, setCookie } = getOrCreateAnonId(null);
    expect(anonId).toMatch(UUID_RE);
    expect(setCookie).not.toBeNull();
  });

  it("Set-Cookie 는 1년 만료 + 보안 속성 포함", () => {
    const { anonId, setCookie } = getOrCreateAnonId(null);
    expect(setCookie).toContain(`anon_id=${anonId}`);
    expect(setCookie).toContain("Max-Age=31536000");
    expect(setCookie).toContain("Path=/");
    expect(setCookie).toContain("Secure");
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("SameSite=Lax");
  });

  it("기존 유효 anon_id 쿠키 → reuse + Set-Cookie 없음", () => {
    const existing = "11111111-2222-4333-8444-555566667777";
    const { anonId, setCookie } = getOrCreateAnonId(`anon_id=${existing}`);
    expect(anonId).toBe(existing);
    expect(setCookie).toBeNull();
  });

  it("다른 쿠키들과 섞여 있어도 anon_id 추출", () => {
    const existing = "11111111-2222-4333-8444-555566667777";
    const header = `theme=dark; anon_id=${existing}; lang=ko`;
    const { anonId, setCookie } = getOrCreateAnonId(header);
    expect(anonId).toBe(existing);
    expect(setCookie).toBeNull();
  });

  it("anon_id 가 UUID 형식이 아니면 무시하고 새로 발급", () => {
    const { anonId, setCookie } = getOrCreateAnonId("anon_id=not-a-uuid");
    expect(anonId).not.toBe("not-a-uuid");
    expect(anonId).toMatch(UUID_RE);
    expect(setCookie).not.toBeNull();
  });

  it("빈 문자열 쿠키 헤더 → 새 UUID 발급", () => {
    const { anonId, setCookie } = getOrCreateAnonId("");
    expect(anonId).toMatch(UUID_RE);
    expect(setCookie).not.toBeNull();
  });
});
