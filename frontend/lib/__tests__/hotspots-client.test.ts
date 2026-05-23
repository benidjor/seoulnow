import { describe, expect, it } from "vitest";
import { normalizeHotspots } from "@/lib/hotspots-client";

describe("normalizeHotspots", () => {
  it("유효한 자치구 혼잡도 row 통과 (전체 등급 — 붐빔 포함)", () => {
    const raw = [
      { district: "강남구", avg_congest_score: 3.5, max_congest_score: 4 },
      { district: "마포구", avg_congest_score: 1.2 },
    ];
    const result = normalizeHotspots(raw);
    expect(result).toHaveLength(2);
    expect(result[0].district).toBe("강남구");
    expect(result[0].avg_congest_score).toBe(3.5);
  });

  it("배열 아님 → 빈 배열", () => {
    expect(normalizeHotspots(null)).toEqual([]);
    expect(normalizeHotspots(undefined)).toEqual([]);
    expect(normalizeHotspots({ items: [] })).toEqual([]);
  });

  it("district 누락 / score 비숫자 row 제거", () => {
    const raw = [
      { district: "강남구", avg_congest_score: 2.0 },
      { avg_congest_score: 1.0 },
      { district: "마포구", avg_congest_score: "high" },
      { district: "종로구", avg_congest_score: 4.0 },
    ];
    const result = normalizeHotspots(raw);
    expect(result.map((r) => r.district)).toEqual(["강남구", "종로구"]);
  });
});
