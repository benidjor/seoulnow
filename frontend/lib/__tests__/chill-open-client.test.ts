import { describe, expect, it } from "vitest";
import {
  mapCongestScoreToGrade,
  congestGradeToColor,
  isOpenAtHour,
  filterChillOpenPlaces,
} from "@/lib/chill-open-client";

describe("mapCongestScoreToGrade", () => {
  it("0.5 → '여유' (1등급)", () => {
    expect(mapCongestScoreToGrade(0.5)).toBe("여유");
  });
  it("1.7 → '보통' (2등급)", () => {
    expect(mapCongestScoreToGrade(1.7)).toBe("보통");
  });
  it("2.6 → '약간 붐빔' (3등급)", () => {
    expect(mapCongestScoreToGrade(2.6)).toBe("약간 붐빔");
  });
  it("3.5 → '붐빔' (4등급)", () => {
    expect(mapCongestScoreToGrade(3.5)).toBe("붐빔");
  });
  it("null → '알수없음'", () => {
    expect(mapCongestScoreToGrade(null)).toBe("알수없음");
  });
});

describe("congestGradeToColor", () => {
  it("'여유' → #28a745", () => {
    expect(congestGradeToColor("여유")).toBe("#28a745");
  });
  it("'붐빔' → #dc3545", () => {
    expect(congestGradeToColor("붐빔")).toBe("#dc3545");
  });
  it("'알수없음' → #9ca3af", () => {
    expect(congestGradeToColor("알수없음")).toBe("#9ca3af");
  });
});

describe("isOpenAtHour", () => {
  it("9-18 영업, 현재 10시 → true", () => {
    expect(isOpenAtHour({ openHour: 9, closeHour: 18 }, 10)).toBe(true);
  });
  it("9-18 영업, 현재 17시 + buffer 60min → false (마감 1h 이내)", () => {
    expect(
      isOpenAtHour({ openHour: 9, closeHour: 18 }, 17, { closingBufferMin: 60 }),
    ).toBe(false);
  });
  it("9-18 영업, 현재 16시 + buffer 60min → true (마감까지 2h)", () => {
    expect(
      isOpenAtHour({ openHour: 9, closeHour: 18 }, 16, { closingBufferMin: 60 }),
    ).toBe(true);
  });
  it("18-02 (자정 넘김), 현재 1시 → true", () => {
    expect(isOpenAtHour({ openHour: 18, closeHour: 26 }, 25)).toBe(true);
  });
  it("openHour/closeHour null → false", () => {
    expect(isOpenAtHour({ openHour: null, closeHour: null }, 12)).toBe(false);
  });
});

describe("filterChillOpenPlaces", () => {
  const places = [
    {
      biz_reg_no: "P1",
      name: "Cafe A",
      district: "강남구",
      latitude: 37.5,
      longitude: 127.0,
      open_hour: 9,
      close_hour: 22,
      avg_congest_score: 1.2,
    },
    {
      biz_reg_no: "P2",
      name: "Bar B",
      district: "강남구",
      latitude: 37.5,
      longitude: 127.0,
      open_hour: 18,
      close_hour: 26,
      avg_congest_score: 1.8,
    },
    {
      biz_reg_no: "P3",
      name: "Cafe Closed",
      district: "강남구",
      latitude: 37.5,
      longitude: 127.0,
      open_hour: 9,
      close_hour: 11,
      avg_congest_score: 1.0,
    },
  ];

  it("현재 15시 → P1만 (P2 미오픈, P3 마감 임박)", () => {
    const result = filterChillOpenPlaces(places, 15, { closingBufferMin: 60 });
    expect(result.map((p) => p.biz_reg_no)).toEqual(["P1"]);
  });

  it("현재 20시 → P1 + P2 (둘 다 영업 + 마감 1h+)", () => {
    const result = filterChillOpenPlaces(places, 20, { closingBufferMin: 60 });
    expect(result.map((p) => p.biz_reg_no).sort()).toEqual(["P1", "P2"]);
  });
});
