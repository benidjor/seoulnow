import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { computeDistrictGradeMap } from "@/components/CongestionMap";

vi.mock("react-leaflet", () => ({
  MapContainer: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="map-container">{children}</div>
  ),
  TileLayer: () => <div data-testid="tile-layer" />,
  GeoJSON: ({ data }: { data: { features: unknown[] } }) => (
    <div data-testid="geojson" data-feature-count={data.features.length} />
  ),
  Marker: ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="marker">{children}</div>
  ),
  Popup: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="popup">{children}</div>
  ),
}));

describe("computeDistrictGradeMap", () => {
  it("place 데이터 자치구별 평균 congest_score → grade enum", () => {
    const places = [
      { district: "강남구", avg_congest_score: 1.0 },
      { district: "강남구", avg_congest_score: 2.0 },
      { district: "마포구", avg_congest_score: 3.5 },
    ];
    const result = computeDistrictGradeMap(places);
    expect(result.get("강남구")).toBe("보통");
    expect(result.get("마포구")).toBe("붐빔");
  });

  it("빈 입력 → 빈 Map", () => {
    const result = computeDistrictGradeMap([]);
    expect(result.size).toBe(0);
  });

  it("district undefined row 무시", () => {
    const places = [
      { district: "강남구", avg_congest_score: 1.0 },
      { avg_congest_score: 2.0 },
    ] as Array<{ district?: string; avg_congest_score?: number }>;
    const result = computeDistrictGradeMap(places);
    expect(result.size).toBe(1);
    expect(result.get("강남구")).toBe("여유");
  });
});

describe("CongestionMap (basic render)", () => {
  it("dynamic import 컴포넌트 — 본 테스트는 computeDistrictGradeMap 만 검증", () => {
    expect(typeof computeDistrictGradeMap).toBe("function");
  });
});
