/**
 * 전체 자치구 혼잡도 (기존 FastAPI `GET /api/hotspots`, fact_hotspot_congestion_5min
 * latest 5분 윈도우). chill_open_now 와 달리 **전체 등급 (여유~붐빔) 모두 포함** —
 * 지도 색상 mapping 의 정식 source (plan §130).
 */
export interface HotspotDistrict {
  district: string;
  gu_code?: string | null;
  window_start?: string | null;
  area_count?: number;
  avg_congest_score: number;
  max_congest_score?: number;
}

export function normalizeHotspots(items: unknown): HotspotDistrict[] {
  if (!Array.isArray(items)) return [];
  return items.filter(
    (it): it is HotspotDistrict =>
      typeof it === "object" &&
      it !== null &&
      typeof (it as { district?: unknown }).district === "string" &&
      typeof (it as { avg_congest_score?: unknown }).avg_congest_score === "number",
  );
}
