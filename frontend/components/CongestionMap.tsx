"use client";

import { useMemo } from "react";
import { MapContainer, TileLayer, GeoJSON, Marker, Popup } from "react-leaflet";
import type { Layer, PathOptions } from "leaflet";
import type { Feature, FeatureCollection, Geometry } from "geojson";
import {
  mapCongestScoreToGrade,
  congestGradeToColor,
  type CongestGrade,
  type ChillOpenPlace,
} from "@/lib/chill-open-client";

interface DistrictRecord {
  district?: string;
  avg_congest_score?: number;
}

/**
 * 자치구별 혼잡도 등급 Map. 입력은 **전체 자치구 혼잡도** (hotspots,
 * `/api/hotspots`) — 전체 등급(여유~붐빔) 포함. chill_open_now (한가 가게만) 가
 * 아니다. district 당 1행이면 그 값, 여러 행이면 평균.
 */
export function computeDistrictGradeMap(
  records: DistrictRecord[],
): Map<string, CongestGrade> {
  const bucket = new Map<string, number[]>();
  for (const p of records) {
    if (!p.district) continue;
    const score = p.avg_congest_score;
    if (typeof score !== "number") continue;
    const arr = bucket.get(p.district) ?? [];
    arr.push(score);
    bucket.set(p.district, arr);
  }
  const result = new Map<string, CongestGrade>();
  for (const [district, scores] of bucket) {
    const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
    result.set(district, mapCongestScoreToGrade(avg));
  }
  return result;
}

interface CongestionMapProps {
  districts: FeatureCollection<Geometry, { name?: string; SIG_KOR_NM?: string }>;
  /** 전체 자치구 혼잡도 (지도 색상 source). `{district, avg_congest_score}` shape. */
  districtCongestion: DistrictRecord[];
  /** 선택 자치구의 한가+영업 가게 마커. */
  visibleMarkers: ChillOpenPlace[];
  onDistrictClick?: (district: string) => void;
}

const SEOUL_CENTER: [number, number] = [37.5665, 126.978];

function pickDistrictName(
  feature: Feature<Geometry, { name?: string; SIG_KOR_NM?: string }>,
): string | undefined {
  return feature.properties?.SIG_KOR_NM ?? feature.properties?.name;
}

export default function CongestionMap({
  districts,
  districtCongestion,
  visibleMarkers,
  onDistrictClick,
}: CongestionMapProps) {
  const gradeMap = useMemo(
    () => computeDistrictGradeMap(districtCongestion),
    [districtCongestion],
  );

  const styleFn = (feature?: Feature): PathOptions => {
    if (!feature) return { fillColor: "#9ca3af", color: "#374151", weight: 1, fillOpacity: 0.55 };
    const name = pickDistrictName(
      feature as Feature<Geometry, { name?: string; SIG_KOR_NM?: string }>,
    );
    const grade = name ? gradeMap.get(name) ?? "알수없음" : "알수없음";
    return {
      fillColor: congestGradeToColor(grade),
      color: "#374151",
      weight: 1,
      fillOpacity: 0.55,
    };
  };

  const onEachFeature = (feature: Feature, layer: Layer) => {
    const name = pickDistrictName(
      feature as Feature<Geometry, { name?: string; SIG_KOR_NM?: string }>,
    );
    if (!name) return;
    const grade = gradeMap.get(name) ?? "알수없음";
    layer.bindTooltip(`${name} · ${grade}`, { sticky: true });
    layer.on("click", () => onDistrictClick?.(name));
  };

  return (
    <MapContainer
      center={SEOUL_CENTER}
      zoom={11}
      style={{ height: "100%", width: "100%" }}
      scrollWheelZoom
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <GeoJSON data={districts} style={styleFn} onEachFeature={onEachFeature} />
      {visibleMarkers.map((place) => (
        <Marker
          key={place.biz_reg_no}
          position={[place.latitude, place.longitude]}
        >
          <Popup>
            <strong>{place.name}</strong>
            <br />
            {place.district}
            {place.category ? ` · ${place.category}` : ""}
            <br />
            영업: {place.open_hour ?? "?"}시–{place.close_hour ?? "?"}시
          </Popup>
        </Marker>
      ))}
    </MapContainer>
  );
}
