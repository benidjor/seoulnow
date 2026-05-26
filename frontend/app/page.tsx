"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useRef, useState } from "react";
import type { FeatureCollection, Geometry } from "geojson";
import {
  filterChillOpenPlaces,
  type ChillOpenPlace,
} from "@/lib/chill-open-client";
import { normalizeHotspots, type HotspotDistrict } from "@/lib/hotspots-client";
import { fetchApiJson } from "@/lib/api-client";
import { buildClientEvent, postEvents } from "@/lib/events-client";

const CongestionMap = dynamic(() => import("@/components/CongestionMap"), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-full text-gray-500">
      지도 로딩 중…
    </div>
  ),
});

const CLOSING_BUFFER_MIN = 60;

interface ChillResponse {
  snapshot_ts: string | null;
  places: ChillOpenPlace[];
}

interface HotspotsResponse {
  items: HotspotDistrict[];
}

export default function Page() {
  const [places, setPlaces] = useState<ChillOpenPlace[]>([]);
  const [hotspots, setHotspots] = useState<HotspotDistrict[]>([]);
  const [snapshotTs, setSnapshotTs] = useState<string | null>(null);
  const [districts, setDistricts] = useState<FeatureCollection<
    Geometry,
    { SIG_KOR_NM?: string; name?: string }
  > | null>(null);
  const [selectedDistrict, setSelectedDistrict] = useState<string | null>(null);
  const [degradedReason, setDegradedReason] = useState<string | null>(null);
  const [geoError, setGeoError] = useState<string | null>(null);

  // 페이지 진입 1회 map_view 발행 (StrictMode 이중 호출 guard)
  const mapViewSent = useRef(false);
  useEffect(() => {
    if (mapViewSent.current) return;
    mapViewSent.current = true;
    postEvents([buildClientEvent("map_view")]);
  }, []);

  // react-leaflet GeoJSON 의 click 핸들러는 mount 시 1회만 bind 되어 클로저가
  // stale 해진다 → 선택값 비교는 ref 로 최신값을 읽는다.
  const selectedDistrictRef = useRef<string | null>(null);
  useEffect(() => {
    selectedDistrictRef.current = selectedDistrict;
  }, [selectedDistrict]);

  // 자치구 클릭 = hotspot_click(클릭 행위) + 선택이 바뀌면 district_filter(필터 변경).
  // 마커 클릭 배선은 CongestionMap 수정이 필요해 Day 11 범위 밖.
  function handleDistrictSelect(district: string) {
    postEvents([buildClientEvent("hotspot_click", { district })]);
    const previous = selectedDistrictRef.current;
    if (district !== previous) {
      postEvents([
        buildClientEvent("district_filter", { district, previous }),
      ]);
    }
    setSelectedDistrict(district);
  }

  useEffect(() => {
    fetch("/seoul-districts.geojson")
      .then((res) => res.json())
      .then(setDistricts)
      .catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e);
        setGeoError(`자치구 GeoJSON 로딩 실패: ${msg}`);
      });
  }, []);

  useEffect(() => {
    async function load() {
      const [hotspotsRes, chillRes] = await Promise.all([
        fetchApiJson<HotspotsResponse>("/api/v1/hotspots", { items: [] }),
        fetchApiJson<ChillResponse>("/api/v1/chill-open-now", {
          snapshot_ts: null,
          places: [],
        }),
      ]);

      setHotspots(normalizeHotspots(hotspotsRes.data.items));
      setPlaces(chillRes.data.places ?? []);
      setSnapshotTs(chillRes.data.snapshot_ts);

      if (hotspotsRes.degraded || chillRes.degraded) {
        setDegradedReason(
          hotspotsRes.reason ?? chillRes.reason ?? "데이터 source 미연결",
        );
      } else {
        setDegradedReason(null);
      }
    }
    void load();
  }, []);

  const currentHour = new Date().getHours();
  const openNow = useMemo(
    () =>
      filterChillOpenPlaces(places, currentHour, {
        closingBufferMin: CLOSING_BUFFER_MIN,
      }),
    [places, currentHour],
  );

  const visibleMarkers = useMemo(
    () =>
      selectedDistrict
        ? openNow.filter((p) => p.district === selectedDistrict)
        : [],
    [openNow, selectedDistrict],
  );

  return (
    <main className="h-screen w-screen flex flex-col">
      <header className="bg-white border-b px-4 py-2 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold">seoulnow</h1>
          <p className="text-xs text-gray-500">
            서울 실시간 혼잡도 + 지금 한가한 카페·술집 (마감 1h+)
          </p>
        </div>
        <div className="text-xs text-gray-500 text-right">
          {snapshotTs
            ? `snapshot: ${snapshotTs}`
            : degradedReason
              ? "데이터 source 미연결"
              : "데이터 로딩…"}
          <br />
          자치구 {hotspots.length}곳 · 선택: {selectedDistrict ?? "(없음)"} · 표시 가게:{" "}
          {visibleMarkers.length}
        </div>
      </header>
      {degradedReason ? (
        <div className="bg-yellow-50 border-b border-yellow-200 px-4 py-1 text-xs text-yellow-800">
          데이터 source 미연결 (degraded): {degradedReason} — Edge API 의 CHILL_API_BASE
          / Tunnel ingress 설정 후 표시됩니다 (Task 11.1+).
        </div>
      ) : null}
      {geoError ? (
        <div className="bg-red-50 border-b border-red-200 px-4 py-1 text-xs text-red-800">
          {geoError}
        </div>
      ) : null}
      <div className="flex-1 min-h-0">
        {districts ? (
          <CongestionMap
            districts={districts}
            districtCongestion={hotspots}
            visibleMarkers={visibleMarkers}
            onDistrictClick={handleDistrictSelect}
          />
        ) : (
          <div className="flex items-center justify-center h-full text-gray-400">
            자치구 데이터 로딩 중…
          </div>
        )}
      </div>
    </main>
  );
}
