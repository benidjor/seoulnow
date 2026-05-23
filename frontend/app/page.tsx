"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import type { FeatureCollection, Geometry } from "geojson";
import {
  filterChillOpenPlaces,
  type ChillOpenPlace,
} from "@/lib/chill-open-client";

const CongestionMap = dynamic(() => import("@/components/CongestionMap"), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-full text-gray-500">
      지도 로딩 중…
    </div>
  ),
});

const CLOSING_BUFFER_MIN = 60;

interface ApiResponse {
  snapshot_ts: string | null;
  places: ChillOpenPlace[];
}

export default function Page() {
  const [places, setPlaces] = useState<ChillOpenPlace[]>([]);
  const [snapshotTs, setSnapshotTs] = useState<string | null>(null);
  const [districts, setDistricts] = useState<FeatureCollection<
    Geometry,
    { SIG_KOR_NM?: string; name?: string }
  > | null>(null);
  const [selectedDistrict, setSelectedDistrict] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    fetch("/seoul-districts.geojson")
      .then((res) => res.json())
      .then(setDistricts)
      .catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e);
        setErrorMessage(`자치구 GeoJSON 로딩 실패: ${msg}`);
      });
  }, []);

  useEffect(() => {
    fetch("/api/v1/chill-open-now")
      .then((res) => res.json())
      .then((data: ApiResponse) => {
        setPlaces(data.places ?? []);
        setSnapshotTs(data.snapshot_ts);
      })
      .catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e);
        setErrorMessage(`chill_open_now API 실패 (degraded mode): ${msg}`);
      });
  }, []);

  const currentHour = new Date().getHours();
  const openNow = useMemo(
    () => filterChillOpenPlaces(places, currentHour, { closingBufferMin: CLOSING_BUFFER_MIN }),
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
          {snapshotTs ? `snapshot: ${snapshotTs}` : "데이터 로딩…"}
          <br />
          선택 자치구: {selectedDistrict ?? "(없음)"} · 표시 가게: {visibleMarkers.length}
        </div>
      </header>
      {errorMessage ? (
        <div className="bg-yellow-50 border-b border-yellow-200 px-4 py-1 text-xs text-yellow-800">
          {errorMessage}
        </div>
      ) : null}
      <div className="flex-1 min-h-0">
        {districts ? (
          <CongestionMap
            districts={districts}
            places={places}
            visibleMarkers={visibleMarkers}
            onDistrictClick={setSelectedDistrict}
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
