'use client';

import { useEffect, useState } from 'react';
import { MapContainer, TileLayer, CircleMarker, Tooltip } from 'react-leaflet';

type Area = {
  area_code: string;
  area_name: string;
  district: string;
  latitude: number;
  longitude: number;
  congest_level_score: number;
  congest_level: string;
  api_response_ts: string;
};

const COLOR_BY_SCORE: Record<number, string> = {
  1: '#22c55e', // 여유
  2: '#facc15', // 보통
  3: '#fb923c', // 약간 붐빔
  4: '#ef4444', // 붐빔
};

export default function HotspotMap() {
  const [areas, setAreas] = useState<Area[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const base = process.env.NEXT_PUBLIC_API_BASE!;
    fetch(`${base}/api/hotspots/areas`)
      .then((r) => r.json())
      .then((d) => setAreas(d.items ?? []))
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <div className="p-4 text-red-400">API 오류: {error}</div>;

  return (
    <MapContainer
      center={[37.5665, 126.978]}
      zoom={11}
      style={{ height: 'calc(100vh - 80px)', width: '100%' }}
      scrollWheelZoom
    >
      <TileLayer
        attribution='&copy; OpenStreetMap'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {areas.map((a) => (
        <CircleMarker
          key={a.area_code}
          center={[a.latitude, a.longitude]}
          radius={10}
          pathOptions={{
            color: COLOR_BY_SCORE[a.congest_level_score] ?? '#71717a',
            fillOpacity: 0.7,
          }}
        >
          <Tooltip direction="top">
            <div className="text-xs">
              <strong>{a.area_name}</strong> &middot; {a.district}
              <br />
              {a.congest_level} ({a.congest_level_score})
              <br />
              <span className="opacity-70">{a.api_response_ts}</span>
            </div>
          </Tooltip>
        </CircleMarker>
      ))}
    </MapContainer>
  );
}
