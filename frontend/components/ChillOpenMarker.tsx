"use client";

import { Marker, Popup } from "react-leaflet";
import type { ChillOpenPlace } from "@/lib/chill-open-client";

interface ChillOpenMarkerProps {
  place: ChillOpenPlace;
}

export default function ChillOpenMarker({ place }: ChillOpenMarkerProps) {
  return (
    <Marker position={[place.latitude, place.longitude]}>
      <Popup>
        <strong>{place.name}</strong>
        <br />
        {place.district}
        {place.category ? ` · ${place.category}` : ""}
        <br />
        영업: {place.open_hour ?? "?"}시–{place.close_hour ?? "?"}시
        <br />
        혼잡도 score: {place.avg_congest_score.toFixed(2)}
      </Popup>
    </Marker>
  );
}
