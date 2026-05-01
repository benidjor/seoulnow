"""Producer 출력 스키마 (Kafka 메시지 본문)."""
from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class HotspotEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    area_code: str
    area_name: str
    congest_level: str
    congest_message: str | None = None
    population_min: int | None = None
    population_max: int | None = None
    road_traffic_index: str | None = None
    road_traffic_speed_kmh: float | None = None
    temperature_c: float | None = None
    precipitation: str | None = None
    api_response_ts: datetime  # 서울 API 의 PPLTN_TIME (KST 가정, naive datetime)

    def kafka_key(self) -> str:
        return self.area_code

    def kafka_headers(self) -> Iterable[tuple[str, bytes]]:
        return [
            ("schema_version", b"v1"),
            ("api_response_ts", self.api_response_ts.isoformat().encode("utf-8")),
            ("source", b"seoul.openapi.citydata"),
        ]


class SubwayCongestionEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    station_code: str
    station_name: str
    line_name: str
    train_no: str | None = None
    direction: str | None = None              # 상행 / 하행 / 내선 / 외선
    congestion_score: float | None = None     # 0~150 류 (API 정의 따름)
    congestion_level: str | None = None       # 여유/보통/주의/혼잡 류
    api_response_ts: datetime

    def kafka_key(self) -> str:
        return f"{self.line_name}:{self.station_code}"

    def kafka_headers(self) -> Iterable[tuple[str, bytes]]:
        return [
            ("schema_version", b"v1"),
            ("api_response_ts", self.api_response_ts.isoformat().encode("utf-8")),
            ("source", b"seoul.subway.congestion"),
        ]
