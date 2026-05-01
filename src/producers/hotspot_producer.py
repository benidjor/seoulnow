"""서울 도시데이터 핫스팟 producer.

폴링 주기: 5분 (HOTSPOT_POLL_INTERVAL_SEC). 핫스팟 N 곳을 순회하며
http://openapi.seoul.go.kr:8088/{KEY}/json/citydata/1/5/{AREA_NM} 호출.
"""
from __future__ import annotations

import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from platform_common import get_settings
from platform_common.kafka import build_producer, produce_json

from .schemas import HotspotEvent

log = structlog.get_logger()

TOPIC = "seoul.hotspot.congestion.v1"
SEOUL_API_BASE = "http://openapi.seoul.go.kr:8088"


def parse_hotspot_payload(payload: dict[str, Any], area_code: str) -> HotspotEvent | None:
    result = payload.get("RESULT", {})
    code = result.get("RESULT.CODE") or result.get("CODE")
    if code and code != "INFO-000":
        return None

    citydata = payload.get("CITYDATA")
    if not isinstance(citydata, dict):
        return None

    live = citydata.get("LIVE_PPLTN_STTS", {}) or {}
    road = ((citydata.get("ROAD_TRAFFIC_STTS") or {}).get("AVG_ROAD_DATA")) or {}
    weather = citydata.get("WEATHER_STTS", {}) or {}

    pttm = live.get("PPLTN_TIME")
    if not pttm:
        return None
    try:
        api_ts = datetime.strptime(pttm, "%Y-%m-%d %H:%M")
    except ValueError:
        return None

    def _to_int(v: Any) -> int | None:
        try:
            return int(str(v).replace(",", "")) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    def _to_float(v: Any) -> float | None:
        try:
            return float(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    return HotspotEvent(
        area_code=area_code,
        area_name=citydata.get("AREA_NM", ""),
        congest_level=live.get("AREA_CONGEST_LVL", ""),
        congest_message=live.get("AREA_CONGEST_MSG"),
        population_min=_to_int(live.get("AREA_PPLTN_MIN")),
        population_max=_to_int(live.get("AREA_PPLTN_MAX")),
        road_traffic_index=road.get("ROAD_TRAFFIC_IDX"),
        road_traffic_speed_kmh=_to_float(road.get("ROAD_TRAFFIC_SPD")),
        temperature_c=_to_float(weather.get("TEMP")),
        precipitation=weather.get("PRECIPITATION"),
        api_response_ts=api_ts,
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_hotspot(client: httpx.Client, api_key: str, area_name: str) -> dict[str, Any]:
    url = f"{SEOUL_API_BASE}/{api_key}/json/citydata/1/5/{area_name}"
    r = client.get(url, timeout=10.0)
    r.raise_for_status()
    return r.json()


def run(area_codes: dict[str, str]) -> None:
    """area_codes = {area_code: area_name}. e.g. {"POI001": "강남역"}."""
    s = get_settings()
    if not s.seoul_openapi_key:
        raise SystemExit("SEOUL_OPENAPI_KEY not set")

    producer = build_producer(client_id="hotspot-producer")
    stop = {"flag": False}

    def _on_signal(_signum, _frame):
        stop["flag"] = True
        log.info("shutdown signal received")

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        with httpx.Client() as client:
            while not stop["flag"]:
                cycle_start = time.monotonic()
                for code, name in area_codes.items():
                    try:
                        payload = fetch_hotspot(client, s.seoul_openapi_key, name)
                    except httpx.HTTPStatusError as e:
                        # API 키가 URL 경로에 평문으로 박히므로 str(e) / URL 노출 금지
                        log.warning("fetch_failed_http", area=name, status=e.response.status_code)
                        continue
                    except Exception as e:
                        log.warning("fetch_failed", area=name, error=type(e).__name__)
                        continue
                    event = parse_hotspot_payload(payload, area_code=code)
                    if event is None:
                        log.warning("parse_returned_none", area=name)
                        continue
                    produce_json(
                        producer,
                        topic=TOPIC,
                        key=event.kafka_key(),
                        value=event.model_dump(mode="json"),
                        headers=event.kafka_headers(),
                    )
                    log.info("produced", topic=TOPIC, area=name, congest=event.congest_level)
                producer.flush(timeout=10)

                elapsed = time.monotonic() - cycle_start
                sleep_for = max(0, s.hotspot_poll_interval_sec - elapsed)
                for _ in range(int(sleep_for)):
                    if stop["flag"]:
                        break
                    time.sleep(1)
    finally:
        # 예외 / 정상 종료 모두 flush 보장. confluent_kafka.Producer 는 close() 없음 — flush 로 충분
        producer.flush(timeout=10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # 데모용 3곳. 본격 운영 시 data/reference/hotspot_regions.csv 로 확장.
    DEFAULT_AREAS = {
        "POI001": "강남역",
        "POI002": "홍대입구역(2호선)",
        "POI003": "여의도",
    }
    run(DEFAULT_AREAS)
    sys.exit(0)
