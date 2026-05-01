"""서울 지하철 실시간 혼잡도 producer.

폴링 주기 60초 (SUBWAY_POLL_INTERVAL_SEC).
실 API endpoint 는 키 발급 시 안내 받음. 본 plan 은 응답 형태가
{ "errorMessage": {...}, "CongestionInfo": [{...}, ...] } 라고 가정.
다르면 parse_subway_payload 의 키만 조정.
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

from .schemas import SubwayCongestionEvent

log = structlog.get_logger()

TOPIC = "seoul.transit.subway.v1"
SUBWAY_API_BASE = "https://openapi.seoulmetro.co.kr"  # 실제 endpoint 발급 시 교체


def _to_float(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


def parse_subway_payload(payload: dict[str, Any]) -> list[SubwayCongestionEvent]:
    err = payload.get("errorMessage", {}) or {}
    code = err.get("code") or err.get("CODE")
    if code and code != "INFO-000":
        return []

    items = payload.get("CongestionInfo") or payload.get("congestionInfo") or []
    out: list[SubwayCongestionEvent] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ts_raw = item.get("responseTime") or item.get("RESPONSE_TIME")
        if not ts_raw:
            continue
        try:
            api_ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        out.append(
            SubwayCongestionEvent(
                station_code=str(item.get("stationCode") or item.get("STATION_CD") or ""),
                station_name=str(item.get("stationName") or item.get("STATION_NM") or ""),
                line_name=str(item.get("lineName") or item.get("LINE_NM") or ""),
                train_no=item.get("trainNo") or item.get("TRAIN_NO"),
                direction=item.get("direction") or item.get("DIR"),
                congestion_score=_to_float(item.get("congestionScore")),
                congestion_level=item.get("congestionLevel") or item.get("CONGEST_LVL"),
                api_response_ts=api_ts,
            )
        )
    return out


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_subway(client: httpx.Client, api_key: str, line: str) -> dict[str, Any]:
    url = f"{SUBWAY_API_BASE}/api/subway/{api_key}/json/realtimeCongestion/{line}"
    r = client.get(url, timeout=10.0)
    r.raise_for_status()
    return r.json()


def run(lines: list[str]) -> None:
    s = get_settings()
    if not s.seoul_subway_api_key:
        raise SystemExit("SEOUL_SUBWAY_API_KEY not set")

    producer = build_producer(client_id="subway-producer")
    stop = {"flag": False}

    def _on_signal(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        with httpx.Client() as client:
            while not stop["flag"]:
                cycle_start = time.monotonic()
                for line in lines:
                    try:
                        payload = fetch_subway(client, s.seoul_subway_api_key, line)
                    except httpx.HTTPStatusError as e:
                        # API 키가 URL 경로에 평문으로 박히므로 str(e) / URL 노출 금지
                        log.warning("fetch_failed_http", line=line, status=e.response.status_code)
                        continue
                    except Exception as e:
                        log.warning("fetch_failed", line=line, error=type(e).__name__)
                        continue
                    events = parse_subway_payload(payload)
                    for event in events:
                        produce_json(
                            producer,
                            topic=TOPIC,
                            key=event.kafka_key(),
                            value=event.model_dump(mode="json"),
                            headers=event.kafka_headers(),
                        )
                    log.info("produced_batch", line=line, count=len(events))
                producer.flush(timeout=10)

                elapsed = time.monotonic() - cycle_start
                sleep_for = max(0, s.subway_poll_interval_sec - elapsed)
                for _ in range(int(sleep_for)):
                    if stop["flag"]:
                        break
                    time.sleep(1)
    finally:
        # 예외 / 정상 종료 모두 flush 보장. confluent_kafka.Producer 는 close() 없음 — flush 로 충분
        producer.flush(timeout=10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    DEFAULT_LINES = ["2호선", "9호선"]
    run(DEFAULT_LINES)
    sys.exit(0)
