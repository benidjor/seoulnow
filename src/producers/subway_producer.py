"""서울 지하철 실시간 도착정보 producer.

폴링 주기 60초 (SUBWAY_POLL_INTERVAL_SEC).
endpoint: swopenapi.seoul.go.kr / realtimeStationArrival
  - 역이름 파라미터 "" → 전체 운행 열차 도착정보 일괄 조회
  - 일 최대 1,000 회 / 1 회 최대 1,000 건
응답 최상위 키: errorMessage (성공 시 code="INFO-000") + realtimeArrivalList
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
SUBWAY_API_BASE = "http://swopenapi.seoul.go.kr"


def _to_float(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


def _unwrap_arrival_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """realtimeStationArrival 응답에서 항목 리스트 반환.

    성공 응답:
      {"errorMessage": {"status":200, "code":"INFO-000", ...},
       "realtimeArrivalList": [{...}, ...]}
    데이터 없음(막차 이후 등):
      {"status":500, "code":"INFO-200", ...}  ← list 없음
    """
    err = payload.get("errorMessage", {}) or {}
    code = str(err.get("code") or payload.get("code") or "")
    if code and code not in ("INFO-000", ""):
        return []
    return payload.get("realtimeArrivalList") or []


def parse_subway_payload(payload: dict[str, Any]) -> list[SubwayCongestionEvent]:
    items = _unwrap_arrival_list(payload)
    out: list[SubwayCongestionEvent] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ts_raw = item.get("recptnDt")
        if not ts_raw:
            continue
        try:
            api_ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            log.debug("skip_bad_recptn_dt", raw=ts_raw)
            continue
        out.append(
            SubwayCongestionEvent(
                station_code=str(item.get("statnId") or ""),
                station_name=str(item.get("statnNm") or ""),
                line_name=str(item.get("subwayId") or ""),
                train_no=item.get("btrainNo"),
                direction=item.get("updnLine"),
                congestion_score=None,   # realtimeStationArrival 미제공
                congestion_level=None,   # realtimeStationArrival 미제공
                api_response_ts=api_ts,
            )
        )
    return out


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_subway(client: httpx.Client, api_key: str, station: str) -> dict[str, Any]:
    url = f"{SUBWAY_API_BASE}/api/subway/{api_key}/json/realtimeStationArrival/1/1000/{station}"
    r = client.get(url, timeout=10.0)
    r.raise_for_status()
    return r.json()


def run(stations: list[str]) -> None:
    """stations: 역이름 리스트. [""] 이면 전체 운행 열차 일괄 조회."""
    s = get_settings()
    if not s.seoul_subway_api_key:
        raise SystemExit("SEOUL_SUBWAY_API_KEY not set")

    producer = build_producer(client_id="subway-producer")
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
                for station in stations:
                    try:
                        payload = fetch_subway(client, s.seoul_subway_api_key, station)
                    except httpx.HTTPStatusError as e:
                        # API 키가 URL 경로에 평문으로 박히므로 str(e) / URL 노출 금지
                        log.warning("fetch_failed_http", station=station, status=e.response.status_code)
                        continue
                    except Exception as e:
                        log.warning("fetch_failed", station=station, error=type(e).__name__)
                        continue
                    events = parse_subway_payload(payload)
                    if not events:
                        # 막차 이후 INFO-200 / errorMessage 코드 오류 등
                        log.warning("parse_returned_empty", station=station or "(all)")
                        continue
                    for event in events:
                        produce_json(
                            producer,
                            topic=TOPIC,
                            key=event.kafka_key(),
                            value=event.model_dump(mode="json"),
                            headers=event.kafka_headers(),
                        )
                    log.info("produced_batch", station=station or "(all)", count=len(events))
                # 매 cycle 즉시 broker commit. finally 의 flush 는 예외 / 비정상 경로 방어용
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
    DEFAULT_STATIONS = [""]   # 빈 문자열 = 전체 운행 열차 일괄 조회
    run(DEFAULT_STATIONS)
    sys.exit(0)
