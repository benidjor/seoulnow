from datetime import UTC, datetime

from producers.hotspot_producer import parse_hotspot_payload
from producers.schemas import HotspotEvent


def test_parse_hotspot_payload_extracts_core_fields(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")

    assert isinstance(event, HotspotEvent)
    assert event.area_code == "POI001"
    assert event.area_name == "강남역"
    assert event.congest_level == "붐빔"
    assert event.population_min == 42000
    assert event.population_max == 44000
    assert event.api_response_ts == datetime(2026, 4, 30, 14, 25, tzinfo=UTC).replace(tzinfo=None)
    # 공기/도로/날씨는 옵셔널
    assert event.road_traffic_index == "서행"
    assert event.temperature_c == 21.3


def test_parse_hotspot_payload_returns_none_when_missing(hotspot_sample):
    bad = {"RESULT": {"RESULT.CODE": "ERROR-500"}}
    assert parse_hotspot_payload(bad, area_code="POI001") is None


def test_hotspot_event_kafka_key_is_area_code(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")
    assert event.kafka_key() == "POI001"


def test_hotspot_event_kafka_headers_includes_api_response_ts(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")
    headers = dict(event.kafka_headers())
    assert "api_response_ts" in headers
    # 헤더는 bytes
    assert headers["api_response_ts"] == b"2026-04-30T14:25:00"
    assert headers["schema_version"] == b"v1"
