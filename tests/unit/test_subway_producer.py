from datetime import datetime

from producers.schemas import SubwayCongestionEvent
from producers.subway_producer import parse_subway_payload


def test_parse_subway_payload_returns_list(subway_sample):
    events = parse_subway_payload(subway_sample)
    assert len(events) == 2
    assert all(isinstance(e, SubwayCongestionEvent) for e in events)


def test_parse_subway_payload_extracts_fields(subway_sample):
    events = parse_subway_payload(subway_sample)
    e0 = events[0]
    assert e0.station_code == "1002000221"
    assert e0.station_name == "강남"
    assert e0.line_name == "1002"
    assert e0.train_no == "2034"
    assert e0.direction == "내선"
    assert e0.congestion_score is None   # realtimeStationArrival 미제공
    assert e0.congestion_level is None   # realtimeStationArrival 미제공
    assert e0.api_response_ts == datetime(2026, 4, 30, 14, 25, 30)


def test_parse_subway_payload_skips_when_error(subway_sample):
    subway_sample["errorMessage"]["code"] = "ERROR-500"
    assert parse_subway_payload(subway_sample) == []


def test_subway_event_kafka_key_includes_line(subway_sample):
    events = parse_subway_payload(subway_sample)
    assert events[0].kafka_key() == "1002:1002000221"
