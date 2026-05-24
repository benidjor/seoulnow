"""Task 11.3 — `user.events.v1` JSON Schema (Draft 2020-12) 검증.

스키마 본문 = `infra/http-receiver/schemas/user_events_v1.json` (계약 단일 출처).
receiver 의 pydantic 모델 (Task 11.2) 과 1:1 매핑되며, 본 테스트는 스키마가
(a) event_id uuid format (b) event_type enum 을 실제로 강제하는지 단언한다.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

SCHEMA_PATH = Path(__file__).parents[2] / "infra" / "http-receiver" / "schemas" / "user_events_v1.json"


def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    # format_checker 를 넘겨야 Draft 2020-12 에서 "format": "uuid" 가
    # annotation 이 아니라 검증 규칙으로 동작한다.
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def _valid_payload() -> dict:
    return {
        "event_id": "8f14e45f-ceea-467a-9b2a-1a2b3c4d5e6f",
        "event_ts": "2026-05-15T01:00:00Z",
        "anon_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "event_type": "map_view",
    }


def test_event_id_must_be_uuid() -> None:
    validator = _validator()
    validator.validate(_valid_payload())  # 정상 payload 는 통과

    bad = _valid_payload()
    bad["event_id"] = "not-a-uuid"
    with pytest.raises(ValidationError):
        validator.validate(bad)


def test_event_type_must_be_in_enum() -> None:
    validator = _validator()
    validator.validate(_valid_payload())  # 정상 payload 는 통과

    bad = _valid_payload()
    bad["event_type"] = "unknown_event"
    with pytest.raises(ValidationError):
        validator.validate(bad)
