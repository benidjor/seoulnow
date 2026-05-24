"""Task 11.2 — HTTP receiver (FastAPI) 통합 테스트.

receiver = `infra/http-receiver/app.py`. Bearer 토큰 검증 → pydantic 검증 →
Kafka `user.events.v1` 발행. Kafka producer 는 AsyncMock 으로 대체하므로 실
브로커 연결 없이 (a) 정상 발행 (b) 401 (c) 422 3 case 를 단언한다.

`app.py` 는 모듈 로드 시 `KAFKA_BOOTSTRAP_SERVERS` / `RECEIVER_TOKEN` env 를
읽으므로 import 전에 설정한다 (테스트가 env 를 소유).
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

TOKEN = "test-token"
os.environ["RECEIVER_TOKEN"] = TOKEN
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

APP_PATH = Path(__file__).parents[2] / "infra" / "http-receiver" / "app.py"


def _load_app_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("http_receiver_app", APP_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # `from __future__ import annotations` 때문에 pydantic 이 forward-ref 를
    # 모듈 네임스페이스로 해석한다 → sys.modules 등록 필요 (uvicorn 정상 import 와 동치).
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def app_module() -> ModuleType:
    return _load_app_module()


@pytest.fixture
def client(app_module: ModuleType) -> TestClient:
    # context manager 없이 생성 → lifespan(real producer.start) 미실행.
    return TestClient(app_module.app)


def _valid_event() -> dict:
    return {
        "event_ts": "2026-05-15T01:00:00Z",
        "anon_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "event_type": "map_view",
        "page": {"path": "/"},
    }


def test_post_events_unauthorized(client: TestClient) -> None:
    resp = client.post(
        "/v1/events",
        headers={"Authorization": "Bearer wrong-token"},
        json={"events": [_valid_event()]},
    )
    assert resp.status_code == 401


def test_post_events_invalid_payload(client: TestClient) -> None:
    bad = _valid_event()
    del bad["event_ts"]  # 필수 필드 누락
    resp = client.post(
        "/v1/events",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={"events": [bad]},
    )
    assert resp.status_code == 422


def test_post_events_publishes_to_kafka(app_module: ModuleType, client: TestClient) -> None:
    mock_producer = AsyncMock()
    app_module.producer = mock_producer

    resp = client.post(
        "/v1/events",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={"events": [_valid_event()]},
    )

    assert resp.status_code == 200
    assert resp.json() == {"published": 1}

    mock_producer.send_and_wait.assert_awaited_once()
    call = mock_producer.send_and_wait.await_args
    assert call.args[0] == "user.events.v1"
    assert call.kwargs["key"] == "3fa85f64-5717-4562-b3fc-2c963f66afa6"
    headers = call.kwargs["headers"]
    assert any(name == "ingest_ts" and isinstance(value, bytes) for name, value in headers)
