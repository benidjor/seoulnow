"""FastAPI app 의 라우트 등록 + /health smoke.

본격 통합 (실 Iceberg 쿼리 결과 검증) 은 Iceberg 데이터가 있어야 하므로
CI 가 아닌 로컬 검증 (Step 7 의 curl) 에서 수행. 본 test 는 라우트가
앱에 등록됐는지 + /health 가 200 인지만 단언.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from api.main import create_app


def test_health_returns_ok() -> None:
    app = create_app()
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_hotspots_route_registered() -> None:
    app = create_app()
    routes = [r.path for r in app.routes]  # type: ignore[attr-defined]
    assert "/api/hotspots" in routes
    assert "/api/hotspots/areas" in routes
