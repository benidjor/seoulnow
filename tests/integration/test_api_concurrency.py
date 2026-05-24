"""API 동시성 integration — 병렬 요청 전부 200 (정찰 Promise.all 502 회귀 방지).

라이브 스택(MinIO + Lakekeeper) 필요. localhost endpoint 로 실행해야 함:
  MINIO_ENDPOINT=http://localhost:9000 LAKEKEEPER_URL=http://localhost:8181 \\
    PYTHONPATH=src pytest tests/integration/test_api_concurrency.py -v
스택/데이터 없으면 skip.
"""
from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(ROOT / "src"))

pytest.importorskip("fastapi")
pytest.importorskip("duckdb")


@pytest.fixture(scope="module")
def client():
    from fastapi.testclient import TestClient

    from api.deps import catalog

    # 라이브 스택 reachable 확인 — 아니면 skip
    try:
        catalog().load_table("gold.fact_hotspot_congestion_5min")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"라이브 Iceberg 스택 미가용: {exc}")

    from api.main import app

    return TestClient(app)


def test_parallel_requests_all_200(client):
    """hotspots + chill-open 을 16 병렬 → 전부 200 (502 회귀 방지)."""
    paths = ["/api/hotspots", "/api/chill-open"] * 8

    def _get(path: str) -> int:
        return client.get(path).status_code

    with ThreadPoolExecutor(max_workers=16) as ex:
        codes = list(ex.map(_get, paths))

    assert all(c == 200 for c in codes), f"non-200 발생: {codes}"
