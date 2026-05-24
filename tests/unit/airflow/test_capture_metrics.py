"""capture_metrics.py multi-table 단위 — build_catalog mock 으로 네트워크 차단."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).parents[3]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))
sys.path.insert(0, str(ROOT / "src"))

pyiceberg = pytest.importorskip("pyiceberg")  # host venv 에 pyiceberg 없으면 skip


def _fake_table(n_files: int, n_snapshots: int, file_bytes: int):
    table = MagicMock()
    files = []
    for _ in range(n_files):
        f = MagicMock()
        f.file.file_size_in_bytes = file_bytes
        files.append(f)
    table.scan.return_value.plan_files.return_value = files
    table.snapshots.return_value = list(range(n_snapshots))
    return table


def test_capture_metrics_multi_table_outputs_list(capsys):
    """두 table 인자 → stdout 마지막 line 이 {"tables":[...]} 2개 entry."""
    from common import capture_metrics  # type: ignore

    fake_catalog = MagicMock()
    fake_catalog.load_table.side_effect = [
        _fake_table(n_files=9161, n_snapshots=3314, file_bytes=1024),
        _fake_table(n_files=12, n_snapshots=5, file_bytes=2_000_000),
    ]
    with patch.object(capture_metrics, "_build_catalog", return_value=fake_catalog):
        capture_metrics.main(["silver.hotspot_congestion", "gold.fact_hotspot_congestion_5min"])

    out = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(out)
    assert "tables" in payload
    assert len(payload["tables"]) == 2
    assert payload["tables"][0]["table"] == "silver.hotspot_congestion"
    assert payload["tables"][0]["files"] == 9161
    assert payload["tables"][0]["snapshots"] == 3314
    assert payload["tables"][0]["bytes"] == 9161 * 1024
    assert payload["tables"][1]["table"] == "gold.fact_hotspot_congestion_5min"
    assert payload["tables"][1]["files"] == 12
    assert payload["tables"][1]["bytes"] == 12 * 2_000_000


def test_capture_metrics_single_table_backward_compat(capsys):
    """단일 table 인자도 동작 (1-element tables list)."""
    from common import capture_metrics  # type: ignore

    fake_catalog = MagicMock()
    fake_catalog.load_table.side_effect = [_fake_table(3, 2, 1024)]
    with patch.object(capture_metrics, "_build_catalog", return_value=fake_catalog):
        capture_metrics.main(["silver.hotspot_congestion"])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert len(payload["tables"]) == 1
