"""GET /api/hotspots, /api/hotspots/areas — Iceberg silver/gold 직접 조회.

deviation 7.2-B — plan 본문의 `iceberg_scan('{base}/...')` 직접 호출 폐기.
Day 4 학습대로 pyiceberg `plan_files()` 로 받은 parquet path list 를
DuckDB `read_parquet(?, hive_partitioning=true)` 로 read. path list 는
`deps.silver_table_paths` / `deps.gold_table_paths` 가 lib 위임.

응답 schema 검증 — silver_to_gold.py 의 gold DDL 11 컬럼 + bronze_to_silver.py
의 silver DDL 17 컬럼 모두 plan 의 응답 cols 와 일치 → deviation E 불필요.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from api.deps import duck_cursor, gold_table_paths, silver_table_paths

router = APIRouter(prefix="/api", tags=["hotspots"])


def _row_to_dict(cols: list[str], row: tuple) -> dict[str, Any]:
    """datetime 류는 ISO-8601 문자열로 직렬화. JSON 응답 안전."""
    out: dict[str, Any] = {}
    for col, val in zip(cols, row, strict=True):
        out[col] = val.isoformat() if hasattr(val, "isoformat") else val
    return out


@router.get("/hotspots")
def list_hotspots() -> dict[str, Any]:
    """district 별 latest 5분 윈도우 (avg/max congest score, area_count)."""
    paths = gold_table_paths()
    if not paths:
        return {"items": [], "count": 0}

    con = duck_cursor()
    rows = con.execute(
        """
        WITH latest AS (
            SELECT *, row_number() OVER (
                PARTITION BY district ORDER BY window_start DESC
            ) AS rn
            FROM read_parquet(?, hive_partitioning = true)
        )
        SELECT
            district,
            gu_code,
            window_start,
            area_count,
            avg_congest_score,
            max_congest_score
        FROM latest
        WHERE rn = 1
        ORDER BY avg_congest_score DESC NULLS LAST
        """,
        [paths],
    ).fetchall()
    cols = [
        "district",
        "gu_code",
        "window_start",
        "area_count",
        "avg_congest_score",
        "max_congest_score",
    ]
    items = [_row_to_dict(cols, r) for r in rows]
    return {"items": items, "count": len(items)}


@router.get("/hotspots/areas")
def list_areas() -> dict[str, Any]:
    """핫스팟(area) 별 latest silver 1행. 위경도 + 최신 score (지도 마커용)."""
    paths = silver_table_paths()
    if not paths:
        return {"items": [], "count": 0}

    con = duck_cursor()
    rows = con.execute(
        """
        WITH latest AS (
            SELECT *, row_number() OVER (
                PARTITION BY area_code ORDER BY silver_arrival_ts DESC
            ) AS rn
            FROM read_parquet(?, hive_partitioning = true)
        )
        SELECT area_code, area_name, district, latitude, longitude,
               congest_level_score, congest_level, api_response_ts
        FROM latest WHERE rn = 1
        """,
        [paths],
    ).fetchall()
    cols = [
        "area_code",
        "area_name",
        "district",
        "latitude",
        "longitude",
        "congest_level_score",
        "congest_level",
        "api_response_ts",
    ]
    items = [_row_to_dict(cols, r) for r in rows]
    return {"items": items, "count": len(items)}
