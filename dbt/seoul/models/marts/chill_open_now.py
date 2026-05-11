"""dbt python model — gold.chill_open_now (한가 + 영업중 가게 후보 view).

Day 8 Task 8.2.

deviation 8.2-B (옵션 1 채택: python model) — plan 본문 (line 1718~1773) 의 SQL view
는 `iceberg_scan('s3://.../gold/fact_hotspot_congestion_5min')` 직접 호출을 가정.
Day 4 archive `2026-05-09-day-4-tasks-4_1-4_3.md` 학습대로 Lakekeeper REST 가 vend
하는 UUID-prefix path 를 DuckDB `iceberg_scan` 이 resolve 못함 → python model 로
변환하면서 lib `table_paths()` 우회로 변경. dim_place (PR #38) / stg_hotspot_silver
(PR #29) 와 동일 패턴 — lib reuse 7번째 consumer 정착.

옵션 1 vs 옵션 2 (jinja macro) 선택 근거:
- jinja macro 로 path 주입은 가능하나 lib import 가 macro context 에 더 복잡.
- dim_place.py / stg_hotspot_silver.py 와 동일한 컨벤션이 유지보수 단순.
- LOC 차이 미미 (둘 다 ~90 line).

mart 의 책임:
- `gold.fact_hotspot_congestion_5min` 의 district 별 latest avg_congest_score.
- `dim_place` (dbt python model, CDC 시드) + `bronze.places_static` (정적 인허가) 를
  biz_reg_no 로 union + dedup (`row_number() ... ORDER BY place_id NULLS LAST`
  → 같은 biz_reg_no 면 CDC 출처 우선).
- district join + status='active' 필터.
- 영업중 판정 (`is_open_now`) 은 본 mart 에서 계산하지 않음 — API 응답 시점에
  Python `datetime.now().hour` 로 재계산 (timezone / clock skew 안전).
- 출력: 한가 (avg_congest_score <= 2) 후보 가게 list. API 가 추가 영업중 filter.

dim_place 참조 — dbt-duckdb python model 에서 `dbt.ref(name)` 은 DuckDB
Relation 객체 (또는 in-memory view name) 반환. f-string interpolation 으로
SQL 안에서 그대로 사용.
"""
from __future__ import annotations


def model(dbt, session):
    dbt.config(materialized="table", schema="gold")

    from flink_jobs.lib.duckdb_iceberg import (
        build_catalog,
        configure_duckdb,
        table_paths,
    )

    # platform_common.get_settings 는 dbt-duckdb runtime 의 PYTHONPATH 에서
    # 자동 잡힘 (Day 5 stg_hotspot_silver.py / Day 6 dim_place.py 정착 패턴).
    from platform_common import get_settings

    catalog = build_catalog()
    gold_paths = table_paths(catalog, "gold.fact_hotspot_congestion_5min")

    s = get_settings()
    places_static_path = (
        f"s3://{s.iceberg_warehouse_bucket}/warehouse/bronze/places_static_v1/data.parquet"
    )

    # dim_place 는 dbt python model — dbt.ref() 가 DuckDB Relation 객체 반환.
    # f-string 으로 직접 박으면 relation 의 데이터가 dump 되므로 view 로 등록 후 name 으로 참조.
    dim_place_relation = dbt.ref("dim_place")
    dim_place_relation.to_view("dim_place_view", replace=True)

    if not gold_paths:
        # streaming 미가동 / snapshot 0 → empty schema-only result.
        return session.sql(
            """
            SELECT
                CAST(NULL AS VARCHAR)   AS biz_reg_no,
                CAST(NULL AS VARCHAR)   AS name,
                CAST(NULL AS VARCHAR)   AS category,
                CAST(NULL AS VARCHAR)   AS district,
                CAST(NULL AS VARCHAR)   AS gu_code,
                CAST(NULL AS DOUBLE)    AS latitude,
                CAST(NULL AS DOUBLE)    AS longitude,
                CAST(NULL AS INTEGER)   AS open_hour,
                CAST(NULL AS INTEGER)   AS close_hour,
                CAST(NULL AS DOUBLE)    AS avg_congest_score
            WHERE 1 = 0
            """
        )

    configure_duckdb(session)

    return session.sql(
        f"""
        WITH district_score AS (
            SELECT district, avg_congest_score
            FROM (
                SELECT
                    district,
                    avg_congest_score,
                    row_number() OVER (
                        PARTITION BY district ORDER BY window_start DESC
                    ) AS rn
                FROM read_parquet({gold_paths!r}, hive_partitioning = true)
            )
            WHERE rn = 1
        ),
        places_combined AS (
            SELECT
                place_id,
                biz_reg_no, name, category, district, gu_code,
                latitude, longitude, open_hour, close_hour, status
            FROM dim_place_view
            UNION ALL
            SELECT
                NULL AS place_id,
                biz_reg_no, name, category, district, gu_code,
                latitude, longitude, open_hour, close_hour, status
            FROM read_parquet('{places_static_path}')
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY biz_reg_no ORDER BY place_id NULLS LAST
                ) AS rn
            FROM places_combined
        )
        SELECT
            p.biz_reg_no,
            p.name,
            p.category,
            p.district,
            p.gu_code,
            p.latitude,
            p.longitude,
            p.open_hour,
            p.close_hour,
            d.avg_congest_score
        FROM ranked p
        JOIN district_score d USING (district)
        WHERE p.status = 'active'
          AND p.rn = 1
          AND d.avg_congest_score <= 2
        ORDER BY d.avg_congest_score ASC, p.name
        """
    )
