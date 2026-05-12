"""Day 9: MERGE INTO 멱등성 검증.

silver.dim_place (append SCD2 골격) → gold.dim_place (current snapshot).
같은 입력으로 두 번 MERGE 했을 때 row count + content hash 가 동일해야 한다.

Plan SoT 사후 보강 (commit 1 의 spark-defaults.conf 와 동일 deviation):
- Deviation 9.1-A: Lakekeeper warehouse = `seoul` (name SoT, S3 path 아님)
- Deviation 9.1-B: iceberg-aws-bundle-1.7.1.jar 의 AWS SDK v2 필요
- Deviation 9.2-A: 3-part identifier (`ice.gold.dim_place`, NOT `ice.seoul.gold.dim_place`).
  Lakekeeper REST 안의 actual namespace = flat single-level (bronze/silver/gold).
  cdc_to_dim_place.py + iceberg_sink.py SoT 따름.
"""

from __future__ import annotations

import hashlib
import sys

from pyspark.sql import SparkSession


def session() -> SparkSession:
    return SparkSession.builder.appName("scp.day9.merge_dim_place").getOrCreate()


GOLD_DDL = """
CREATE TABLE IF NOT EXISTS ice.gold.dim_place (
    place_id BIGINT,
    biz_reg_no STRING,
    name STRING,
    category STRING,
    district STRING,
    gu_code STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    open_hour INT,
    close_hour INT,
    status STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
)
USING iceberg
PARTITIONED BY (district)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
)
"""


MERGE_SQL = """
MERGE INTO ice.gold.dim_place t
USING (
    WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY place_id ORDER BY valid_from DESC) AS rn
        FROM ice.silver.dim_place
    )
    SELECT
        place_id, biz_reg_no, name, category, district, gu_code,
        latitude, longitude, open_hour, close_hour, status,
        valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        (cdc_op <> 'd') AS is_current
    FROM ranked WHERE rn = 1
) s
ON t.place_id = s.place_id
WHEN MATCHED AND (
        t.name <> s.name OR t.status <> s.status OR
        t.open_hour <> s.open_hour OR t.close_hour <> s.close_hour
    ) THEN UPDATE SET
        name = s.name, category = s.category,
        district = s.district, gu_code = s.gu_code,
        latitude = s.latitude, longitude = s.longitude,
        open_hour = s.open_hour, close_hour = s.close_hour,
        status = s.status, valid_from = s.valid_from,
        valid_to = s.valid_to, is_current = s.is_current
WHEN MATCHED THEN UPDATE SET is_current = s.is_current
WHEN NOT MATCHED THEN INSERT *
"""


def snapshot_signature(spark: SparkSession) -> tuple[int, str]:
    df = spark.sql("""
        SELECT place_id, biz_reg_no, name, category, district, status,
               open_hour, close_hour, is_current
        FROM ice.gold.dim_place
        ORDER BY place_id
    """)
    rows = df.collect()
    h = hashlib.sha256(repr(rows).encode("utf-8")).hexdigest()
    return len(rows), h


def main() -> None:
    spark = session()
    spark.sql(GOLD_DDL)

    print("== first MERGE ==")
    spark.sql(MERGE_SQL)
    n1, h1 = snapshot_signature(spark)
    print(f"after 1st merge: rows={n1} hash={h1[:12]}...")

    print("== second MERGE (idempotent expected) ==")
    spark.sql(MERGE_SQL)
    n2, h2 = snapshot_signature(spark)
    print(f"after 2nd merge: rows={n2} hash={h2[:12]}...")

    if (n1, h1) != (n2, h2):
        print("FAIL: not idempotent")
        sys.exit(1)
    print("OK: idempotent (rows + content hash 동일)")


if __name__ == "__main__":
    main()
