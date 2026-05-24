"""Day 11 Task 11.1-A: silver + gold snapshot 만료.

gold 가 3,314 snapshot 누적 → plan_files() manifest 읽기 비용(2.29s)의 원인.
rewrite_data_files 직후 실행되어 (1) 오래된 snapshot 제거 (2) 그 snapshot 만
참조하던 old small data file 회수. retain_last=5 로 최근 일부 보존.

Day 9 결정 A3(expire_snapshots placeholder 유지) 해제. rewrite >> expire 순서는
iceberg_maintenance DAG dependency chain 이 보장.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession

TABLES = ["silver.hotspot_congestion", "gold.fact_hotspot_congestion_5min"]
RETAIN_LAST = 5


def session() -> SparkSession:
    return SparkSession.builder.appName("scp.day11.expire_snapshots").getOrCreate()


def snapshot_count(spark: SparkSession, table: str) -> int:
    return int(spark.sql(f"SELECT count(*) AS n FROM ice.{table}.snapshots").collect()[0]["n"])


def main() -> None:
    spark = session()
    older_than = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")

    for table in TABLES:
        n_before = snapshot_count(spark, table)
        spark.sql(f"""
            CALL ice.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{older_than}',
                retain_last => {RETAIN_LAST}
            )
        """)
        n_after = snapshot_count(spark, table)
        print(f"{table}: snapshots {n_before} -> {n_after}")


if __name__ == "__main__":
    main()
