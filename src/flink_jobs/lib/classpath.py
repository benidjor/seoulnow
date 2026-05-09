"""PyFlink `pipeline.jars` 옵션용 connector JAR classpath.

`infra/flink/jars/*.jar` 5종 (kafka 3.3.0-1.20 / iceberg-flink-runtime 1.7.1
/ iceberg-aws-bundle 1.7.1 / hadoop-client-api 3.3.4 / hadoop-client-runtime
3.3.4) 을 sorted file:// URI list 로 반환.

Day 4 까지 `bronze_to_silver._classpath` private 함수가 단일 출처였고
`silver_to_gold` 가 sibling private import 로 호출 — Day 5 진입 전 본
모듈로 분리하면서 public 함수화 (silver_to_gold:38 TODO 정리).
"""
from __future__ import annotations

from pathlib import Path

#: `infra/flink/jars/` 절대 경로. 본 모듈 위치 기준 3 단계 상위
#: (lib → flink_jobs → src → <project_root>) 의 `infra/flink/jars`.
JAR_DIR = Path(__file__).resolve().parents[3] / "infra" / "flink" / "jars"


def flink_classpath() -> str:
    """`*.jar` sorted list 의 `;` 구분 file:// URI string."""
    jars = sorted(JAR_DIR.glob("*.jar"))
    return ";".join(f"file://{p}" for p in jars)
