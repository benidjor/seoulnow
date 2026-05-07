#!/usr/bin/env bash
set -euo pipefail

JAR_DIR="$(dirname "$0")/jars"
mkdir -p "$JAR_DIR"

FLINK_VER=1.20.0
FLINK_MINOR="${FLINK_VER%.*}"
ICEBERG_VER=1.7.1
KAFKA_VER=3.3.0-${FLINK_MINOR}
HADOOP_VER=3.3.4

declare -a JARS=(
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VER}/flink-sql-connector-kafka-${KAFKA_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MINOR}/${ICEBERG_VER}/iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VER}/iceberg-aws-bundle-${ICEBERG_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VER}/hadoop-client-api-${HADOOP_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VER}/hadoop-client-runtime-${HADOOP_VER}.jar"
)

for url in "${JARS[@]}"; do
  fname=$(basename "$url")
  if [[ -f "$JAR_DIR/$fname" ]]; then
    echo "skip $fname"
    continue
  fi
  echo "downloading $fname"
  curl -fsSL -o "$JAR_DIR/$fname" "$url"
done

echo
ls -lh "$JAR_DIR"

# PyFlink driver JVM 의 classpath 에 connector JAR 노출.
# Iceberg FlinkCatalogFactory.clusterHadoopConf 가 driver 단계에서 hadoop / aws class 를 require.
# pipeline.jars 옵션은 작업 (INSERT) runtime 에만 적용되어 driver 단계 catalog 등록에는 부족.
# .venv 가 다른 경로면 환경 변수로 override 가능.
if [[ -z "${PYFLINK_LIB_DIR:-}" ]]; then
  PYFLINK_LIB_DIR="$(uv run python -c 'import pyflink, os; print(os.path.dirname(pyflink.__file__) + "/lib")' 2>/dev/null || true)"
fi

if [[ -n "${PYFLINK_LIB_DIR:-}" && -d "$PYFLINK_LIB_DIR" ]]; then
  echo
  echo "syncing JARs to PyFlink lib: $PYFLINK_LIB_DIR"
  cp "$JAR_DIR"/*.jar "$PYFLINK_LIB_DIR/"
  ls -lh "$PYFLINK_LIB_DIR/" | grep -E "iceberg|hadoop-client|kafka" || true
else
  echo
  echo "(skip) PyFlink lib not found. uv sync --extra flink 후 본 스크립트 재실행 권장."
fi
