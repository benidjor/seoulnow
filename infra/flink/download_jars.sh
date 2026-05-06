#!/usr/bin/env bash
set -euo pipefail

JAR_DIR="$(dirname "$0")/jars"
mkdir -p "$JAR_DIR"

FLINK_VER=1.20.0
ICEBERG_VER=1.7.1
KAFKA_VER=3.3.0-1.20

declare -a JARS=(
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VER}/flink-sql-connector-kafka-${KAFKA_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VER}/iceberg-flink-runtime-1.20-${ICEBERG_VER}.jar"
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
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
