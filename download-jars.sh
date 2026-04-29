#!/bin/bash
# Download Flink connector JARs to flink-libs/ before building the image
# Run this once from the project root:  bash download-jars.sh

set -e
mkdir -p flink-libs

echo "[1/9] Flink S3 Hadoop filesystem (resolves s3:// URIs in Flink layer)..."
curl -L -o flink-libs/flink-s3-fs-hadoop-1.18.1.jar \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar"

echo "[2/9] Kafka SQL connector..."
curl -L -o flink-libs/flink-sql-connector-kafka-3.1.0-1.18.jar \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"

echo "[3/9] Iceberg Flink runtime..."
curl -L -o flink-libs/iceberg-flink-runtime-1.18-1.5.2.jar \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar"

echo "[4/9] Iceberg AWS bundle (S3FileIO + AWS SDK v2)..."
curl -L -o flink-libs/iceberg-aws-bundle-1.5.2.jar \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar"

echo "[5/9] Hadoop client API (provides org.apache.hadoop.conf.Configuration)..."
curl -L -o flink-libs/hadoop-client-api-3.3.4.jar \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar"

echo "[6/9] Hadoop client runtime (uber-jar for Hadoop implementation)..."
curl -L -o flink-libs/hadoop-client-runtime-3.3.4.jar \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar"

echo "[7/9] Woodstox XML parser (unshaded - required by Hadoop config loading)..."
curl -L -o flink-libs/woodstox-core-6.5.0.jar \
  "https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.5.0/woodstox-core-6.5.0.jar"

echo "[8/9] Stax2 API (woodstox dependency)..."
curl -L -o flink-libs/stax2-api-4.2.1.jar \
  "https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar"

echo "[9/9] Flink Prometheus metrics reporter..."
curl -L -o flink-libs/flink-metrics-prometheus-1.18.1.jar \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar"

# Remove old individual Hadoop JARs if they exist from previous runs
rm -f flink-libs/hadoop-common-3.3.4.jar
rm -f flink-libs/hadoop-hdfs-client-3.3.4.jar
rm -f flink-libs/hadoop-auth-3.3.4.jar

echo ""
echo "Done! JARs in flink-libs/:"
ls -lh flink-libs/
