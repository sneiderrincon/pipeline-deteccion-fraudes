#!/bin/bash
# download_jars.sh — downloads Spark-Kafka connector JARs into spark/jars/
# Run once before docker compose up

set -e

JARS_DIR="$(dirname "$0")/spark/jars"
mkdir -p "$JARS_DIR"

SPARK_VERSION="3.5.0"
KAFKA_VERSION="3.7.0"
SCALA="2.12"
MVN="https://repo1.maven.org/maven2"

declare -A JARS=(
  ["spark-sql-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar"]="${MVN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar"
  ["spark-token-provider-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar"]="${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar"
  ["kafka-clients-${KAFKA_VERSION}.jar"]="${MVN}/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar"
  ["commons-pool2-2.11.1.jar"]="${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
)

echo "Downloading Spark-Kafka JARs into $JARS_DIR ..."
for jar in "${!JARS[@]}"; do
  target="$JARS_DIR/$jar"
  if [ -f "$target" ]; then
    echo "  [skip] $jar already exists"
  else
    echo "  [download] $jar"
    curl -fsSL "${JARS[$jar]}" -o "$target"
  fi
done

echo "Done. JARs in $JARS_DIR:"
ls -lh "$JARS_DIR"
