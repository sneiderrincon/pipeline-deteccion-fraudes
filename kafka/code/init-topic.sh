#!/bin/bash
set -e

TOPIC="${KAFKA_TOPIC:-fraud-transactions}"
SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION="${KAFKA_REPLICATION:-1}"

echo "[kafka-init] Waiting for Kafka broker at $SERVER..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$SERVER" --list > /dev/null 2>&1; do
  echo "[kafka-init] Not ready yet, retrying in 3s..."
  sleep 3
done

echo "[kafka-init] Kafka ready. Creating topic '$TOPIC'..."
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$SERVER" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

echo "[kafka-init] Done."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$SERVER" --describe --topic "$TOPIC"
