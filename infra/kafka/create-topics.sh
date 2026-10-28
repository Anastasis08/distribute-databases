#!/usr/bin/env bash
# Creates the Kafka topics used by the pipeline. Safe to run repeatedly.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
TOPIC="${PRODUCTS_TOPIC:-products}"
DLQ_TOPIC="${PRODUCTS_DLQ_TOPIC:-products.dlq}"
PARTITIONS="${PRODUCTS_TOPIC_PARTITIONS:-3}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

echo "Waiting for Kafka at $BOOTSTRAP ..."
for attempt in $(seq 1 30); do
  if "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "Kafka is not reachable, giving up." >&2
    exit 1
  fi
  sleep 2
done

# Main topic: key = product id, so all changes of one product land in one partition (ordered).
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 \
  --config retention.ms=604800000

# Dead-letter topic for messages that can never be processed (invalid payload, rejected document).
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$DLQ_TOPIC" --partitions 1 --replication-factor 1 \
  --config retention.ms=2592000000

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --describe --topic "$DLQ_TOPIC"
echo "Kafka topics are ready."
