#!/bin/bash
# Cria os tópicos dos dois protótipos com a mesma configuração de partições e réplicas.
# Roda nos dois modos, para que o cluster seja idêntico entre Software A e Software B.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
PARTITIONS="${KAFKA_PARTITIONS:-6}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-3}"
MIN_ISR="${KAFKA_MIN_INSYNC_REPLICAS:-2}"
BIN=/opt/kafka/bin

TOPICS=(
  paysim.domain-events.transactions
  paysim.cdc.public.transactions
  paysim.cdc.public.accounts
)

for t in "${TOPICS[@]}"; do
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$t" --partitions "$PARTITIONS" --replication-factor "$REPLICATION" \
    --config min.insync.replicas="$MIN_ISR"
done

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic 'paysim\..*' | grep -E '^Topic:' || true
