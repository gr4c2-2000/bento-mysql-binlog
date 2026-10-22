#!/usr/bin/env bash
# End-to-end check: write rows to MySQL, assert each sink received exactly the
# slice its filter allows.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/docker-compose.yml)

MYSQL=("${COMPOSE[@]}" exec -T mysql mysql -uroot -prootpass --silent)
RABBIT_API="http://localhost:15672/api"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

log "Starting the stack"
"${COMPOSE[@]}" up -d --build --wait

log "Waiting for replication to attach"
for _ in $(seq 1 60); do
  if "${COMPOSE[@]}" logs bento 2>/dev/null | grep -q "start sync binlog"; then
    break
  fi
  sleep 1
done
"${COMPOSE[@]}" logs bento | grep -m1 "start sync binlog" \
  || fail "the input never attached to the binlog"
sleep 2

log "Writing rows"
"${MYSQL[@]}" <<'SQL'
INSERT INTO shop.orders (id, status, amount) VALUES (1,'new',100.00),(2,'new',250.50),(3,'new',99.99);
INSERT INTO shop.customers (id, email) VALUES (1,'a@example.com'),(2,'b@example.com');
UPDATE shop.orders SET status='paid' WHERE id=1;
DELETE FROM shop.orders WHERE id=3;
DELETE FROM shop.customers WHERE id=2;
INSERT INTO ops.heartbeat (id) VALUES (1);
SQL

# shop.orders: 3 inserts + 1 update + 1 delete = 5 rows for Kafka.
# deletes anywhere in shop: 2 rows for RabbitMQ (one of them is also in Kafka).
# ops.heartbeat is outside include_tables and must reach neither.
KAFKA_WANT=5
RABBIT_WANT=2

kafka_dump() {
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic shop.orders \
    --from-beginning --timeout-ms 5000 \
    --property print.key=true --property key.separator='|' 2>/dev/null || true
}

rabbit_count() {
  # The queue may not have been published to yet; treat any hiccup as zero and
  # let the polling loop retry.
  curl -fsS -u guest:guest "$RABBIT_API/queues/%2F/shop.deletes" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("messages", 0))' 2>/dev/null \
    || echo 0
}

log "Waiting for both sinks"
KAFKA_OUT=""
for _ in $(seq 1 30); do
  KAFKA_OUT="$(kafka_dump)"
  KAFKA_GOT="$(printf '%s' "$KAFKA_OUT" | grep -c . || true)"
  RABBIT_GOT="$(rabbit_count)"
  [[ "$KAFKA_GOT" -ge "$KAFKA_WANT" && "$RABBIT_GOT" -ge "$RABBIT_WANT" ]] && break
  sleep 2
done

log "Kafka topic shop.orders"
printf '%s\n' "$KAFKA_OUT"
[[ "$KAFKA_GOT" -eq "$KAFKA_WANT" ]] || fail "Kafka: want $KAFKA_WANT messages, got $KAFKA_GOT"
grep -q '"table":"customers"' <<<"$KAFKA_OUT" && fail "Kafka received customers rows"
grep -q '"schema":"ops"' <<<"$KAFKA_OUT" && fail "Kafka received rows from a table outside include_tables"
grep -q '^null|' <<<"$KAFKA_OUT" && fail "Kafka message key interpolation produced null"

log "RabbitMQ queue shop.deletes"
RABBIT_BODY="$(curl -fsS -u guest:guest -H 'content-type: application/json' \
  -d '{"count":20,"ackmode":"ack_requeue_true","encoding":"auto"}' \
  "$RABBIT_API/queues/%2F/shop.deletes/get" \
  | python3 -c 'import json,sys
for m in json.load(sys.stdin):
    print(m["routing_key"], m["payload"])')"
printf '%s\n' "$RABBIT_BODY"
[[ "$RABBIT_GOT" -eq "$RABBIT_WANT" ]] || fail "RabbitMQ: want $RABBIT_WANT messages, got $RABBIT_GOT"
grep -q '"action":"insert"' <<<"$RABBIT_BODY" && fail "RabbitMQ received a non-delete event"

log "Committed binlog position"
"${COMPOSE[@]}" exec -T bento cat /var/lib/bento/binlog.pos

printf '\n\033[32mPASS\033[0m  Kafka=%s (orders only)  RabbitMQ=%s (deletes only)\n' \
  "$KAFKA_GOT" "$RABBIT_GOT"
