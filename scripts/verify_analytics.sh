#!/usr/bin/env bash
# Start the analytics profile (Schema Registry, Kafka Streams app, Kafka Connect, Postgres)
# on top of a running stack (scripts/deploy.sh) and verify it end to end:
#   * a job submitted through Kong reaches Postgres as per-region window totals,
#   * a poison message lands on the DLQ with its reason and does not stall the stream,
#   * Schema Registry holds the Avro subjects and rejects a breaking schema change.
set -euo pipefail
cd "$(dirname "$0")/.."

GATEWAY=${GATEWAY:-http://localhost:8080}
APIKEY=${APIKEY:-demo-key-change-me}
REGISTRY=http://localhost:8081
CONNECT=http://localhost:8083

kafka() { docker compose exec -T kafka /opt/kafka/bin/"$@"; }
psql_q() { docker compose exec -T postgres psql -U quotes -d analytics -tAc "$1"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

docker compose --profile analytics run --rm analytics-init
docker compose --profile analytics up -d --build --wait schema-registry postgres connect
docker compose --profile analytics up -d --build analytics

echo "== register the JDBC sink connector"
curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data @connect/region-stats-sink.json "$CONNECT/connectors/region-stats-postgres/config" > /dev/null

echo "== submit a bulk job through Kong (pinned to the Nuclio route)"
job='{"quotes": [
  {"items": [{"sku": "A-1", "qty": 3, "unit_price": "19.99"}], "region": "EU"},
  {"items": [{"sku": "B-2", "qty": 2, "unit_price": "5.00"}, {"sku": "C-3", "qty": 1, "unit_price": "1.00"}], "region": "IN"}
]}'
curl -fsS -X POST -H "apikey: $APIKEY" -H 'Content-Type: application/json' \
  --data "$job" "$GATEWAY/next/v1/quote-jobs"
echo

echo "== publish a poison message straight to quote-jobs"
echo 'poison-1|{"job_id": "poison-1", "quotes": []}' | kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic quote-jobs --property parse.key=true --property 'key.separator=|'

count_committed() {
  kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$1" --from-beginning \
    --isolation-level read_committed --timeout-ms 8000 --property print.value=false --property print.key=true \
    2>/dev/null | grep -c . || true
}

echo "== wait for Postgres to hold every quote the stream accepted"
for i in $(seq 1 40); do
  accepted=$(count_committed quote-requests)
  stored=$(psql_q "select coalesce(sum(quote_count), 0) from quote_region_stats" 2>/dev/null | tr -d '[:space:]' || true)
  echo "quote-requests=$accepted postgres_sum=${stored:-n/a}"
  [ "${accepted:-0}" -ge 2 ] && [ "$stored" = "$accepted" ] && break
  sleep 3
done
[ "${accepted:-0}" -ge 2 ] || fail "no quote events reached quote-requests"
[ "$stored" = "$accepted" ] || fail "postgres total $stored != $accepted accepted quotes"
psql_q "select region, window_start, quote_count, item_count, gross_amount_cents from quote_region_stats order by window_start, region"
eu_cents=$(psql_q "select coalesce(sum(gross_amount_cents), 0) from quote_region_stats where region = 'EU'" | tr -d '[:space:]')
[ "$eu_cents" -ge 5997 ] || fail "EU gross $eu_cents is missing the submitted 3 x 19.99"

echo "== dead-letter topic"
dlq=$(kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic quote-jobs-dlq --from-beginning \
  --isolation-level read_committed --timeout-ms 8000 --property print.headers=true --property print.key=true 2>/dev/null || true)
echo "$dlq"
echo "$dlq" | grep -q 'dlq.error:quotes must be a list of 1-50 requests' || fail "poison message not on the DLQ with its reason"

echo "== connector and schema registry"
status=$(curl -fsS "$CONNECT/connectors/region-stats-postgres/status")
echo "$status"
echo "$status" | python3 -c 'import json, sys; s = json.load(sys.stdin); sys.exit(0 if s["tasks"] and all(t["state"] == "RUNNING" for t in s["tasks"]) else 1)'   || fail "sink connector task is not RUNNING"

subjects=$(curl -fsS "$REGISTRY/subjects")
echo "subjects: $subjects"
for s in quote-requests-value quote-region-stats-value; do
  echo "$subjects" | grep -q "\"$s\"" || fail "subject $s missing"
done

# Adding a field without a default would break consumers reading old records under BACKWARD.
breaking=$(curl -fsS "$REGISTRY/subjects/quote-region-stats-value/versions/latest/schema" | python3 -c '
import json, sys
schema = json.load(sys.stdin)
schema["fields"].append({"name": "currency", "type": "string"})
print(json.dumps({"schema": json.dumps(schema)}))')
verdict=$(curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data "$breaking" \
  "$REGISTRY/compatibility/subjects/quote-region-stats-value/versions/latest")
echo "breaking change check: $verdict"
echo "$verdict" | grep -q '"is_compatible":false' || fail "registry accepted a breaking schema change"

echo "analytics pipeline verified"
