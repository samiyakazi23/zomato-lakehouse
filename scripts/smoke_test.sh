#!/usr/bin/env bash
# Verifies every layer of the running platform. Usage: bash scripts/smoke_test.sh
set -uo pipefail
# works for both the standard and the lite stack
if docker ps --format "{{.Names}}" | grep -q "^zomato-spark-master$"; then DC="docker compose"; else DC="docker compose -f docker-compose.lite.yml"; fi
pass=0; fail=0
ok()   { echo "  [PASS] $1"; pass=$((pass+1)); }
bad()  { echo "  [FAIL] $1"; fail=$((fail+1)); }
sql()  { $DC exec -T postgres psql -U zomato -d zomato -Atc "$1" 2>/dev/null | tr -d '[:space:]'; }

echo "== Kafka"
topics=$($DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -c '^zomato\.')
[ "${topics:-0}" -ge 4 ] && ok "4 zomato topics exist" || bad "zomato topics missing (found ${topics:-0})"

echo "== Reference data"
n=$(sql "select count(*) from dims.restaurants"); [ "${n:-0}" -gt 0 ] && ok "dims.restaurants = $n" || bad "dims not loaded"

echo "== Spark Structured Streaming"
q=$(sql "select count(distinct query_name) from realtime.stream_progress where ts > now() - interval '3 minutes'")
[ "${q:-0}" -ge 10 ] && ok "$q streaming queries committing" || bad "only ${q:-0}/10 streaming queries active"
lag=$(sql "select extract(epoch from now() - max(window_start))::int from realtime.city_orders_1m")
[ -n "$lag" ] && [ "$lag" -lt 300 ] && ok "real-time windows fresh (${lag}s)" || bad "real-time windows stale (${lag:-none})"
rps=$(sql "select round(sum(input_rps)) from (select distinct on (query_name) query_name, input_rps from realtime.stream_progress where query_name='bronze_events' order by query_name, ts desc) t")
echo "         bronze ingest rate: ${rps:-0} events/sec"

echo "== Lakehouse (S3)"
objs=$($DC run --rm -T --entrypoint aws s3-init --endpoint-url http://s3:9000 s3 ls s3://lakehouse --recursive 2>/dev/null | wc -l)
[ "${objs:-0}" -gt 0 ] && ok "$objs objects in s3://lakehouse" || bad "lakehouse bucket empty"

echo "== Batch (Airflow -> Spark gold -> dbt)"
g=$(sql "select count(*) from gold.daily_city_metrics"); [ "${g:-0}" -gt 0 ] && ok "gold.daily_city_metrics = $g rows" || bad "gold not built yet (backfill/gold DAG still running?)"
m=$(sql "select count(*) from analytics.mart_restaurant_rankings"); [ "${m:-0}" -gt 0 ] && ok "dbt marts built ($m restaurants ranked)" || bad "dbt marts not built yet"

echo ""; echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
