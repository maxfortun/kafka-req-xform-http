#!/bin/bash
# Produces the same payload through a plain broker and a lake broker, with and without a throttled broker disk.
#   ./prepare.sh && ./run.sh
# Env: SIZES (bytes), TOTAL_MB per run, PRODUCERS (parallel perf-test processes), PARTITIONS, SCENARIOS, CACHE_REPLICAS,
#      BROKER_WRITE_BPS for the throttled scenarios (default 50mb).
set -euo pipefail
cd "$(dirname "$0")"

SIZES=${SIZES:-"102400 1048576 4194304"}
TOTAL_MB=${TOTAL_MB:-3072}
PRODUCERS=${PRODUCERS:-4}
PARTITIONS=${PARTITIONS:-12}
SCENARIOS=${SCENARIOS:-"plain lake plain_throttled lake_throttled"}
PERSIST_TIMEOUT_SEC=${PERSIST_TIMEOUT_SEC:-900}
CACHE_REPLICAS=${CACHE_REPLICAS:-1}
out=results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$out"

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

# Bytes written per container since it started.
io_snapshot() {
	docker stats --no-stream --format '{{.Name}}|{{.BlockIO}}' | python3 -c '
import sys, json, re
units = {"B": 1, "kB": 1e3, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}
def size(text):
    number, unit = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip()).groups()
    return int(float(number) * units[unit])
snapshot = {}
for line in sys.stdin:
    name, io = line.strip().split("|")
    if name.startswith("content-lake-perf-"):
        service = name.removeprefix("content-lake-perf-").rsplit("-", 1)[0]
        snapshot[service] = snapshot.get(service, 0) + size(io.split("/")[1])
print(json.dumps(snapshot))'
}

files_for() {
	if [[ $1 == *throttled ]]; then echo "-f docker-compose.yml -f throttle.yml"; else echo "-f docker-compose.yml"; fi
}

docker compose -p content-lake-perf build

for scenario in $SCENARIOS; do
	files=$(files_for "$scenario")
	dc() { docker compose -p content-lake-perf $files "$@"; }
	if [[ $scenario == lake* ]]; then
		export INTERCEPTOR=org.apache.kafka.common.requests.transform.TransformingProduceRequestInterceptor
	else
		export INTERCEPTOR=org.apache.kafka.common.requests.DefaultProduceRequestInterceptor
	fi
	for size in $SIZES; do
		echo "=== $scenario, ${size}-byte records"
		dc down -v --remove-orphans >/dev/null 2>&1 || true
		dc up -d --wait --scale cache="$CACHE_REPLICAS" >/dev/null
		topic=perf-$size
		dc exec -T broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$topic" --partitions "$PARTITIONS" >/dev/null
		records=$(( TOTAL_MB * 1024 * 1024 / size / PRODUCERS ))
		total=$(( records * PRODUCERS ))

		io_before=$(io_snapshot)
		started=$(now_ms)
		dc exec -T perf bash -c "
			for i in \$(seq $PRODUCERS); do
				/opt/kafka/bin/kafka-producer-perf-test.sh --topic $topic --num-records $records --record-size $size --throughput -1 \
					--producer-props bootstrap.servers=broker:9092 acks=all linger.ms=5 batch.size=1048576 max.request.size=20971520 \
					buffer.memory=268435456 compression.type=none request.timeout.ms=120000 delivery.timeout.ms=900000 \
					> /tmp/producer.\$i.log 2>&1 &
			done
			wait
			for i in \$(seq $PRODUCERS); do tail -n1 /tmp/producer.\$i.log; done" > "$out/$scenario-$size.producers.log"
		produced=$(now_ms)

		persisted=None
		redis_peak=
		if [[ $scenario == lake* ]]; then
			deadline=$(( $(date +%s) + PERSIST_TIMEOUT_SEC ))
			while :; do
				count=$(dc exec -T mongo mongosh --quiet --eval 'db.getSiblingDB("content_lake").records.countDocuments()' | tr -d '\r')
				if (( count >= total )); then persisted=$(now_ms); break; fi
				if (( $(date +%s) > deadline )); then echo "  persisted only $count of $total"; break; fi
				sleep 1
			done
			redis_peak=$(dc exec -T redis redis-cli info memory | tr -d '\r' | awk -F: '$1=="used_memory_peak"{print $2}')
		fi
		# Flush page caches so block I/O counts the bytes that actually reach the disk.
		dc exec -T broker sync
		dc exec -T cache sync
		io_after=$(io_snapshot)

		python3 - "$out/$scenario-$size.producers.log" > "$out/$scenario-$size.json" <<PY
import json, re, sys
lines = [line for line in open(sys.argv[1]) if 'records sent' in line]
sum_mb = sum(float(re.search(r'\(([\d.]+) MB/sec\)', line).group(1)) for line in lines)
p99 = max(float(re.search(r'([\d.]+) ms 99th', line).group(1)) for line in lines) if lines else None
avg = sum(float(re.search(r'([\d.]+) ms avg latency', line).group(1)) for line in lines) / len(lines) if lines else None
before, after = json.loads('''$io_before'''), json.loads('''$io_after''')
written = {name: after.get(name, 0) - before.get(name, 0) for name in after}
elapsed = ($produced - $started) / 1000
payload_mb = $total * $size / 1e6
persisted = $persisted
print(json.dumps({
    "scenario": "$scenario", "cache_replicas": $CACHE_REPLICAS, "record_bytes": $size, "records": $total, "producers": $PRODUCERS, "payload_mb": round(payload_mb, 1),
    "producers_reporting": len(lines), "produce_sec": round(elapsed, 1), "produce_mb_per_sec": round(payload_mb / elapsed, 1),
    "producer_reported_mb_per_sec": round(sum_mb, 1), "avg_latency_ms": avg, "max_p99_latency_ms": p99,
    "persist_sec": round((persisted - $started) / 1000, 1) if persisted else None,
    "persisted_mb_per_sec": round(payload_mb / ((persisted - $started) / 1000), 1) if persisted else None,
    "redis_peak_mb": round(int("$redis_peak") / 1e6) if "$redis_peak" else None,
    "written_mb": {name: round(value / 1e6) for name, value in sorted(written.items()) if value > 1e6}
}))
PY
		cat "$out/$scenario-$size.json"
	done
	dc down -v --remove-orphans >/dev/null 2>&1 || true
done

python3 summarize.py "$out"
