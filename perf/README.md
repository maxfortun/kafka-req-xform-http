# Kafka with and without the content lake

Hypothesis: when a broker's disk is the bottleneck, moving large record content out of Kafka (claim check: the
record keeps only the lake's record id and URLs) raises produce throughput.

## Setup

One image is both brokers: the fork's jars plus this plugin. `INTERCEPTOR` picks the no-op interceptor (plain) or
the content-lake interceptor (lake), so that is the only difference between the two.

- **plain**: the producer's records, content included, are written to the broker's log.
- **lake**: the plugin posts each record to content-lake-api, which stores it in Redis (memory only; no AOF or
  snapshots) and announces it on a separate Kafka. The broker logs the claim check: an empty value and the
  `cl-api-*` headers. content-lake-cache persists records to Mongo (headers) and a volume (content) afterwards, outside
  the produce request and outside Kafka.
- **throttled**: the broker's disk writes are capped at 50 MB/s (`blkio device_write_bps`), with the broker limited to
  3 GB of memory so the page cache can't absorb the run. The lake's content volume is not throttled, standing in for
  S3. This models production: broker volumes are the expensive, constrained disk.

Each run: a fresh stack, 4 parallel `kafka-producer-perf-test.sh` producers, 3 GB of payload, 12 partitions,
`acks=all`, replication factor 1, no compression. Produce MB/s is payload over wall time. Persisted MB/s is payload over
the time until every record was in Mongo. Disk MB is bytes written per container (block I/O after `sync`).
Run on Docker Desktop (10 CPUs, 16 GB, one SSD shared by everything): one API pod and one cache worker unless noted.

## Results

| record | scenario | workers | produce MB/s | avg / p99 latency ms | broker disk MB | lake disk MB | persisted MB/s |
|---|---|---|---|---|---|---|---|
| 100 KB | plain | | 588 | 846 / 1741 | 3229 | | |
| 100 KB | lake | 1 | 56 | 15488 / 21236 | 18 | 3718 | 12.5 |
| 100 KB | plain, throttled | | 60 | 11519 / 29518 | 3219 | | |
| 100 KB | lake, throttled | 1 | 57 | 15114 / 21135 | 18 | 3719 | 12.7 |
| 1 MB | plain | | 579 | 755 / 1435 | 3229 | | |
| 1 MB | lake | 1 | 282 | 2746 / 3848 | 3 | 3236 | 97 |
| 1 MB | plain, throttled | | 59 | 13720 / 38734 | 3219 | | |
| 1 MB | lake, throttled | 1 | 290 | 2717 / 3698 | 2 | 3236 | 99 |
| 1 MB | lake, throttled | 4 | 246 | 3162 / 4557 | 9 | 3245 | 217 |
| 4 MB | plain | | 432 | 1241 / 3911 | 3229 | | |
| 4 MB | lake | 1 | 382 | 1933 / 3895 | 1 | 3224 | 224 |
| 4 MB | plain, throttled | | 56 | 14579 / 43022 | 3219 | | |
| 4 MB | lake, throttled | 1 | 384 | 1952 / 3785 | 0 | 3224 | 209 |
| 4 MB | lake, throttled | 4 | 308 | 2432 / 4320 | 0 | 3235 | 296 |

## What it shows

- **Confirmed when the broker disk is the bottleneck and records are large.** With the broker capped at 50 MB/s, plain
  Kafka is pinned at the cap (56-60 MB/s). The lake path doesn't touch that disk: 290 MB/s at 1 MB and 384 MB/s at
  4 MB, 5-7x more. Broker disk writes drop from 3.2 GB to almost nothing; the same bytes land on the lake's storage
  instead.
- **Durability has to keep up, or the gain is only a burst.** Produce speed is bounded by Redis, but durable speed is
  the cache workers'. One worker persisted 99 MB/s at 1 MB, below the produce rate, so Redis held the backlog. Four
  workers persisted 217-296 MB/s, close to the produce rate, so the durable rate is still about 4x the throttled
  plain broker. Size the workers (and topic partitions) so persistence keeps up; otherwise Redis fills and checkins
  fall back to synchronous durable writes.
- **Not worth it for small records.** At 100 KB the lake path manages about 56 MB/s (560 records/s) whether or not
  the disk is throttled, and one worker persists only 12.5 MB/s. Per-record costs dominate: an HTTP call per record
  inside the broker, a Redis write, an idempotency check and an event per checkin, then a Mongo write, a file and an
  event per record in the worker.
- **On a fast, unthrottled disk, plain Kafka is faster** (580 vs 282 MB/s at 1 MB). Kafka's sequential writes to a
  local SSD aren't the bottleneck there; the lake path adds a synchronous HTTP round trip per record, which also
  shows in latency (2.7 s average at 1 MB under this load, against 0.76 s plain).

## Caveats

- Replication factor 1. With the usual 3, every byte of content is written to three broker disks and sent over the
  replication network, so the plain path does 3x the broker I/O and the lake's advantage grows.
- Everything shares one machine's CPUs: the scaled-worker runs produced slightly less because the workers competed
  for CPU with the broker and the API. Separate nodes would not have that contention.
- The lake's durable storage is a local volume here, not S3; S3 throughput scales with parallelism.
- Redis peak memory equals the whole run (about 4 GB) because persisted records keep a 2-minute TTL; Redis needs room
  for (produce rate - persist rate) x burst length plus persisted_ttl_sec of traffic.
- One API pod. The plugin calls the lake synchronously per record within each produce request, so throughput also
  scales with producer connections and API pods.

## Run it

```
./prepare.sh                                   # fork jars from ~/.m2 (KAFKA_VERSION, default 3.7.2-MAX12) + plugin
./run.sh                                       # all scenarios, 100 KB / 1 MB / 4 MB
SIZES=1048576 SCENARIOS=lake_throttled CACHE_REPLICAS=4 ./run.sh
```

Settings: `SIZES`, `TOTAL_MB` (per run), `PRODUCERS`, `PARTITIONS`, `SCENARIOS`, `CACHE_REPLICAS`,
`BROKER_WRITE_BPS` (default `50mb`), `BROKER_DISK` (default `/dev/vda`, Docker Desktop's VM disk), `BROKER_MEM`.
Results go to `results/<timestamp>/`: one JSON per run and the producers' output.
