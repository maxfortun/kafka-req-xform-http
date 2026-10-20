# Kafka with and without the content lake

Hypothesis: Kafka's bottleneck for large records is broker disk I/O, so moving the content out of Kafka (claim check:
the record keeps only the lake's record id and URLs) allows higher throughput.

**Result:** confirmed when the broker's disk is the constraint and records are 1-100 MB: 2-7x the produce throughput
of a disk-bound broker, with almost no broker disk writes. It does not help small records (100 KB) or brokers with
spare disk bandwidth, and at 1 GB per record the producer client is the limit on both paths. Projected at scale with
7-day retention, the lake costs 50-80% less than plain Kafka. Against Kafka with tiered storage it is about even at
1-4 MB records, and cheaper when the lake's traffic stays in one availability zone or records exceed the cache limit.

## Setup

One image is both brokers: the Kafka fork's jars plus this plugin. `INTERCEPTOR` selects the no-op interceptor
(plain) or the content-lake interceptor (lake), so that is the only difference between the two.

- **plain**: records, content included, are written to the broker's log.
- **lake**: the plugin posts each record to content-lake-api, which keeps it in Redis (memory only: no AOF, no
  snapshots) and announces it on a separate Kafka, then answers with an empty body and `cl-api-*` headers. The broker
  logs that claim check. content-lake-cache persists records to Mongo (headers) and a volume (content) afterwards,
  outside the produce request and outside Kafka. Records over 10 MB skip Redis: the API writes them straight to
  durable storage within the request.
- **throttled**: the broker's disk writes are capped at 50 MB/s (`blkio device_write_bps`), and the broker's memory is
  capped so the page cache can't absorb the run. The lake's content volume is not throttled, standing in for S3:
  in production the broker volumes are the expensive, constrained disk.

Each run starts a fresh stack and sends 3-6 GB of payload with `kafka-producer-perf-test.sh`, 12 partitions,
`acks=all`, replication factor 1, no compression. Produce MB/s is payload over wall time; persisted MB/s is payload over
the time until every record was in Mongo. Disk MB is bytes written per container (block I/O after `sync`). CPU is each
container's cgroup CPU time over the run. Hardware: Docker Desktop on one laptop (10 CPUs, 16 GB, one SSD), one API
pod, one cache worker unless noted.

## Results

### 100 KB to 4 MB records, 4 producers

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

### 100 MB and 1 GB records

100 MB: 4 producers, 4 GB per run. 1 GB: 1 producer, 4 GB per run (2 producers got the plain broker OOM-killed at a
9 GB container limit). Producers need a send buffer sized to a few records (`buffer.memory` 500 MB for 100 MB records);
a 2 GB buffer starved the producer's heap and cut throughput by 4x.

| record | scenario | produce MB/s | avg latency s | broker disk MB | lake disk MB | persisted MB/s |
|---|---|---|---|---|---|---|
| 100 MB | plain | 157 | 9.9 | 4049 | | |
| 100 MB | lake | 130 | 12.5 | 51 | 4183 | 128 |
| 100 MB | plain, throttled | 57 | 27.9 | 4139 | | |
| 100 MB | lake, throttled | 119 | 13.7 | 25 | 4068 | 118 |
| 1 GB | plain | 18 | 92 | 4069 | | |
| 1 GB | lake | 17 | 97 | 147 | 4210 | 17 |
| 1 GB | plain, throttled | 17 | 100 | 4119 | | |
| 1 GB | lake, throttled | 18 | 93 | 71 | 4104 | 18 |

Uploading straight to content-lake-api, bypassing Kafka (what a client-side claim check would do):

| upload | MB/s |
|---|---|
| 1 GB, one at a time | 276-458 |
| 1 GB, 2 in parallel | 322 |
| 100 MB, one at a time | 597 |
| 100 MB, 4 in parallel | 437-613 |

### CPU per GB of payload

Extra CPU-seconds per GB on the lake path, over the plain run at the same record size (idle background CPU cancels):

| record | api | broker (plugin) | cache worker | events Kafka | mongo | redis | total |
|---|---|---|---|---|---|---|---|
| 1 MB | 3.4 | 1.3 | 5.8 | 2.6 | 1.0 | 2.0 | 16.1 |
| 4 MB | 2.9 | 0.8 | 3.6 | 1.0 | 0.3 | 1.9 | 10.5 |
| 100 MB (skips redis) | 2.6 | 0.2 | 0.1 | 0.3 | 0.2 | 0 | 3.4 |

The plain broker itself used 1.6-1.7 CPU-s/GB at 1-4 MB records and 4.2 at 100 MB (replication factor 1).

## What it shows

- **Confirmed for a disk-bound broker and 1-100 MB records.** Capped at 50 MB/s, plain Kafka is pinned at the cap
  (56-60 MB/s). The lake path leaves that disk almost untouched: 290 MB/s at 1 MB, 384 MB/s at 4 MB, 119 MB/s at
  100 MB, 2-7x more. Broker disk writes drop from 3.2-4 GB to 0-150 MB; the same bytes land on the lake's storage.
- **Durability has to keep up, or the gain is only a burst.** Produce speed is bounded by Redis, durable speed by the
  cache workers. One worker persisted 99 MB/s at 1 MB, below the produce rate, so Redis held the backlog; four persisted
  217-296 MB/s, close to it. Size workers and topic partitions so persistence keeps up, or Redis fills and checkins fall
  back to synchronous durable writes.
- **Not for small records.** At 100 KB the lake path manages 56 MB/s (560 records/s) whether or not the disk is
  throttled, and a worker persists 12.5 MB/s. Per-record costs dominate: an HTTP call per record inside the broker,
  a Redis write, an idempotency check and an event per checkin, then a Mongo write, a file and an event per record in
  the worker.
- **A broker with spare disk bandwidth is faster plain** (580 vs 282 MB/s at 1 MB): local sequential writes aren't the
  bottleneck there, and the lake path adds a synchronous round trip per record, which also shows in latency.
- **1 GB records are a poor fit for Kafka either way.** One producer reaches about 18 MB/s on both paths: the client
  and broker heaps (whole records in memory, possibly several per request) are the limit, and on the lake path the
  content still passes through the broker's memory. Uploading the same 1 GB files straight to the lake runs at
  276-458 MB/s, 15-25x faster. For very large files, have producers check content in themselves and put only the claim
  check on Kafka.

## Projections at scale

`project.py` turns the measurements into monthly cost and capacity for a given ingest rate, record size, retention and
replication, using AWS us-east-1 on-demand list prices. Every price and assumption is a command-line flag:

```
python3 project.py                                         # the scenarios below
python3 project.py --ingest-mb-s 2000 --record-mb 4 --retention-days 14 --consumers 2 --cross-az-gb 0.02
```

It compares four setups:

- **plain Kafka**: content on every replica's disk for the whole retention.
- **Kafka + tiered storage** (KIP-405): broker disks keep 24 hours, S3 one copy of the rest. Writes still go to every
  replica's disk and cross AZs for replication.
- **content lake**: Kafka keeps claim checks; content passes through the broker once (producer in, out to the API),
  sits in Redis (one replica) for the persist lag plus the 2-minute TTL, and is kept in S3 for the retention.
- **content lake, AZ-local**: as above, with the API, its Redis shard and the worker in the same AZ as the broker that
  received the record, so only the producer hop, Redis replication and consumer reads cross AZs.

Brokers are sized by whichever needs the most: disk writes (250 MB/s per broker, gp3 provisioned), disk space (16 TB
per broker at 60% fill), or network (10 Gbps at 60%); each is a 10.5 vCPU / 48 GB pod as in the streamproc deployment.
Lake compute is the measured CPU per GB at 60% utilization; Redis is 1.3x the buffered bytes, replicated once.
Prices: gp3 $0.08/GB-month plus $0.04 per MB/s above 125; S3 $0.023/GB-month, $0.005 per 1,000 PUTs and $0.0004 per
1,000 GETs; cross-AZ $0.02/GB; $0.048 per vCPU-hour (m6i); $0.008 per GB-hour of RAM for Redis (r6i).

Monthly cost, 7 days retention, replication factor 3, one consumer group:

| ingest | record | plain Kafka | Kafka + tiered storage | content lake | content lake, AZ-local | brokers (plain / tiered / lake) |
|---|---|---|---|---|---|---|
| 100 MB/s | 1 MB | $49k | $24k | $24k | $17k | 19 / 3 / 3 |
| 100 MB/s | 100 MB | $49k | $24k | $10k | $10k | 19 / 3 / 3 |
| 1 GB/s | 1 MB | $491k | $234k | $226k | $155k | 189 / 27 / 3 |
| 1 GB/s | 4 MB | $491k | $234k | $214k | $144k | 189 / 27 / 3 |
| 1 GB/s | 100 MB | $491k | $234k | $86k | $86k | 189 / 27 / 3 |
| 5 GB/s | 4 MB | $2,455k | $1,171k | $1,067k | $717k | 945 / 135 / 7 |

The 1 GB/s, 1 MB case in detail:

| | plain Kafka | Kafka + tiered storage | content lake | content lake, AZ-local |
|---|---|---|---|---|
| brokers | 189 | 27 | 3 | 3 |
| broker compute | $72,848 | $10,407 | $1,156 | $1,156 |
| broker disk (gp3 GB) | $241,920 | $34,560 | $242 | $242 |
| broker disk throughput | $945 | $135 | $0 | $0 |
| s3 storage | - | $13,910 | $13,910 | $13,910 |
| s3 requests | - | $13 | $14,191 | $14,191 |
| lake compute (27 vCPU) | - | - | $961 | $961 |
| redis memory (338 GB) | - | - | $1,974 | $1,974 |
| mongo (headers, 3-node set) | - | - | $420 | $420 |
| cross-AZ transfer | $175,200 | $175,200 | $192,720 | $122,640 |
| **total / month** | **$490,913** | **$234,225** | **$225,576** | **$155,496** |

What drives the numbers:

- **Plain Kafka pays for retention three times on gp3** (3 PB of broker disk at 1 GB/s for 7 days), which also sets
  the broker count. Tiered storage fixes the storage cost but still needs brokers for three copies of every write.
- **Cross-AZ transfer is the largest cost for every setup that keeps traffic spread across AZs.** Kafka replication
  sends each byte to two other AZs. The lake replaces that with Redis replication and S3 (which replicates for free),
  but its default layout still moves content between AZs on the way to Redis and the worker. Keeping the API, Redis
  shard and worker AZ-local is the lake's largest saving; Kafka's equivalent levers are fetch-from-follower for
  consumers (KIP-392) and fewer replicas.
- **S3 PUTs scale with record count**: $14k/month at 1,000 records/s. Larger records cut it proportionally; batching
  several records per object would too (not implemented).
- **Lake compute and Redis are small** next to storage and transfer: 27 vCPU and 338 GB of Redis at 1 GB/s of 1 MB
  records. Records over the 10 MB cache limit skip Redis entirely.
- **Throughput per broker**: a disk-bound plain broker absorbs what its disk allows divided by the replication factor
  (about 83 MB/s of ingest per broker at 250 MB/s of disk). A lake broker is bounded by network, since content passes
  through it once (about 750 MB/s at 60% of 10 Gbps). That is why 3 lake brokers carry 1 GB/s where plain Kafka needs
  189 for the disk space and tiered storage 27.

These are projections from one machine's benchmark, not measurements at scale. Check prices against your contract;
check `--broker-write-mb-s`, `--cpu-util` and the CPU table against your hardware.

## Caveats

- Replication factor 1 in the benchmark. With 3, the plain path writes every byte to three broker disks and sends it
  over the replication network, so its disk bottleneck tightens; the projections use 3.
- Everything shares one machine's CPUs and one SSD: the scaled-worker runs produced slightly less because the workers
  competed with the broker and the API for CPU.
- The lake's durable storage is a local volume here, not S3; S3 throughput scales with parallelism.
- Redis peak memory equals the run's payload (about 4 GB) because persisted records keep a 2-minute TTL. Redis needs
  room for (produce rate - persist rate) x burst length plus `persisted_ttl_sec` of traffic.
- One API pod. The plugin calls the lake synchronously per record within each produce request, so throughput also
  scales with producer connections and API pods.
- The API buffers each request body in memory: size API pods for concurrent uploads x record size. Streaming bodies to
  storage would remove that (not implemented).

## Fixes the test led to

Large records exposed two bugs in the lake, fixed in content-lake-lib and content-lake-api, and one in the harness:

- A durable write longer than the 2 s attempt timeout was retried while the first write was still running, so the
  retry found a half-written file and failed with a permanent conflict. Content writes now get 120 s attempts, and a
  size mismatch on a write-once ref is retryable.
- Checkin hashed the content twice, synchronously; four 1 GB checkins blocked the event loop long enough for unrelated
  Mongo calls to time out. It now hashes once, on the thread pool (`crypto.subtle.digest`).
- `run.sh` aborted the matrix when a broker died; it now records the run as `broker OOM` and continues.

## Run it

```
./prepare.sh                     # fork jars from ~/.m2 (KAFKA_VERSION, default 3.7.2-MAX12) + the plugin
./run.sh                         # all scenarios, 100 KB / 1 MB / 4 MB
SIZES=1048576 SCENARIOS=lake_throttled CACHE_REPLICAS=4 ./run.sh
SIZES=100000000 TOTAL_MB=4000 PRODUCERS=4 PERF_HEAP=1500m BUFFER_MEMORY=500000000 BROKER_HEAP=4g BROKER_MEM=9g ./run.sh
SIZES=1000000000 TOTAL_MB=4000 PRODUCERS=1 PERF_HEAP=3g BUFFER_MEMORY=1200000000 BROKER_HEAP=5g BROKER_MEM=11g ./run.sh
python3 project.py --help
```

Settings: `SIZES`, `TOTAL_MB` (per run), `PRODUCERS`, `PARTITIONS`, `SCENARIOS`, `CACHE_REPLICAS`,
`BROKER_WRITE_BPS` (default `50mb`), `BROKER_DISK` (default `/dev/vda`, Docker Desktop's VM disk), `BROKER_HEAP`,
`BROKER_MEM`, `PERF_HEAP`, `BUFFER_MEMORY`. Results go to `results/<timestamp>/`: one JSON per run (throughput,
latency, disk, CPU) and the producers' output. Docker Desktop needs about 16 GB for the 100 MB and 1 GB runs.
