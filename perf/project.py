"""Projects monthly cost and capacity of plain Kafka vs the content lake at a given ingest rate.

    python3 project.py                                   # the README's scenarios
    python3 project.py --ingest-mb-s 2000 --record-mb 4 --retention-days 14 --consumers 2

CPU per GB comes from the measured runs (see README). Prices are AWS us-east-1 on-demand list prices as parameters;
check them against your own rates and discounts. Every figure is a projection from a single-machine benchmark.
"""
import argparse
import math

HOURS = 730
SECONDS = HOURS * 3600

# Measured extra CPU-seconds per GB of payload on the lake path, over the plain run at the same record size
# (lake minus plain per component, so idle background CPU cancels out). One run each.
# Cached path (records up to the cache limit): at 1 MB and at 4 MB records.
MEASURED_LAKE_CPU = {
    'api': (3.4, 2.9),
    'broker_plugin': (1.3, 0.8),
    'cache_worker': (5.8, 3.6),
    'events_kafka': (2.6, 1.0),
    'mongo': (1.0, 0.3),
    'redis': (2.0, 1.9),
}
# Direct-to-durable path (records over the cache limit skip redis and the worker copy): at 100 MB records.
MEASURED_LAKE_CPU_UNCACHED = {'api': 2.6, 'broker_plugin': 0.2, 'events_kafka': 0.3, 'mongo': 0.2}


def records_per_gb(record_mb):
    return 1000 / record_mb


def fit(cpu_1mb, cpu_4mb):
    n1, n4 = records_per_gb(1.048576), records_per_gb(4.194304)
    per_record = (cpu_1mb - cpu_4mb) / (n1 - n4)
    per_gb = cpu_1mb - per_record * n1
    return max(per_gb, 0.0), max(per_record, 0.0)


def lake_cpu_per_gb(record_mb, cache_limit_mb):
    if record_mb > cache_limit_mb:
        return dict(MEASURED_LAKE_CPU_UNCACHED)
    total = {}
    for name, points in MEASURED_LAKE_CPU.items():
        per_gb, per_record = fit(*points)
        total[name] = per_gb + per_record * records_per_gb(record_mb)
    return total


def project(a):
    gb_s = a.ingest_mb_s / 1000
    gb_month = gb_s * SECONDS
    records_s = a.ingest_mb_s / a.record_mb
    cross_az = 2 / 3  # share of traffic between random endpoints that crosses AZs, with 3 AZs
    vcpu_month = a.vcpu_hour * HOURS
    plain, tiered, lake = {}, {}, {}

    broker_month = a.broker_vcpu * vcpu_month + a.broker_extra_ram_gb * a.extra_ram_gb_hour * HOURS
    write_mb_s = a.ingest_mb_s * a.replication
    kafka_cross = gb_month * (cross_az + (a.replication - 1) + a.consumers * cross_az) * a.cross_az_gb

    # Brokers are sized by disk writes, disk space and network, whichever needs the most.
    network_mb_s = a.broker_network_mb_s * a.network_util

    def brokers_for(stored_gb, write, network_in, network_out):
        return max(3, math.ceil(write / a.broker_write_mb_s), math.ceil(stored_gb / (a.max_broker_disk_tb * 1000)),
                   math.ceil(max(network_in, network_out) / network_mb_s))

    # Kafka with content: producers plus replication in; replication plus consumer fetches out.
    kafka_in = a.ingest_mb_s * a.replication
    kafka_out = a.ingest_mb_s * (a.replication - 1 + a.consumers)

    # Plain Kafka: content in the log, on every replica, for the whole retention.
    stored_gb = gb_s * 86400 * a.retention_days * a.replication / a.disk_fill
    brokers = brokers_for(stored_gb, write_mb_s, kafka_in, kafka_out)
    plain['brokers'] = brokers
    plain['broker compute'] = brokers * broker_month
    plain['broker disk (gp3 GB)'] = stored_gb * a.ebs_gb_month
    plain['broker disk throughput'] = brokers * max(0, a.broker_write_mb_s - 125) * a.ebs_mb_s_month
    plain['cross-AZ transfer'] = kafka_cross

    # Plain Kafka with tiered storage (KIP-405): local disks keep local_retention_hours, S3 keeps one copy of the rest.
    # Writes still hit every replica's disk and cross AZs for replication.
    local_gb = gb_s * 3600 * min(a.local_retention_hours, a.retention_days * 24) * a.replication / a.disk_fill
    tiered_brokers = brokers_for(local_gb, write_mb_s, kafka_in, kafka_out)
    tiered['brokers'] = tiered_brokers
    tiered['broker compute'] = tiered_brokers * broker_month
    tiered['broker disk (gp3 GB)'] = local_gb * a.ebs_gb_month
    tiered['broker disk throughput'] = tiered_brokers * max(0, a.broker_write_mb_s - 125) * a.ebs_mb_s_month
    tiered['cross-AZ transfer'] = kafka_cross
    tiered['s3 storage'] = gb_s * 86400 * a.retention_days * a.s3_gb_month
    tiered['s3 requests'] = gb_s * SECONDS / a.segment_gb / 1000 * a.s3_put_k

    # Lake: Kafka keeps claim checks; content goes broker -> api -> redis -> worker -> S3, and consumers check it out.
    claim_ratio = a.claim_kb / (a.record_mb * 1000)
    # The content still passes through the broker once: in from the producer, out to the api.
    lake['brokers'] = brokers_for(stored_gb * claim_ratio, write_mb_s * claim_ratio, a.ingest_mb_s, a.ingest_mb_s)
    lake['broker compute'] = lake['brokers'] * broker_month
    lake['broker disk (gp3 GB)'] = stored_gb * claim_ratio * a.ebs_gb_month
    lake['broker disk throughput'] = 0.0
    cpu = lake_cpu_per_gb(a.record_mb, a.cache_limit_mb)
    lake_vcpu = sum(cpu.values()) * gb_s / a.cpu_util
    lake['lake compute'] = lake_vcpu * vcpu_month
    cached = a.record_mb <= a.cache_limit_mb
    redis_gb = gb_s * (a.persist_lag_sec + a.persisted_ttl_sec) * a.redis_overhead * (1 + a.redis_replicas) if cached else 0
    lake['redis memory'] = redis_gb * a.ram_gb_hour * HOURS
    lake['s3 storage'] = gb_s * 86400 * a.retention_days * a.s3_gb_month
    lake['s3 requests'] = records_s * SECONDS / 1000 * (a.s3_put_k + a.consumers * a.s3_get_k)
    lake['mongo (headers, 3-node set)'] = 3 * a.mongo_vcpu * vcpu_month

    def lake_cross_az(az_local):
        hops = cross_az  # producer -> broker
        if cached:
            # api -> redis primary and worker -> redis stay in the broker's AZ when az_local; replication always crosses.
            hops += (0 if az_local else 2 * cross_az) + a.redis_replicas
        return gb_month * (hops + a.consumers * cross_az) * a.cross_az_gb  # consumers check out through the api

    lake['cross-AZ transfer'] = lake_cross_az(False)
    local = dict(lake)
    local['cross-AZ transfer'] = lake_cross_az(True)

    return plain, tiered, lake, local, {'lake vCPU': lake_vcpu, 'redis GB': redis_gb, 'plain stored TB': stored_gb / 1000,
                         'records/s': records_s, 'lake cpu-s/GB': sum(cpu.values())}


def report(a):
    plain, tiered, lake, local, extra = project(a)
    columns = [plain, tiered, lake, local]
    print(f"\n### {a.ingest_mb_s:g} MB/s of {a.record_mb:g} MB records, {a.retention_days:g} days retention, "
          f"RF {a.replication}, {a.consumers} consumer group(s)\n")
    print('| | plain Kafka | Kafka + tiered storage | content lake | content lake, AZ-local |')
    print('|---|---|---|---|---|')
    print('| brokers | ' + ' | '.join(str(column['brokers']) for column in columns) + ' |')
    keys = [key for key in dict.fromkeys(list(plain) + list(tiered) + list(lake)) if key != 'brokers']
    money = lambda value: '-' if value is None else f'${round(value):,}'
    for key in sorted(keys, key=lambda key: key == 'cross-AZ transfer'):
        print(f'| {key} | ' + ' | '.join(money(column.get(key)) for column in columns) + ' |')
    totals = [sum(value for key, value in column.items() if key != 'brokers') for column in columns]
    print('| **total / month** | ' + ' | '.join(f'**{money(total)}**' for total in totals) + ' |')
    print(f"\nLake sizing: {extra['lake vCPU']:.0f} vCPU of api/worker/redis/mongo/plugin work "
          f"({extra['lake cpu-s/GB']:.1f} CPU-s/GB), {extra['redis GB']:.0f} GB redis; "
          f"plain keeps {extra['plain stored TB']:.0f} TB on broker disks; {extra['records/s']:.0f} records/s.")


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add = p.add_argument
    add('--ingest-mb-s', type=float, default=None, help='average content ingest, MB/s')
    add('--record-mb', type=float, default=None, help='average record size, MB')
    add('--retention-days', type=float, default=7)
    add('--replication', type=int, default=3)
    add('--consumers', type=int, default=1, help='consumer groups reading the content')
    add('--broker-write-mb-s', type=float, default=250, help='sustained disk writes one broker is sized for (gp3 provisioned)')
    add('--broker-vcpu', type=float, default=10.5, help='broker pod size, as in the streamproc deployment')
    add('--broker-extra-ram-gb', type=float, default=6, help='RAM beyond 4 GB per vCPU (48 GB pod - 42)')
    add('--broker-network-mb-s', type=float, default=1250, help='broker NIC, MB/s per direction (10 Gbps)')
    add('--network-util', type=float, default=0.6, help='target NIC utilization')
    add('--disk-fill', type=float, default=0.6, help='target broker disk utilization')
    add('--max-broker-disk-tb', type=float, default=16, help='largest broker volume (gp3 max is 16 TiB)')
    add('--local-retention-hours', type=float, default=24, help='tiered storage: hours kept on broker disks')
    add('--segment-gb', type=float, default=1, help='tiered storage: segment size uploaded per S3 PUT')
    add('--claim-kb', type=float, default=1, help='claim check record size, KB')
    add('--cache-limit-mb', type=float, default=10, help='records above this skip redis (tiers.cache.max_content_bytes)')
    add('--persist-lag-sec', type=float, default=10)
    add('--persisted-ttl-sec', type=float, default=120)
    add('--redis-replicas', type=int, default=1)
    add('--redis-overhead', type=float, default=1.3)
    add('--cpu-util', type=float, default=0.6, help='target CPU utilization for lake pods')
    add('--mongo-vcpu', type=float, default=4)
    add('--vcpu-hour', type=float, default=0.048, help='general purpose vCPU-hour incl. 4 GB (m6i)')
    add('--extra-ram-gb-hour', type=float, default=0.004, help='RAM beyond 4 GB/vCPU (r6i vs m6i)')
    add('--ram-gb-hour', type=float, default=0.008, help='memory-optimized RAM GB-hour for redis (r6i)')
    add('--ebs-gb-month', type=float, default=0.08, help='gp3 storage')
    add('--ebs-mb-s-month', type=float, default=0.04, help='gp3 throughput above 125 MB/s')
    add('--s3-gb-month', type=float, default=0.023)
    add('--s3-put-k', type=float, default=0.005, help='per 1,000 PUTs')
    add('--s3-get-k', type=float, default=0.0004, help='per 1,000 GETs')
    add('--cross-az-gb', type=float, default=0.02, help='inter-AZ transfer, both directions')
    return p


if __name__ == '__main__':
    args = parser().parse_args()
    if args.ingest_mb_s is None:
        for ingest, record in [(100, 1), (1000, 1), (1000, 4), (1000, 100), (5000, 4)]:
            args.ingest_mb_s, args.record_mb = ingest, record
            report(args)
    else:
        args.record_mb = args.record_mb or 1
        report(args)
