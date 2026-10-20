import json, sys
from pathlib import Path

def label(size):
    for unit, factor in (('GB', 1e9), ('MB', 1e6), ('KB', 1e3)):
        if size % factor == 0:
            return f'{size // int(factor)} {unit}'
    for unit, factor in (('MB', 1 << 20), ('KB', 1 << 10)):
        if size >= factor:
            return f'{size // factor} {unit}'
    return f'{size} B'


def ms(value):
    return '-' if value is None else f'{value:.0f}'


rows = [json.loads(path.read_text()) for path in sorted(Path(sys.argv[1]).glob('*.json'))]
order = {'plain': 0, 'lake': 1, 'plain_throttled': 2, 'lake_throttled': 3}
rows.sort(key=lambda row: (row['record_bytes'], order.get(row['scenario'], 9)))
print('| record | scenario | workers | produce MB/s | avg / p99 ms | broker disk MB | lake disk MB (cache+mongo) | persisted MB/s | redis peak MB |')
print('|---|---|---|---|---|---|---|---|---|')
for row in rows:
    written = row['written_mb']
    lake_disk = written.get('cache', 0) + written.get('mongo', 0) + written.get('api', 0)
    size = label(row['record_bytes'])
    print(f"| {size} | {row['scenario']} | {row.get('cache_replicas', 1)} | {row['produce_mb_per_sec'] if row['producers_reporting'] else ('broker OOM' if row.get('broker_oom_killed') else 'failed')} | {ms(row['avg_latency_ms'])} / {ms(row['max_p99_latency_ms'])} | "
          f"{written.get('broker', 0)} | {lake_disk if row['scenario'].startswith('lake') else '-'} | "
          f"{row['persisted_mb_per_sec'] or '-'} | {row['redis_peak_mb'] or '-'} |")
