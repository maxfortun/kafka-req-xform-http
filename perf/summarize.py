import json, sys
from pathlib import Path

rows = [json.loads(path.read_text()) for path in sorted(Path(sys.argv[1]).glob('*.json'))]
order = {'plain': 0, 'lake': 1, 'plain_throttled': 2, 'lake_throttled': 3}
rows.sort(key=lambda row: (row['record_bytes'], order.get(row['scenario'], 9)))
print('| record | scenario | workers | produce MB/s | avg / p99 ms | broker disk MB | lake disk MB (cache+mongo) | persisted MB/s | redis peak MB |')
print('|---|---|---|---|---|---|---|---|---|')
for row in rows:
    written = row['written_mb']
    lake_disk = written.get('cache', 0) + written.get('mongo', 0) + written.get('api', 0)
    size = f"{row['record_bytes'] // 1024} KB" if row['record_bytes'] < 1 << 20 else f"{row['record_bytes'] >> 20} MB"
    print(f"| {size} | {row['scenario']} | {row.get('cache_replicas', 1)} | {row['produce_mb_per_sec']} | {row['avg_latency_ms']:.0f} / {row['max_p99_latency_ms']:.0f} | "
          f"{written.get('broker', 0)} | {lake_disk if row['scenario'].startswith('lake') else '-'} | "
          f"{row['persisted_mb_per_sec'] or '-'} | {row['redis_peak_mb'] or '-'} |")
