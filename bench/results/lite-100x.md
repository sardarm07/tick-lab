# lite, 100x

The busiest hour of the history (2026-10-08T15:00:00+00:00), both exchanges: 1,362,042 trades.

| Measure | Value |
|---|---|
| Replay time (due) | 36.0 s (36.0 s) |
| Trades published per second | 37,826 |
| Kept to schedule | yes |
| Largest consumer lag | ticklab_core 664,222, ticklab_signals 429,723 |
| Pipeline latency of trades, p50 / p95 / p99 / max (ms) | 15,355 / 21,095 / 22,257 / 22,384 |
| Output latency of candles, p50 / p95 / p99 / max (ms) | 28,714 / 33,685 / 38,000 / 38,067 |
| Longest checkpoint (ms), largest (KB) | ticklab_core 288 / 71, ticklab_signals 1,507 / 1,200 |
| Peak memory (MiB) | clickhouse 503, flink-jobmanager 478, flink-taskmanager 637, gap-repair 32, grafana 210, prometheus 75, redpanda 170 |
