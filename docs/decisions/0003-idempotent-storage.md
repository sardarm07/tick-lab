# 0003: Idempotent storage instead of transactions

**Status:** accepted, milestone 1 (for trades; the same pattern applies to every later table)

## Context

A trade can reach the pipeline more than once: producer retries, overlapping WebSocket
connections, replays, and gaps repaired from REST. There is no transactional path from Kafka
or Flink into ClickHouse, and a two-phase commit would add a checkpoint interval of latency to
every result.

## Decision

- Every ClickHouse table is a `ReplacingMergeTree` with a deterministic key. For `trades`, the
  key is `(exchange, symbol, trade_id)`.
- Every query that counts or aggregates uses `FINAL`, which collapses rows with the same key
  at read time, before background merges have done it.
- The table is partitioned by the trade's own date (`toYYYYMMDD(trade_time)`). Collapsing only
  happens within a partition, and this way every copy of a trade lands in the same one.
- `trades` keeps rows for 7 days after insertion, not after trade time, so replayed history
  isn't deleted as it arrives.

## Consequences

- At-least-once delivery anywhere upstream is enough for exact results.
- `FINAL` costs some query time; at this data size it doesn't matter.
- For `trades`, any copy of a duplicate can survive, because the trade's own fields are
  identical in every copy; only metadata such as `received_time` differs. Tables whose rows
  change over time (candles, from milestone 2) need a version column.
