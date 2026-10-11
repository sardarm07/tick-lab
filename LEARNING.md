# Learning notes

Each streaming concept, in plain words, as it shows up in this project, with a pointer to the
code. Entries are added as the pieces are built.

## Milestone 1: getting trades in, exactly once in storage

### Raw data, normalised data, and why both are kept

An exchange sends *raw* messages in its own format: Binance writes `"m": true` for "the buyer
was the maker", Kraken writes `"side": "sell"`. The *normaliser* turns each one into the same
`Trade` record (`ticklab/model.py`), so everything downstream sees one format.

The producer writes every raw frame to disk *before* parsing it (`ticklab/recorder.py`). If the
normaliser has a bug, the bug can be fixed and the recording replayed through the corrected
code (`ticklab/replay.py`), which reproduces exactly what should have been stored. Recording
after parsing would have kept only the buggy output. The exchanges' history files are handled
the same way: kept unmodified, and normalised when replayed.

A replay is only trustworthy if it is exact, so the M1 check replays 15 recorded minutes into
an empty stack and requires the very same set of trades as the live run.

### Topics, partitions and keys

Kafka (here Redpanda, which speaks the same protocol) stores messages in *topics*. A topic is
split into *partitions*; each partition is an ordered log, and order is only guaranteed within
one partition.

Every feed (one market on one exchange) gets its own partition of the `trades` topic, listed in
`config/feeds.toml`. The partition is chosen explicitly (`ticklab/kafka.py`), not by hashing the
message key, because a hash can put two feeds into the same partition. One feed per partition
means one ordered log per feed, and later one watermark per feed in Flink.

The message key, `exchange:symbol`, still matters: it travels with each message and tells any
reader which feed it belongs to.

### The schema registry, Avro, and exact decimals

Messages are encoded with Avro (`schemas/trade.avsc`): a compact binary format with a schema.
The schema is stored once in the *schema registry*, and every message starts with a 5-byte
header: a zero byte, then the schema's 4-byte ID (`ticklab/avro.py`). Readers such as ClickHouse
fetch the schema by that ID and decode, so producers and consumers agree on the format without
sharing code.

Prices and quantities use Avro's `decimal` type: an integer plus a fixed scale (12 digits after
the point). A float can't hold 0.1 exactly, and a candle's volume is a sum of thousands of
quantities, so float errors would make "our candle equals the exchange's candle" impossible to
prove. Exact decimals are kept end to end: `Decimal` in Python, `Decimal(28, 12)` in ClickHouse.

### Producer idempotence versus end-to-end idempotence

The producer is *idempotent* (`enable.idempotence` in `ticklab/kafka.py`): if a send times out
and is retried, the broker recognises the repeat and stores the message once. That only covers
one producer's retries. The same trade can still arrive twice for other reasons: two
overlapping WebSocket connections, a replay, a gap being repaired.

*End-to-end* idempotence means the final result is right however many times a trade arrives.
Here it comes from the storage: each trade has a deterministic key, and a second copy replaces
the first instead of adding to it (next entry). From milestone 2, Flink also removes duplicates
before computing candles.

### ReplacingMergeTree and FINAL

ClickHouse's `trades` table (`infra/clickhouse/sql/001-trades.sql`) is a `ReplacingMergeTree`
ordered by `(exchange, symbol, trade_id)`. ClickHouse writes data in *parts* and merges them in
the background; when it merges, rows with the same key collapse into one.

Merges happen whenever ClickHouse gets to them, so at any moment duplicates may still be on
disk. A query with `FINAL` collapses them while reading, which makes `count()` exact. That is
why every check and dashboard query uses `FINAL`.

Collapsing only happens within a partition of the table. The table is partitioned by the trade's
own date, so every copy of a trade lands in the same partition, wherever it came from.

### Staying connected: backoff, jitter and make-before-break

A WebSocket connection fails sooner or later, so the client in `ticklab/stream.py` plans for it:

- **Backoff with jitter.** After an error it waits before reconnecting, doubling the wait each
  time (1 s, 2 s, 4 s … up to 60 s), so a broken server isn't hammered. Half of each wait is
  random, so thousands of clients cut off at once don't all return at the same instant.
- **Make before break.** Binance closes every connection after 24 hours, and sometimes
  announces a restart with a `serverShutdown` event. In both cases the client opens the new
  connection first, waits for its first trade plus 2 seconds, and only then closes the old one.
  The two overlap, so no trade falls between them; the overlap's duplicates are harmless,
  because storage is idempotent.
- **Liveness.** The library pings every 20 s and answers the server's pings. A connection that
  delivers nothing for 60 s is replaced.

### Counters across restarts

Prometheus counters live in the process's memory and start from zero when it restarts.
Prometheus treats a counter that drops as a reset, which `rate()` and `increase()` handle. But a
restart can also be invisible: a crashed producer that comes back and opens its first
connection shows `connects = 1`, exactly like the process before it. Restarts are therefore
counted from `process_start_time_seconds` (`changes()` over a window), as the "Reconnects and
restarts" panel does (`infra/grafana/dashboards/feed-health.json`).

### Event time, arrival time, and disorder

Every trade has two times. *Event time* is when the exchange matched it (`trade_time`).
*Arrival time* is when it reached the producer (`received_time`). Results such as "the
12:00 candle" must be computed in event time, or they would change with network delays.

A stream processor can't wait forever for stragglers, so it keeps a *watermark*: a promise
that no more trades older than a certain time will come. Here it will be "the newest trade
time seen in this feed, minus a bound B". A trade that arrives after the watermark has passed
its time is *late*.

The data study (`ticklab/study.py`, results in `docs/data-study.md`) measured how late trades
arrive in practice. On one connection, Binance delivers each pair strictly in order: in 65,360
trades, none was late. Arrival was *delayed* (about 125 ms, once 5 s), but a delay that keeps
order doesn't matter: the watermark only advances when newer trades arrive. B stays at 2 s,
a margin for handovers between connections and for replays that reorder on purpose.

### Crashes versus stops

The M1 check kills the producer with `docker kill`, and Docker does *not* restart it, despite
`restart: unless-stopped`: Docker treats a kill sent through its API as a deliberate stop.
Restart policies apply when a process exits on its own, for example after a crash or an
out-of-memory kill. `make up` (Compose) brings a stopped service back. The difference matters
when testing recovery: a test should say which of the two it simulates.
