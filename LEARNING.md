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

## Milestone 2: candles in Flink, checked against the exchange

### Event time in Flink, and watermarks per partition

The core job (`flink/src/main/java/ticklab/flink/CoreJob.java`) reads the `trades` topic and
gives every trade its *event time*: the trade time, not the moment Flink sees it
(`CorePipeline.watermarks`). Windows such as "12:00 to 12:01" are then defined by what happened
when, so a replay at 10× speed produces exactly the candles of a live run.

The Kafka source generates watermarks *per partition*, that is per feed (decision note 0002),
and the job's watermark is the minimum over all of them. A feed that goes quiet would hold that
minimum back and freeze every window, so a partition with nothing for 10 s is marked *idle* and
left out until it speaks again.

### Windows, allowed lateness and side outputs

Candles are *tumbling event-time windows* of one minute, keyed by feed
(`CorePipeline.build`). A window fires when the watermark passes its end, about 2 s after the
minute (the bound B). The `CandleAggregate` keeps a small running summary instead of every trade,
and takes open and close from the lowest and highest trade ID, so arrival order never changes
the result (`CandleAggregateTest`).

A trade can arrive after its window fired, for example one fetched by gap repair. *Allowed
lateness* (L = 5 minutes) keeps each window's state that much longer: a late trade updates the
candle and emits it again, with a higher `trade_count`, which ClickHouse uses as the version to
keep. Anything later still goes to a *side output*, the `late-trades` topic, so nothing is
dropped silently.

### Keyed state: deduplication in one number per feed

`Dedup.java` keeps, per feed, the highest ID up to which every trade has been seen, plus a
small map of IDs that arrived ahead of a missing one. That is enough to recognise every repeat,
and it shows where the gaps are. The state is *keyed*: Flink keeps one copy per feed and hands
each trade only its own feed's state. A gap that stays open past the lateness budget is given up
on, so the map can't grow without bound. `DedupTest` drives the operator through test harnesses
that control input, watermarks and timers exactly.

### Checkpoints, savepoints and draining

Every 10 s Flink takes a *checkpoint*: a consistent snapshot of all state (deduplication, open
windows) together with the Kafka offsets it corresponds to. After a crash it restarts from the
last checkpoint and replays Kafka from those offsets, so state and input stay in step.

A *savepoint* is the same snapshot taken on request. `make down` stops the job with one, and
`make up` resumes from it (`ticklab/flinkctl.py`), so a stop loses nothing: windows that were
open simply continue.

*Draining* (`flink stop --drain`, `make replay-drain`) is different: it moves event time to the
end of time first, so every open window and timer fires. A replay of a bounded input ends this
way, or its last minutes would never close.

### Idleness, and why replays switch it off

Idleness is measured on the machine's clock, which makes it a trap when reading a backlog. In
the first run of the milestone 2 checks, a whole day of history sat in Kafka at once. The Kafka
consumer handed the job BTC and SOL records for more than 10 s before any ETH records, so the
ETH partition was taken for idle and the watermark ran ahead without it. When ETH's trades
finally came, their minutes were long past, and they went to `late-trades`. And when every
partition had gone idle, draining didn't fire the last window.

So idleness is for live data only (`--idleness-seconds`, decision note 0015). A replay lets the
watermark wait for every feed: slower when one feed is quiet, but never wrong.

### At-least-once output into idempotent storage

After a restart from a checkpoint, Flink emits again whatever it had emitted since that
checkpoint. Exactly-once delivery to Kafka would need transactions, which add a checkpoint
interval of latency to every result. Here the sinks are *at-least-once* (`Sinks.java`), and the
storage makes repeats harmless: each candle has a deterministic key, and ClickHouse keeps one
version per key, the one with the most trades (decision note 0003).

### Reconciliation: proving the numbers

The reconciler (`ticklab/reconciler.py`) fetches Binance's official 1-minute candles and
compares every field exactly: open, high, low, close, volume, quote volume, trade count and the
taker-buy volumes. It waits until a minute has *settled*, 6 minutes after it ends (B + L + a
margin), and until the job has emitted a candle at least L newer, so neither the clock nor a
backlog makes it compare too early. Each mismatch gets a likely cause: the producer was down,
trades came too late, IDs are missing, or unknown. The goal is not "no mismatches" but "no
*unexplained* mismatch".

## Milestone 3: gaps, silence, failures and chaos

### Event-time timers versus processing-time timers

A Flink timer fires at a time, but which clock? An *event-time* timer fires when the watermark
passes it, so it follows the data: replaying the same trades fires it at the same point every
time. A *processing-time* timer fires on the machine's clock.

Gap detection uses event time (`Dedup.java`): it waits until the watermark passes the first
trade after a missing ID, so a trade that is merely reordered never looks like a gap. Silence
detection has to use processing time (`FeedHealthMonitor.java`): when trades stop, event time
stops too, and an event-time timer would never fire. The price is that silence events depend
on the clock, so replays aren't expected to reproduce them.

### Gaps from sequence numbers

Within a feed, trade IDs go up by one. The deduplication state already knows the highest ID up
to which everything has arrived, and which IDs came early, so a gap is simply the missing run
between them. It is reported DETECTED once the watermark passes the trade after it; gap repair
(`ticklab/gap_repair.py`) fetches those IDs from the exchange's REST API and publishes them;
the gap is CLOSED when the last of them has arrived. If repair takes longer than the lateness
budget, the gap EXPIRES: its trades are counted as skipped, and any that still arrive go to
`late-trades`.

### Late data as a feature

Repaired trades arrive minutes after their candle was first emitted. Because the candle window
keeps its state for the lateness budget L, each repaired trade updates the candle and emits it
again, complete, and storage keeps the newest version. Nothing about a gap needs special
handling downstream: late data is just data.

### Recovering from a checkpoint

`make fail-taskmanager` kills the TaskManager, where all the job's state lives. Flink notices,
waits for it to come back (with exponential backoff between attempts), restores the last
checkpoint, and rewinds the Kafka source to that checkpoint's offsets. Everything after the
checkpoint is processed again; candles emitted twice are the same rows, so storage doesn't
change.

### Chaos, deterministically

`ticklab/chaos.py` turns a clean fixture into a messy feed: shuffled within the watermark bound,
some trades delayed past it but within the lateness budget, repeats, and runs of IDs that never
arrive. A fixed seed makes the mess the same every run, so a failure can be reproduced. Because
everything injected stays within what the pipeline promises to handle, the expected result is
still exact: every candle equal to the exchange's (`ticklab/checks/e2e.py`).

The fake exchange (`ticklab/fake_exchange.py`) plays the fixture's trades and candles as
Binance's REST API, so gap repair and the checks run unchanged, and CI never touches a real
exchange.
