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

## Milestone 4: a second exchange, and signals across both

### Knowing what a timestamp means

Kraken keeps each trade's time in nanoseconds. Its WebSocket truncates that to microseconds;
its REST API sends it as a binary floating-point number of seconds, which can't hold every
nanosecond time and lands up to about 0.12 µs either side of it. So the same trade can carry two
different microsecond times, depending on where it came from: 7 of 50 did in one check. None of
the results depend on it (candles order trades by ID), but it was only found by asking Kraken
for a trade's exact time and comparing (decision note 0016). An exchange's documentation says
what a field is called, not always what it means.

### Paging by time instead of by ID

Binance can return trades from an ID, so repairing a gap is a lookup. Kraken pages by time:
"trades since t". Gap repair therefore turns the gap into a time, from the trade just before it
that is already in ClickHouse, and pages forward until it is past the gap. `since` is
inclusive, so consecutive pages share a trade; keeping track of the IDs already seen makes
the overlap harmless (`kraken_rest.pages_between_ids`).

### Flink SQL on a stream

The signals job hands deduplicated trades to Flink SQL as a table whose event time and
watermarks come from the stream (`SignalsPipeline.java`). A tumbling window then reads like
ordinary SQL: `TUMBLE(TABLE trades, DESCRIPTOR(rowtime), INTERVAL '1' SECOND)`, grouped by feed.
Each window's row comes out once, when the watermark passes its end. A trade later than that is
dropped from the window: these bars are a live signal, while the candles accept late trades.

SQL's `LAST_VALUE` means the last row to arrive, not the last trade. The last trade is the one
with the highest ID, so the job defines its own aggregate, `LAST_BY_ID(price, trade_id)`
(`LastPriceById.java`).

The window aggregation runs differently from the DataStream windows: it buffers rows in Flink's
*managed memory*, off the Java heap. The core job never used any, so the cluster had none
configured, and the signals job's first window failed until it got 64 MB (decision note 0010).

### Versioned tables and temporal joins

A price is only meaningful at a moment. To convert a Kraken BTC/USD price to USDT, the job needs
the USDT/USD rate *as it was when that Kraken price was set*, not the latest rate. Flink SQL says
this directly:

```sql
JOIN usd_per_usdt FOR SYSTEM_TIME AS OF k.bar_time AS r ON k.`quote` = r.`quote`
```

`usd_per_usdt` is a *versioned table*: every past rate, each valid from its own time until the
next. Deduplicating a stream on the newest time per key is how Flink SQL declares one. An
*event-time temporal join* waits until the watermark passes each Kraken row, then joins it with
the version of the rate valid at that row's time. The result doesn't depend on when rows arrive,
only on their times. A second temporal join gives each Binance second the Kraken price as of
that second.

One rule took a failed test to find: a row may carry only one event-time attribute. A temporal
join keeps the time columns of both sides as event time, so the joined rows had two, and Flink
could no longer tell which one versioned the next table. Casting the version's time to a plain
`TIMESTAMP` leaves the Kraken bar's time as the only event time (`SignalsPipeline.java`).

### From SQL back to DataStream

Some work is easier outside SQL: exact decimal arithmetic with chosen rounding, and alerts with
state machines and timers. `toDataStream` turns the joined table back into a stream of rows,
keeping each row's event time and the watermarks, so the DataStream operators after it still run
in event time.

### Alerts as state machines, in event-time order

An alert is a small state machine per key: closed, then open once a condition has held for a
while, then closed again (`DivergenceAlerts.java`). Two thresholds instead of one, a higher one
to open and a lower one to close, keep a value that hovers around a threshold from opening and
closing the alert over and over: that is *hysteresis*. "Held for 30 seconds" is an event-time
timer, set when the condition starts and checked when it fires.

Timers fire as the watermark passes them, but elements arrive whenever they arrive. In a fast
replay the watermark can jump a minute at once, and an element could then be handled before a
timer that is earlier in event time, so two replays could produce different alerts. The alert
operators avoid this by holding each element until the watermark passes it and handling it in
the same timer queue (`InEventTimeOrder.java`): elements and timers in strict event-time order,
whatever the watermark does. A harness test feeds the same spreads once with a watermark after
each and once with a single watermark at the end, and requires identical alerts.

### Draining a job that is behind

The first two replays of a whole day gave different spreads. BTC's stopped at 15:21 in one and
at 14:54 in the other, ETH's at other times, and SOL's never. Nothing was wrong with the
spreads: the signals job does more work per trade than the core job, so after a fast replay it
was minutes behind its input. The check waited only for the core job's candles, then drained
both jobs. Draining stops each source where it has read to, and only then sends the final
watermark. The signals job had read SOL's lighter partition to the end of the day but not
BTC's or ETH's, so their spreads simply ended there, at a point that depended on timing.

`flink drain` now first waits until each job's Kafka source has nothing left to read (its
`pendingRecords` metric, `ticklab/flinkctl.py`). After that, two replays of the day agree, and
every one of their 152,691 spreads equals the plain-Python recomputation from the same history.

## Milestone 5: how fast, and at what cost

### Three latencies, three clocks

"How long does a trade take?" has no single answer, so the project measures three, each from
timestamps that really exist (`ticklab/bench.py`, the "Pipeline" dashboard):

- **Feed latency:** received by our producer minus the exchange's trade time. It mixes two
  clocks, the exchange's and ours, so it includes their offset and can even be negative
  (Kraken's median was 0.3 s in one session and −23 ms in another, on a different virtual
  machine; a negative delay can only come from the clocks).
- **Pipeline latency:** stored in ClickHouse minus published to Kafka, both on our clock. It is
  dominated by ClickHouse reading Kafka in blocks, every few seconds, not by any processing.
- **Output latency:** a candle emitted by Flink minus the publishing of the newest trade in it,
  our clock again. A candle can only come out once the watermark passes the end of its
  minute, and the watermark waits for the quietest feed, so this measures waiting more than
  processing (below).

One trap: a Kafka record's timestamp is "publish time" only if the producer sets it so. Flink's
Kafka sink stamps each record with its event time instead, so a candle's Kafka timestamp is the
end of its minute, not when it was sent.

### Throughput, lag and backpressure

A replay sends trades on a schedule: at 10x, the hour of trades in 6 minutes. If the pipeline
keeps up, the Flink sources' *consumer lag* (trades published but not yet read, their
`pendingRecords`) stays near zero. If it doesn't, lag grows, and a busy operator makes the ones
before it wait for buffer space: *backpressure*, visible in Flink as busy time near 1,000 ms
per second on one task and "backpressured" time on those upstream.

Benchmarking honestly means the load generator mustn't be the bottleneck. Turning a CSV row
into an Avro message in Python takes longer than Kafka needs to store it, so the benchmark
encodes the whole hour first and starts the clock only for sending.

At 100x the busiest hour is 37,800 trades a second. Kafka accepted every one on time on both
machines, so the sending rate says nothing about whether the pipeline keeps up; lag does. On 2
cores, shared by Redpanda, ClickHouse, both Flink jobs and the replay itself, the jobs fell up
to 664,000 trades behind. On 4 cores, with parallelism 2, never more than 19,000.

Candles came out sooner at 10x than at 1x: 0.6 s after their newest trade in the median,
against 4.6 s. The watermark is the minimum over all seven feeds, so a minute's candles wait
until the quietest feed trades past the minute's end. That wait is measured in trade time, and
a 10x replay runs through trade time ten times faster. Live, idleness ends the wait after 10 s
of silence, which is why live candles come out about 10 s after their minute.

### Changing a job that has state

A savepoint files state by *operator ID*, and a restarted job gets each operator's state back
by the same ID. An operator given a fixed ID in the code (`.uid("divergence-alerts")`) keeps it
whatever else changes. One without gets an ID computed from its place in the graph, so a change
nearby gives it a new one. Flink then finds state in the savepoint that no operator takes, and
refuses to start rather than drop it silently.

Milestone 5 hit this. Computing the 1-second bars once instead of once per query changed the SQL
operators, whose IDs Flink SQL generates, and the signals job's savepoint from milestone 4 no
longer fitted. The signals job is now resumed with `allowNonRestoredState`
(`ticklab/flinkctl.py`). Every operator with a fixed ID gets its state back: the Kafka offsets,
deduplication, the alerts' state machines. The SQL operators' state, a few seconds of bars and
the latest prices, is dropped, and the JobManager logs each operator it skips (32 of them here).
The core job keeps Flink's strict default. Its state is the open candles themselves, so a
mismatch there should stop the start, not pass unnoticed.

Changing what an operator stores is a different matter. The candle accumulator gained a field in
this milestone, and the core job still resumed from its old savepoint: Flink stores the class as
a *POJO* (public fields, a no-argument constructor), and a POJO may gain fields, which start at
Java's default, here 0.

### Zero, or nothing measured?

The milestone 5 check asks Grafana for every panel's data, and it found a figure that had never
worked. Gap repair serves metrics, but Prometheus had no job to scrape it, so its "trades
fetched" figure showed 0 all along: the query ended in `or vector(0)`, which turns "no series"
into 0. A dashboard can't tell "nothing happened" from "nothing measured" unless the metrics
exist from the start. Gap repair now creates its labelled series at 0 when it starts, the figure
lost its fallback, and the check requires Prometheus's own `up` metric to be 1 for every service
that serves metrics.

The same evening showed what gap repair couldn't do. The stack had been stopped for 8 hours, so
1.7 million Binance trades were missing: 72 minutes of requests at gap repair's budget, all held
in memory, while every newer gap waited behind them. The repair was cut short when its container
restarted, and because the consumer had committed each gap's offset soon after reading it, none
of the three gaps was ever reported REPAIRED or FAILED. Now an offset is *stored* for commit
only once the gap's outcome is reported (`enable.auto.offset.store` off, then `store_offsets`):
the same at-least-once rule as Flink's checkpoints, where input counts as read only once its
results are safe. A gap too large to repair within the candles' lateness budget is reported
FAILED at once, with its size (decision note 0011).

## Milestone 6: who moves first

### Turning prices into events

"Does Binance move before Kraken?" needs events to compare, not two price series. A *move* is a
feed's second whose close is more than a threshold away from the close of its previous second
with trades (`MoveDetector.java`). The threshold comes from the data: Binance's 99.9th
percentile of 1-second moves, well above the *bid-ask bounce*, the step a price takes when a buy
at the ask follows a sell at the bid, which is noise rather than news (docs/data-study.md).

Seconds would be too coarse a clock: most leads would read as zero. So each move is timed by
the first trade of its second already beyond the threshold, to the microsecond: the moment that
exchange's price first showed it.

### Pairing events in event time

A move on one exchange pairs with the earliest unpaired move of the other in the same direction
at most 2 s before it (`LeadLagMatcher.java`). That needs the moves in event-time order, which
`InEventTimeOrder` already provides, with one addition: moves of the same second carry the same
Flink timestamp, the end of that second's window, and they arrive in whatever order the two
feeds' windows fired. Sorting them by their own times, then by exchange, makes the result the
same however the input is delivered. A move that no move can reach any more is reported
unpaired by a timer at the end of the second in which its 2 s window closes.

### One definition, written twice

The same definitions exist in Java, in the job, and in plain Python (`ticklab/lead_lag.py`), and
`make check-m6` requires every row from the job to equal the Python result over a whole week.
That only works if nothing is left to chance: exact decimals with the same rounding (half-even
at 12 places) on both sides, the close chosen by trade ID rather than arrival, and a fixed order
for events at the same instant. Over a week of 40 million trades the two agreed on all 1,478
pairs and unpaired moves, which is far more convincing than either one tested alone.

### What a lead can't tell you

Each exchange stamps its trades with its own clock, so a difference between the clocks would
look exactly like a lead; the trades alone can't reveal it. And a market that trades less shows
a move only at its next trade: Kraken trades a fraction as often as Binance, so it tends to look
like the follower even when both prices changed together. A lead is evidence of where prices
are discovered, not proof of it.

### Slots and metaspace: what jobs share

A Flink job runs its tasks in *slots* of the TaskManager. The tasks of one job can share a slot,
but two jobs never do, so a third job needed a third slot (six on the full profile, at
parallelism 2). Managed memory is split evenly between the slots, so it grew to keep each
slot's share (`config/sizing/`).

The third job also brought the JobManager down, the first time the end-to-end test ran with it.
A job submitted through the REST API runs its main method in the JobManager, which loads the
job's jar in a class loader of its own: the Kafka connector, Avro and, for the signals job, the
whole SQL planner. Classes live outside the heap, in the JVM's *metaspace*. Three jobs needed 163
MiB of the 160 MB it had: `OutOfMemoryError: Metaspace`, the JobManager restarted, and a
JobManager without high availability comes back with no jobs at all. Measuring each job's share
gave the new size, 256 MB (decision note 0010).

## Milestone 7: round-number magnets

### A control group for a claim

"Prices turn back at round numbers" is a comparison: more often than at other numbers. So the
job tests other numbers too. Alongside each tier of round levels it tests as many random levels,
one in each step, at a place drawn from SHA-256 of a fixed seed, the pair, the tier and the
step: every run, and the job and Python alike, test the same ones (`RoundLevelTester.java`,
`ticklab/round_levels.py`).

The control changed what the numbers mean. With a band of one typical minute's move, a level
that pulled on nothing would seem to bounce half the time. Random levels bounced only 39% to
51% of the time: a price that has just travelled a band's width to reach a level tends to keep
going. Measured against 50%, every level would look like a weak barrier. Measured against the
random levels, round ones were no different: in every pair and tier, the two bounce shares'
95% intervals overlap.

### Deciding later, in event time

A test is decided by the close at the end of the second 60 s after it began: an event-time
timer, as for the alerts. The decision must see the close of the seconds before it, in order,
even when a fast replay moves the watermark many seconds at once and delivers several seconds
together. The tester therefore holds each second until the watermark passes it and handles
seconds and timers in one event-time order (`InEventTimeOrder`), as the alert operators do.

### Exact, so two programs agree

Prices are whole cents, levels are whole cents, and the random levels come from integer
arithmetic on the hash, so the job and Python put every level in the same place and decide every
test the same way. `make check-m7` requires all of the job's tests over two busy days to equal
Python's: 3,509 tests from 18 million trades, every one the same.

### An identity two events can share

Every row in storage needs an identity, and the first one chosen for a level test was the level
and the moment it was reached. In a spike on 2026-10-08, ETH swung 1.4% within one second, and
taker orders filled at the same microsecond pushed it through the same levels several times,
up and down. Python counted each test; the table keeps one row per key, so tests that shared a
moment collapsed into one, and the check found seven missing. Even the check's own comparison
hid some: a set merges two identical rows, so it now compares counts. The fix was twofold: a
level is tested once at a time (overlapping tests aren't independent anyway), and a test is
identified by the trade that reached the level, which nothing else can share.

### A share needs its interval

A bounce share from a few hundred tests is uncertain by several percentage points. Each one is
reported with a 95% interval (Wilson's, which stays sensible for small counts and shares near
0 or 1), and a difference between round and random levels only means something when it is
larger than those intervals.
