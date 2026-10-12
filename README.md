# Tick Lab

A live monitor for crypto trades, built to learn stream processing properly.

Trades from Binance and Kraken flow through Python producers into Redpanda, are processed by
Flink, stored in ClickHouse and shown in Grafana. Every number the pipeline produces is checked
against the exchanges' own figures.

**Status:** work in progress. Milestones 1 to 5 are done:

- live Binance trades are normalised, published as Avro, stored in ClickHouse, recorded raw,
  and can be replayed exactly;
- Flink removes duplicates and builds 1-minute candles in event time;
- a reconciler compares every settled minute with Binance's official candles, field by field;
- gaps in trade IDs are found in Flink and filled from Binance's REST API, and feeds that go
  quiet are reported;
- an end-to-end test replays a fixture with injected chaos (reordering, delays, repeats and
  missing trades) through a throwaway stack, in CI;
- Kraken's trades come in alongside Binance's, are reconciled against Kraken's own candles, and
  its gaps are filled from its REST API, which pages by time rather than by trade ID;
- a second Flink job compares BTC, ETH and SOL on both exchanges every second, converting
  Kraken's US dollar prices to USDT at the USDT/USD price of the moment (Flink SQL with temporal
  joins), and raises alerts for divergences, price moves and volume spikes;
- the busiest hour of the history, replayed at 1, 10 and 100 times real time on a 2-core and a
  4-core machine, measures throughput, consumer lag and latency, which a "Pipeline" dashboard
  also shows live.

## How it fits together

```mermaid
flowchart LR
  subgraph exchanges[Exchanges]
    BWS[Binance WebSocket]
    KWS[Kraken WebSocket]
    REST[REST APIs: old trades, official candles]
  end
  BWS --> PB[producer-binance]
  KWS --> PK[producer-kraken]
  PB -. raw frames .-> REC[(recordings)]
  PK -. raw frames .-> REC
  PB --> T[[trades topic: one partition per feed]]
  PK --> T
  REC -. replay .-> T
  T --> CORE[Flink core job: deduplication, gaps, 1-minute candles, feed health]
  T --> SIG[Flink signals job: 1-second bars, spreads, alerts]
  CORE --> OUT[[candles, gaps, feed-health, late-trades]]
  SIG --> OUT2[[spread-1s, alerts]]
  OUT -- gaps --> GR[gap repair]
  REST --> GR
  GR --> T
  T --> CH[(ClickHouse)]
  OUT --> CH
  OUT2 --> CH
  REST --> RC[reconciler]
  RC --> CH
  CH --> GF[Grafana]
  PR[Prometheus] --> GF
```

Producers normalise each exchange's trades into one Avro schema, one Kafka partition per feed.
Two Flink jobs read every trade: the core job removes duplicates, finds gaps (which gap repair
fills from the exchange's REST API) and builds 1-minute candles; the signals job compares the
two exchanges every second. ClickHouse stores everything, idempotently, and the reconciler
checks every minute against the exchange's own candles.

## Results

### Correctness

In the milestone 2 check, all 120 live minutes on each of the three pairs, and all 4,320
minutes of a replayed day of history, matched exactly. In the milestone 3 check, the Flink
TaskManager was killed, the producer restarted and its internet cut for 60 s; every minute
around those failures still matched (64 on each pair), and all 9 gaps they left were filled
within a second of being found.

In the milestone 4 check, all 120 live minutes matched on all seven feeds of both exchanges,
through a TaskManager kill and a restart and a network cut of each producer. Kraken's past 24
hours, replayed, matched all 2,852 minutes it still publishes. A day of both exchanges, replayed
twice, gave the same 152,691 spreads and 23 alerts both times, each spread equal to a
plain-Python recomputation from the same history.

### Throughput and latency

The benchmark replays the busiest hour of the history (2026-10-08 15:00 to 16:00 UTC, 1,362,042
trades from both exchanges) at 1, 10 and 100 times real time, into an empty stack running both
Flink jobs (`make bench`, decision note 0019). Every run's results, checkpoints and memory
included, are in [bench/results/](bench/results/).

| Profile | Speed | Trades a second | Largest consumer lag, core / signals | Trade pipeline latency, p50 / p99 | Candle output latency, p50 / p99 |
|---|---:|---:|---:|---:|---:|
| lite (2 cores, 8 GB) | 1× | 378 | 0 / 0 | 2.6 s / 5.1 s | 4.6 s / 15.3 s |
| lite (2 cores, 8 GB) | 10× | 3,783 | 0 / 0 | 2.8 s / 5.7 s | 0.6 s / 2.2 s |
| lite (2 cores, 8 GB) | 100× | 37,826 | 664,222 / 429,723 | 15.4 s / 22.3 s | 28.7 s / 38.0 s |
| full (4 cores, 16 GB) | 1× | 378 | 0 / 0 | 2.6 s / 5.1 s | 4.6 s / 15.4 s |
| full (4 cores, 16 GB) | 10× | 3,784 | 0 / 0 | 2.7 s / 5.4 s | 0.6 s / 2.1 s |
| full (4 cores, 16 GB) | 100× | 37,830 | 11,053 / 18,667 | 3.7 s / 6.9 s | 0.4 s / 4.5 s |

- **The lite profile keeps up with ten times the busiest hour,** 3,783 trades a second, with no
  lag at all. At 100 times, Kafka still took every trade on schedule, but the Flink jobs fell up
  to 664,000 trades behind, and the end of the replay took 75 s to drain and settle instead of
  27 s. The full profile kept up even at 100 times, never more than 19,000 trades (half a
  second) behind. It has twice the cores and memory and runs the jobs at parallelism 2, so these
  runs don't say which of those mattered.
- **Pipeline latency is ClickHouse's batching,** not processing: ClickHouse reads `trades` from
  Kafka in blocks flushed at most every 5 s, so a trade waits 2.6 s in the median at any speed
  the stack keeps up with.
- **Candle output latency is the watermark's.** A minute's candles come out once the watermark
  passes the minute's end, and the watermark is the minimum over all seven feeds. In a replay,
  where idleness is off, that means waiting for the quietest feed's next trade: 4.6 s in the
  median at 1×, 0.6 s at 10×, where the same wait in trade time passes ten times faster.
- **Checkpoints stay small and quick:** at most 1.5 s and 1.2 MB (the signals job, lite at
  100×), because the state is a few open windows and the latest prices.

Live, on the lite profile over 7 quiet minutes (2026-10-11 22:08 to 22:15 UTC: 22,917 trades and
49 candles):

| Latency | Binance, p50 / p99 | Kraken, p50 / p99 |
|---|---|---|
| Feed: received by our producer minus the exchange's trade time | 89 / 326 ms | −23 / 28 ms |
| Pipeline: stored in ClickHouse minus published to Kafka | 2.2 / 5.1 s | 2.3 / 5.1 s |
| Candle output: emitted minus the publishing of its newest trade | 8.5 / 15.7 s | 10.6 / 21.0 s |

Live, idleness is on: a quiet feed holds the watermark back until it has been silent for 10 s,
so candles come out about 10 s after their minute, and a quiet feed's output latency also counts
the rest of the minute after its last trade. Kraken's feed latency is negative because its clock
was ahead of this machine's.

## Limitations

- **One machine, one hour.** The benchmarks describe a GitHub codespace replaying one busy hour
  of one week. They are a baseline to compare changes against, not a capacity claim.
- **Two clocks.** Feed latency subtracts an exchange's timestamp from our clock, so it includes
  the offset between them, which changes with the machine: Kraken's median was 0.3 s in one
  session and −23 ms in another.
- **One watermark per job.** A minute's candles wait for the quietest of the seven feeds: live,
  until it trades past the minute or has been silent for 10 s. A busy pair's candle can wait on
  a quiet one.
- **Downtime leaves holes.** A repaired trade still reaches its candle only within the 5-minute
  lateness budget, so an outage longer than that leaves minutes without candles; their trades
  are stored all the same, and listed in `late_trades`. Gaps of more than 50,000 trades, such as
  the stack being stopped for hours, are reported as failed and not fetched at all.
- **Spreads are a live signal.** The 1-second bars drop trades that come later than the
  watermark, such as repaired ones; candles, which accept late trades for 5 minutes, are the
  exact record.
- **No high availability.** One broker, one JobManager, one ClickHouse. Flink recovers from a
  TaskManager crash by itself, from its last checkpoint. A JobManager crash stops the jobs, and
  `make up` restarts them from Kafka's committed offsets but without their state, so the
  candles of the minute in progress come out incomplete.

## Running it

Needs Docker with Compose, GNU Make and [uv](https://docs.astral.sh/uv/). It is developed on a
machine with 2 cores and 8 GB of RAM (a GitHub codespace on the default image).

```sh
make up        # live stack: Binance, Kraken -> producers -> Redpanda -> Flink -> ClickHouse, Grafana
make ps        # containers and their health
make down      # stop; Flink saves its state first, and `make up` resumes from it
```

Grafana is on http://localhost:3000 (dashboards "Feed health", "Correctness", "Market" and
"Pipeline"), the Flink web UI on :8081, Prometheus on :9090 and ClickHouse's HTTP interface on
:8123. All ports are bound to 127.0.0.1.

Other targets (`make help` lists them all):

| Target | What it does |
|---|---|
| `make record [EXCHANGE=kraken]` | Records live frames without Docker or Kafka |
| `make history FROM=2026-10-09` | Downloads Binance's daily trade files and checks them |
| `make kraken-history FROM=… TO=…` | Downloads Kraken's trades for a time range and checks them |
| `make replay-up`, `make replay FROM=… TO=…` | Replays recordings into a separate, empty stack |
| `make replay-history FROM=…`, `make replay-kraken-history FROM=… TO=…` | Replays history the same way |
| `make replay-drain` | Ends a replay: every remaining window fires |
| `make reconcile FROM=… TO=…` | Compares candles with the exchanges' official ones, minute by minute |
| `make study-signals FROM=… TO=…` | The distributions behind the alert thresholds |
| `make bench SPEED=10 PROFILE=lite` | Replays the busiest hour at 1, 10 or 100 times real time and measures it |
| `make e2e`, `make e2e-kraken`, `make chaos SEED=11` | The end-to-end tests: a fixture with chaos through a throwaway stack |
| `make fail-taskmanager`, `fail-producer`, `fail-network` [`PRODUCER=kraken`] | Failures to inject into the live stack |
| `make ci` | Lint, Python and Java tests and the end-to-end test, as GitHub Actions runs them |
| `make check-m1` … `make check-m5` | Each milestone's checks, with their evidence |

Settings live in `.env`, created from `.env.example` on first use. Nothing in it is secret.

## Learning

[LEARNING.md](LEARNING.md) explains each streaming concept as it appears in the code, and
[docs/decisions/](docs/decisions/) records why things are built the way they are.
