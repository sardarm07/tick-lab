# Tick Lab

A live monitor for crypto trades, built to learn stream processing properly.

Trades from Binance and Kraken flow through Python producers into Redpanda, are processed by
Flink, stored in ClickHouse and shown in Grafana. Every number the pipeline produces is checked
against the exchanges' own figures.

**Status:** work in progress. Milestones 1 to 4 are done:

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
  joins), and raises alerts for divergences, price moves and volume spikes.

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

## Running it

Needs Docker with Compose, GNU Make and [uv](https://docs.astral.sh/uv/). It is developed on a
machine with 2 cores and 8 GB of RAM (a GitHub codespace on the default image).

```sh
make up        # live stack: Binance, Kraken -> producers -> Redpanda -> Flink -> ClickHouse, Grafana
make ps        # containers and their health
make down      # stop; Flink saves its state first, and `make up` resumes from it
```

Grafana is on http://localhost:3000 (dashboards "Feed health", "Correctness" and "Market"), the Flink
web UI on :8081, Prometheus on :9090 and ClickHouse's HTTP interface on :8123. All ports are
bound to 127.0.0.1.

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
| `make e2e`, `make e2e-kraken`, `make chaos SEED=11` | The end-to-end tests: a fixture with chaos through a throwaway stack |
| `make fail-taskmanager`, `fail-producer`, `fail-network` [`PRODUCER=kraken`] | Failures to inject into the live stack |
| `make ci` | Lint, Python and Java tests and the end-to-end test, as GitHub Actions runs them |
| `make check-m1` … `make check-m4` | Each milestone's checks, with their evidence |

Settings live in `.env`, created from `.env.example` on first use. Nothing in it is secret.

## Learning

[LEARNING.md](LEARNING.md) explains each streaming concept as it appears in the code, and
[docs/decisions/](docs/decisions/) records why things are built the way they are.
