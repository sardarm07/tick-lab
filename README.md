# Tick Lab

A live monitor for crypto trades, built to learn stream processing properly.

Trades from Binance (and, from milestone 4, Kraken) flow through Python producers into Redpanda,
are processed by Flink, stored in ClickHouse and shown in Grafana. Every number the pipeline
produces is checked against the exchanges' own figures.

**Status:** work in progress. Milestones 1 and 2 are done:

- live Binance trades are normalised, published as Avro, stored in ClickHouse, recorded raw,
  and can be replayed exactly;
- Flink removes duplicates and builds 1-minute candles in event time;
- a reconciler compares every settled minute with Binance's official candles, field by field.

In the milestone 2 check, all 120 live minutes on each of the three pairs, and all 4,320
minutes of a replayed day of history, matched exactly.

## Running it

Needs Docker with Compose, GNU Make and [uv](https://docs.astral.sh/uv/). It is developed on a
machine with 2 cores and 8 GB of RAM (a GitHub codespace on the default image).

```sh
make up        # live stack: Binance -> producer -> Redpanda -> Flink -> ClickHouse, Prometheus, Grafana
make ps        # containers and their health
make down      # stop; Flink saves its state first, and `make up` resumes from it
```

Grafana is on http://localhost:3000 (dashboards "Feed health" and "Correctness"), the Flink
web UI on :8081, Prometheus on :9090 and ClickHouse's HTTP interface on :8123. All ports are
bound to 127.0.0.1.

Other targets (`make help` lists them all):

| Target | What it does |
|---|---|
| `make record` | Records live frames without Docker or Kafka |
| `make history FROM=2026-10-09` | Downloads Binance's daily trade files and checks them |
| `make replay-up`, `make replay FROM=… TO=…` | Replays recordings into a separate, empty stack |
| `make replay-history FROM=…` | Replays history the same way |
| `make replay-drain` | Ends a replay: every remaining window fires |
| `make reconcile FROM=… TO=…` | Compares candles with Binance's official ones, minute by minute |
| `make ci` | Lint, Python and Java tests, as GitHub Actions runs them |
| `make check-m1`, `make check-m2` | Each milestone's checks, with their evidence |

Settings live in `.env`, created from `.env.example` on first use. Nothing in it is secret.

## Learning

[LEARNING.md](LEARNING.md) explains each streaming concept as it appears in the code, and
[docs/decisions/](docs/decisions/) records why things are built the way they are.
