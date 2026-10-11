# Tick Lab

A live monitor for crypto trades, built to learn stream processing properly.

Trades from Binance (and, from milestone 4, Kraken) flow through Python producers into Redpanda,
are processed by Flink, stored in ClickHouse and shown in Grafana. Every number the pipeline
produces is checked against the exchanges' own figures.

**Status:** work in progress. Milestone 1 is done: live Binance trades are normalised,
published as Avro, stored in ClickHouse, recorded raw, and can be replayed exactly. The
exchanges' trade history can be imported and replayed the same way.

## Running it

Needs Docker with Compose, GNU Make and [uv](https://docs.astral.sh/uv/). It is developed on a
machine with 2 cores and 8 GB of RAM (a GitHub codespace on the default image).

```sh
make up        # live stack: Binance -> producer -> Redpanda -> ClickHouse, plus Prometheus and Grafana
make ps        # containers and their health
make down      # stop (data is kept)
```

Grafana is on http://localhost:3000 (dashboard "Feed health"), Prometheus on :9090 and
ClickHouse's HTTP interface on :8123. All ports are bound to 127.0.0.1.

Other targets (`make help` lists them all):

| Target | What it does |
|---|---|
| `make record` | Records live frames without Docker or Kafka |
| `make history FROM=2026-10-09` | Downloads Binance's daily trade files and checks them |
| `make replay-up`, `make replay FROM=… TO=…` | Replays recordings into a separate, empty stack |
| `make replay-history FROM=…` | Replays history the same way |
| `make ci` | Lint and unit tests, as GitHub Actions runs them |
| `make check-m1` | The milestone 1 checks, with their evidence (about 30 minutes) |

Settings live in `.env`, created from `.env.example` on first use. Nothing in it is secret.

## Learning

[LEARNING.md](LEARNING.md) explains each streaming concept as it appears in the code, and
[docs/decisions/](docs/decisions/) records why things are built the way they are.
