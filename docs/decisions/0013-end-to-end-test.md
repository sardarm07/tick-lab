# 0013: The end-to-end test: a fixture, chaos, a fake exchange, an exact answer

**Status:** accepted, milestone 3

## Context

Unit tests check pieces. The claim that matters is about the whole pipeline: given a messy feed
(reordered, repeated, delayed, with holes), the candles still come out exactly right. That test
has to run in CI on every push, without a live exchange and without flakiness.

## Decision

- **Fixtures** (`make fixture`, `tests/fixtures/e2e/`): 15 minutes of real Binance trades from
  the daily history files, complete by trade ID, with Binance's official candles. Small enough
  to commit: the first one (`binance-2026-10-10T0400-15m`, 21,881 trades) is 181 KB.
- **Chaos** (`ticklab/chaos.py`) with a fixed seed: reordering within the watermark bound, 1% of
  trades delayed past it but within the lateness budget, 5% repeats, two dropped runs of 20 IDs
  per market.
- **A fake exchange** (`ticklab/fake_exchange.py`) serves the fixture as Binance's REST API.
  Gap repair uses it, and in CI the real exchanges' hostnames resolve to 127.0.0.1
  (`compose.ci.yaml`).
- **The run** (`make e2e`, part of `make ci`): a throwaway stack, replay at 10× (repair needs
  wall-clock seconds, which the lateness budget must cover), wait until every gap has an
  outcome, drain, then assert exactly: every candle equal to the official one, every dropped
  range repaired, nothing in `late-trades`, repeats counted.

## Consequences

- CI needs Docker and about 4 GB of memory for the stack; GitHub's standard runners have 16 GB.
- A seed that fails can be rerun locally with `make chaos SEED=…`.
