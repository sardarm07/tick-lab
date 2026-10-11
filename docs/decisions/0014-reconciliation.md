# 0014: Reconciling every settled minute against the exchange

**Status:** accepted, milestone 2

## Context

"The candles are right" needs a referee outside the pipeline. Binance publishes official
1-minute candles (`klines`), including the trade count and taker-buy volumes, so every minute
can be checked field by field. The questions are when to check (too early and a late trade can
still change the candle) and what to do with a mismatch.

## Decision

`ticklab/reconciler.py`, running every minute in the live stack:

- **When:** a minute is compared once it has *settled*: 6 minutes after it ends (B = 2 s, plus
  L = 5 min, plus a margin), and only once the job has emitted a candle at least L newer. The
  second condition holds the comparison back while Flink works through a backlog, for example
  after a restart. A mismatch is checked again 10 minutes after the minute ends, and that result
  replaces the first.
- **What:** open, high, low, close, volume, quote volume, trade count, taker-buy volume and
  taker-buy quote volume, compared exactly as decimals. A missing candle of ours counts as zero
  trades (rule 7 of the plan). VWAP isn't compared: Binance doesn't publish it.
- **Why, for mismatches:** a likely cause, checked in order:
  1. *producer down*: live frames stopped for more than 10 s around the minute;
  2. *too late*: trades of the minute ended up in `late-trades`;
  3. *gap not repaired*: trade IDs are missing around the minute;
  4. *unknown*: none of these. An unknown mismatch is a bug until proven otherwise.
- **Evidence:** raw `klines` responses are recorded like WebSocket frames, and the official
  candles are stored next to ours in ClickHouse (`official_candles`).

## Consequences

- The Grafana "Correctness" dashboard shows the exact-match rate and every mismatch with its
  cause.
- The bar for a milestone is no *unexplained* mismatch, not no mismatch: outages happen, and they
  must be explained.
- Binance block trades (`@blockTrade`, off-exchange) are recorded and counted. None occurred on
  the three pairs during milestone 2's runs, so whether they count in official candles is still
  open.
