# 0011: Gaps detected in Flink, repaired through the trades topic

**Status:** accepted, milestone 3

## Context

A producer restart, a network cut or a reconnect leaves trades missing. Binance's REST API can
return any past trade by ID, so a gap can be filled, but two questions remain: when is an ID
really missing rather than reordered, and how do repaired trades reach the candles?

## Decision

- **Detection in the core job** (`Dedup.java`): a gap is reported (DETECTED, on `gaps`) only
  once the watermark passes the first trade after it. Until then the missing trades may simply
  be late within the watermark bound.
- **Repair as a separate service** (`ticklab/gap_repair.py`): it reads DETECTED gaps, fetches
  the IDs with `historicalTrades`, and publishes them to `trades` like any other trade (source
  REST). The core job treats them as late trades: within the lateness budget they update the
  candle, and deduplication drops any it already had.
- **Statuses on one topic**, keyed by the gap's first missing ID: DETECTED, CLOSED and EXPIRED
  from Flink, REPAIRED and FAILED from gap repair. ClickHouse keeps one row per status, so repair
  times can be measured.
- **Rate limits:** at most 600 of Binance's 6,000 request weight per minute; on 429 or 418 the
  repair stops and the gap is reported FAILED rather than retried blindly, and no Binance request
  is sent until the response's `Retry-After` has passed (milestone 4). Kraken: decision 0016.
- **Only gaps that can still be repaired in time** (milestone 5): a gap of more than 50,000
  trades, about 2 minutes of Binance requests, is reported FAILED at once with its size. Such
  gaps come from downtime: the stack stopped for 8 hours left 1.7 million Binance trades
  missing, 72 minutes of fetching, during which every newer gap waited. Trades are published a
  page at a time, so memory doesn't grow with the gap, and a gap's offset on `gaps` is stored
  only once its outcome is reported, so a repair cut short by a restart is done again, never
  skipped.

## Consequences

- A gap closes in seconds after the feed resumes, well within the 5-minute lateness budget.
- Repair adds no new path into the pipeline: the repaired trades follow the live ones.
- A gap open for longer than the budget EXPIRES. Its trades can still arrive, and they go to
  `late-trades` instead of silently changing old candles.
- The trades of a downtime stay missing from `trades`, reported as a FAILED gap with its size;
  the exchanges' history files have them (`make history`, `make kraken-history`).
