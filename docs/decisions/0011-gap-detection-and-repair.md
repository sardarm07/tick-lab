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

## Consequences

- A gap closes in seconds after the feed resumes, well within the 5-minute lateness budget.
- Repair adds no new path into the pipeline: the repaired trades follow the live ones.
- A gap open for longer than the budget EXPIRES. Its trades can still arrive, and they go to
  `late-trades` instead of silently changing old candles.
