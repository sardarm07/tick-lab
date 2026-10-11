# 0009: Each Flink job removes duplicates itself

**Status:** accepted, milestone 2

## Context

Trades reach the `trades` topic more than once by design: overlapping connections during a
handover, replays, gap repair. Either a separate job could write a "clean trades" topic for
everyone else, or each job could deduplicate its own input.

## Decision

Every job runs the same `Dedup` operator (`flink/src/main/java/ticklab/flink/Dedup.java`) right
after its source, keyed by feed:

- State per feed: the highest ID up to which every trade has been seen, plus a map of IDs that
  arrived ahead of a missing one.
- For one watermark bound after a feed's first trade, every ID goes into the map, so trades
  reordered at the very start aren't lost.
- A gap still open once the watermark is L past the first trade after it is given up on, and its
  IDs are recorded as skipped. If one turns up later anyway, it is let through once, as a late
  trade.
- Counters `duplicates`, `skippedIds` and `recoveredAfterSkip` go to Prometheus.

## Consequences

- No extra topic and no Kafka transactions between jobs; each job's result depends only on the
  raw trades.
- The deduplication state is a few numbers per feed in normal operation.
- The same state is where gap detection (milestone 3) gets its gaps from.
