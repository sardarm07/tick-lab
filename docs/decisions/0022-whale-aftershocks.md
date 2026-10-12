# 0022: Whale aftershocks, from taker orders and a rolling percentile

**Status:** accepted, milestone 8

## Context

When someone buys an unusually large amount at once, does the price keep rising, or does it come
back? Answering it needs "unusually large" defined without hindsight, a whale counted as one
order rather than as the dozens of fills it took, and price moves measured from what the whale
actually paid.

## Decision

- **Taker orders, not trades** (`WhaleTracker.java`, `ticklab/whales.py`): on Binance, the fills
  of one taker order share a time and have consecutive trade IDs. Grouping them first means the
  size measured is the taker's, not that of the resting orders it happened to hit. Binance's
  three pairs only, for that reason.
- **"Large" is relative to the hour before:** an order is a whale when its value is above the
  99.9th percentile of the pair's orders in the 60 complete minutes before it, never including
  its own minute, so nothing from after the order decides whether it is one. Nothing is flagged
  until 30 minutes have been seen.
- **A histogram, not a list:** each minute's orders are counted in buckets 5% wide (edges at
  1.05^k USDT), and the hour is the sum of 60 minutes' counts, kept up to date as minutes come
  and go. The percentile is then known to within its bucket, and a whale is an order at or above
  the bucket's top. The edges are exact (1.05^k is 21^k / 20^k), so the job and Python put every
  order in the same bucket.
- **Aftershocks** at 10, 30, 60 and 120 s: the last price at or before each, against the whale's
  own average price, in bps, signed so that positive means the price went on the way the whale
  pushed it. At a horizon where that is negative, the whale has reverted.
- **Findings from four weeks of history** by the Python definitions, which `make check-m8`
  proves equal to the job's on a two-day replay.

## Consequences

- The threshold adapts: a quiet night and a busy afternoon each have their own "large".
- An average move hides how spread out the moves are, so each comes with its 95% interval, and
  each share reverted with its own.
- Moves are measured from trade prices, which bounce between bid and ask: for SOL, whose tick is
  about 0.9 bps of its price, small moves partly measure the spread.
- Over four weeks the answer was continuation, not reversal: the price stayed about 1 bp further
  in the whale's direction for two minutes, and fewer than half of the whales had reverted by
  then (README).
