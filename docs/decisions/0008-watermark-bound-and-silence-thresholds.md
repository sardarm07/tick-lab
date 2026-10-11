# 0008: Watermark bound and silence thresholds, from the data study

**Status:** accepted, milestone 1 (used from milestones 2 and 3)

## Context

Two constants decide how the pipeline treats time:

- **B:** how far a feed's watermark trails its newest trade time. Too small, and in-order
  trades count as late. Too large, and every candle is emitted later than it needs to be.
- **Silence thresholds:** how long a feed may go without a trade before it counts as silent.
  Too small, and normal quiet spells raise alerts.

Both were left open in the plan until real data could be measured.

## Decision

From `docs/data-study.md`:

- **B = 2 s** for the core job. Live Binance data showed no disorder at all (0 late trades in
  65,360). The margin covers connection handovers and replays. The analyses keep B = 10 s.
- **Silence thresholds:** twice the longest gap seen in a week of history, rounded up to 15 s,
  at least 30 s. That gives BTC/USDT 30 s, ETH/USDT 75 s and SOL/USDT 45 s. Kraken's pairs get
  theirs from their own data in milestone 4.

## Consequences

- Candles are emitted about 2 s after their minute ends, as soon as a later trade arrives.
- The study is repeatable (`make study-disorder`, `make study-gaps`), so both numbers can be
  re-measured if market behaviour changes.
