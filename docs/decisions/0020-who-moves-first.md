# 0020: Who moves first, from moves paired across the exchanges

**Status:** accepted, milestone 6

## Context

When BTC jumps, does Binance's price move first, or Kraken's? Answering it needs a definition of
a move that ignores noise, a way to pair a move on one exchange with the same move on the other,
and times precise enough that a lead of a fraction of a second shows. It also has to be
computed twice, in the Flink job and in plain Python, with the same result, so the job can be
checked.

## Decision

- **A move** (`MoveDetector.java`, `ticklab/lead_lag.py`): a feed's second whose close (its last
  trade by ID) is more than the base's threshold away from its reference, the close of the
  feed's previous second with trades. Seconds on a 1-second grid would make most leads zero, so
  a move is timed by the first trade of the second already beyond the threshold in the move's
  direction: when that exchange's price first showed it.
- **A reference at most 10 s old.** Kraken's pairs trade every few seconds, so its previous
  close is often from a few seconds back; a move then appears at Kraken's first trade at the new
  price. A reference older than 10 s, the same limit as the spreads' (decision 0017), is too
  stale to compare with.
- **Thresholds from a week of history** (2026-10-04 to 10, `make study-lead-lag`): Binance's
  99.9th percentile of 1-second moves, rounded up to a whole basis point. BTC 5, ETH 7 and SOL
  9 bps. Each is at least 3.6 times the 90th percentile of the noisier exchange's bid-ask
  bounce (the price step between consecutive trades from opposite sides of the book), and gives
  about 17 to 28 pairs a day per asset (docs/data-study.md).
- **Pairing** (`LeadLagMatcher.java`): in event-time order, each move takes the earliest
  unpaired move of the other exchange in the same direction at most 2 s before it, and the
  earlier of the two leads. A move that no move of the other exchange reaches within 2 s is
  reported unpaired, by a timer at the end of the second in which its window closes. Moves of
  one second are handled in order of their own times, then by exchange, whatever order they
  arrive in, so the result is the same however the input is delivered.
- **A third Flink job, `analyses`**, reading the trades under its own consumer group like the
  signals job, so the analyses can replay weeks of history on their own (`JOBS=analyses`).
  Output: `analysis-lead-lag`, then `lead_lag` in ClickHouse, keyed by the leading move.

## Consequences

- Times are each exchange's own. A difference between their clocks would look exactly like a
  lead, and it can't be measured from the trades: exchanges synchronise their clocks to UTC, so
  leads of hundreds of milliseconds are unlikely to come from it, but leads of a few are
  meaningless.
- The less active market shows a move only at its next trade, so it tends to look like the
  follower even when prices changed at the same moment. Kraken trades far less often than
  Binance; part of Binance's lead is that.
- Like the spreads, moves come from 1-second windows that drop trades later than the
  watermark: a signal about now, not a record.
- Each job needs a task slot of its own on the session cluster: 3 on the lite profile and 6 on
  the full one (decision 0010).
