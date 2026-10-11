# 0018: Alert thresholds, chosen from a week of history

**Status:** accepted, milestone 4

## Context

An alert that fires every few minutes gets ignored, and one that never fires teaches nothing.
The thresholds of the three alerts (decision 0017) should make each one rare but real, and the
only way to know what "rare" means for these markets is to measure them.

`make study-signals FROM=2026-10-04 TO=2026-10-10` replays a week of both exchanges' history
through plain Python (`ticklab/signal_study.py`), with the job's definitions, and counts how
often each alert would fire at a range of thresholds (docs/data-study.md has the tables).

What the week showed:

- Kraken, converted to USDT, tracks Binance closely: the median |spread| is 0.5 to 0.9 bps and
  the 99th percentile 2 to 5 bps. The widest seconds (34 to 59 bps) are single seconds during
  fast moves, when one exchange's last trade is already out of date.
- A spread above 2 bps for 30 s in a row happened about once a day for each asset; above
  2.5 bps, less than once a day; above 5 bps, never.
- One-minute moves of at least 8 times the past hour's typical move: 0.4 to 1.1 minutes a day
  per feed, on both exchanges alike.
- Volume is where the exchanges differ. A minute of 20 times the past hour's average happened
  1.3 to 2.7 times a day on Binance, but 6 to 10 times a day on Kraken, whose thinner markets
  are moved by single large orders. 50 times happened 0.7 to 2.9 times a day on Kraken.

## Decision

`flink/src/main/java/ticklab/flink/AlertRules.java`:

| Alert | Opens | Closes | Per day in the study week |
|---|---|---|---|
| Divergence | \|spread\| above 2 bps for 30 s | below 1 bp for 30 s | about 1 per asset |
| Price move | \|z\| of at least 8 | \|z\| below 4 | 0.4 to 1.1 per feed |
| Volume spike, Binance | 20 times the hour's average minute | below 10 times | 1.3 to 2.7 per feed |
| Volume spike, Kraken | 50 times | below 25 times | 0.7 to 2.9 per feed |

Each alert closes at half its opening threshold: hysteresis, so a value hovering around the
threshold doesn't open and close it repeatedly. A divergence also closes after 60 s without a
spread, and a spread is only computed while Kraken's price is at most 10 s old, which it is in
93% to 98% of Binance's seconds.

## Consequences

- In the study week, about 20 qualifying runs or minutes a day across all assets and feeds,
  fewer alerts once consecutive minutes merge into one: enough to see them work, few enough
  to read.
- The counts are of minutes or runs in one week; markets change, and the study can be re-run.
- The thresholds are constants in the job. Changing them means rebuilding and restarting the
  signals job, which keeps its state through a savepoint.
