# Data study (milestone 1)

Two numbers later milestones depend on, measured rather than guessed. Both come from
`ticklab/study.py`; the commands below reproduce them.

## Arrival disorder, from live recordings

`make study-disorder FROM=… TO=… SESSION=…` replays recorded frames in arrival order and, per
feed, compares each trade's time with the newest trade time already seen. A trade that arrives
with an older time than one before it is *late*: a watermark that trailed by less than its
lateness would already have moved past it.

Two recording sessions on 2026-10-11 (UTC), 73 minutes and 65,360 trades:

| Session | Feed | Trades | Repeats | Missing IDs | IDs out of order | Late | Received − trade time p50 / p99 / max (ms) |
|---|---|---:|---:|---:|---:|---:|---|
| 00:07–00:47 | BTC/USDT | 9,070 | 0 | 0 | 0 | 0 | 130 / 333 / 5,160 |
| 00:07–00:47 | ETH/USDT | 20,768 | 0 | 0 | 0 | 0 | 130 / 195 / 5,350 |
| 00:07–00:47 | SOL/USDT | 5,096 | 0 | 0 | 0 | 0 | 130 / 199 / 350 |
| 04:32–05:05 | BTC/USDT | 11,840 | 0 | 0 | 0 | 0 | 125 / 366 / 380 |
| 04:32–05:05 | ETH/USDT | 14,781 | 0 | 0 | 0 | 0 | 124 / 160 / 179 |
| 04:32–05:05 | SOL/USDT | 3,805 | 0 | 0 | 0 | 0 | 124 / 137 / 372 |

- **No disorder at all.** On one connection, Binance delivers each pair strictly in trade-ID and
  trade-time order.
- **Delay is not disorder.** Once, frames arrived up to 5.3 s late, but still in order. A
  watermark simply waits for them: it only moves when newer trade times arrive.
- "Received − trade time" includes the offset between Binance's clock and ours, so it is an
  upper bound on the network delay, not a measurement of it.

**The watermark bound B stays at 2 s.** Reordering can still come from two places, and 2 s
covers both with a wide margin:

- During a connection handover, two connections deliver the same trades with slightly
  different delays, a few tens of milliseconds apart. Repeats are removed before any window
  sees them.
- In replays, the chaos tests (milestone 3) reorder trades on purpose, and their bound is
  chosen relative to B.

Gap repair delivers trades that are far later than B; that is what the 5-minute allowed
lateness L is for.

## Gaps between trades, from a week of history

`make study-gaps FROM=2026-10-03 TO=2026-10-09` reads Binance's daily trade files (38.3 million
trades) and measures the time between each trade and the next one in the same market.

| Feed | Trades | ID breaks | p50 | p99 | p99.9 | p99.99 | Longest (ms) | Ended at (UTC) | Gaps ≥ 10 s | ≥ 30 s | ≥ 60 s |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|
| BTC/USDT | 18,545,451 | 0 | 0 | 782 | 1,983 | 3,516 | 9,814 | 2026-10-03 21:39:05 | 0 | 0 | 0 |
| ETH/USDT | 15,901,140 | 0 | 0 | 1,092 | 3,667 | 7,322 | 32,731 | 2026-10-04 00:42:02 | 360 | 1 | 0 |
| SOL/USDT | 3,892,629 | 0 | 0 | 3,017 | 6,464 | 10,488 | 22,345 | 2026-10-06 05:50:55 | 519 | 0 | 0 |

Quantiles are in whole milliseconds. A median of 0 means at least half of all trades come less
than a millisecond after the previous one, typically one taker order filled against several
resting orders.

Trading volume varies a lot: BTC/USDT had 0.67 million trades on Saturday 2026-10-03 and 4.9
million on Wednesday 2026-10-07.

**Busiest hours** (UTC), which the milestone 5 benchmarks replay:

| Feed | Busiest hours, trades |
|---|---|
| BTC/USDT | 10-07 08:00 872,658; 10-08 15:00 493,789; 10-07 07:00 416,473 |
| ETH/USDT | 10-08 15:00 656,823; 10-08 16:00 463,477; 10-07 02:00 356,795 |
| SOL/USDT | 10-08 15:00 170,095; 10-07 02:00 137,378; 10-08 17:00 104,208 |
| all three | 2026-10-08 15:00 1,320,707; 2026-10-07 08:00 1,055,087; 2026-10-08 16:00 931,483 |

**Silence thresholds.** A feed counts as silent (milestone 3) after twice the longest gap seen in
this week, rounded up to 15 s, and never under 30 s:

| Feed | Longest gap | Threshold |
|---|---:|---:|
| BTC/USDT | 9.8 s | 30 s |
| ETH/USDT | 32.7 s | 75 s |
| SOL/USDT | 22.3 s | 45 s |

The producer replaces a connection after 60 s without any frame. Its one stream carries all
three pairs, so it only goes quiet when all three do at once, and BTC/USDT alone never paused
for even 10 s this week.

Kraken's markets trade far less often and get their own study in milestone 4.
