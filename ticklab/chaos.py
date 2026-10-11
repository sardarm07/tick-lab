"""Chaos for replays: the troubles of a real feed, injected on purpose, with a fixed seed.

Starting from trades in trade-time order, each trade gets an arrival time, and then:

- **reordering:** most trades arrive up to 1.5 s after they happened, in shuffled order. That is
  within the watermark bound B (2 s), so none of them may count as late;
- **delays:** 1% arrive 3 to 120 s late: past B, so after their candle was first emitted, but
  within the lateness budget L (5 minutes), so the candle must be emitted again, corrected;
- **duplicates:** 5% arrive a second time, up to 1 s after the first;
- **dropped ranges:** two runs of 20 consecutive trade IDs per market never arrive at all. The
  core job must report each gap and gap repair must fetch it, from the fake exchange in tests.

Everything stays within L, so the expected result is exact: candles equal to the official ones,
every gap repaired, no trade in `late-trades`.
"""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field

from ticklab.model import Trade

REORDER_S = 1.5
DELAY_SHARE, DELAY_RANGE_S = 0.01, (3.0, 120.0)
DUPLICATE_SHARE, DUPLICATE_WITHIN_S = 0.05, 1.0
DROPPED_RANGES, DROPPED_LENGTH = 2, 20


@dataclass
class Injected:
    duplicates: int = 0
    delayed: int = 0
    dropped: dict[str, list[tuple[int, int]]] = field(default_factory=lambda: defaultdict(list))


def apply(
    events: Iterable[tuple[int, Trade]], seed: int
) -> tuple[list[tuple[int, Trade]], Injected]:
    """(arrival time in µs, trade) in arrival order, and what was injected."""
    rng = random.Random(seed)
    trades = [trade for _, trade in events]
    injected = Injected()
    dropped_ids: set[tuple[str, int]] = set()
    by_symbol: dict[str, list[int]] = defaultdict(list)
    for trade in trades:
        by_symbol[trade.symbol].append(trade.trade_id)
    for symbol, ids in sorted(by_symbol.items()):
        # Spread the ranges over the middle of the window, away from its edges.
        span = len(ids) // (DROPPED_RANGES + 1)
        for n in range(DROPPED_RANGES):
            low = span * (n + 1) - DROPPED_LENGTH // 2
            start = rng.randint(max(low - span // 4, 1), low + span // 4)
            first, last = ids[start], ids[start + DROPPED_LENGTH - 1]
            injected.dropped[symbol].append((first, last))
            dropped_ids.update((symbol, i) for i in range(first, last + 1))

    arrivals: list[tuple[int, int, Trade]] = []
    for order, trade in enumerate(trades):
        if (trade.symbol, trade.trade_id) in dropped_ids:
            continue
        if rng.random() < DELAY_SHARE:
            delay = rng.uniform(*DELAY_RANGE_S)
            injected.delayed += 1
        else:
            delay = rng.uniform(0, REORDER_S)
        arrival = trade.trade_time + int(delay * 1_000_000)
        arrivals.append((arrival, order, trade))
        if rng.random() < DUPLICATE_SHARE:
            again = arrival + int(rng.uniform(0, DUPLICATE_WITHIN_S) * 1_000_000)
            arrivals.append((again, order, trade))
            injected.duplicates += 1
    arrivals.sort(key=lambda item: (item[0], item[1]))
    return [(arrival, trade) for arrival, _, trade in arrivals], injected
