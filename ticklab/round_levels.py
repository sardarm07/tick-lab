"""Round-number magnets: do prices turn back at round numbers more than anywhere else? (M7)

The definitions of the analyses job's level tests (`RoundLevelTester.java`), in plain Python, so
the job can be checked against them and findings computed over weeks of history. Binance's
three pairs only: they trade in cents, so prices and levels are compared as whole cents.

- **Levels,** in two tiers: major (BTC every 1,000, ETH 100, SOL 10) and minor (BTC every 100,
  ETH 10, SOL 1; a major level is also a minor one). As a control, one random level in each step
  of each tier, drawn from a fixed seed at least a tenth of the minor step away from any round
  level: as many levels as the round ones, but nowhere special.
- **A test** of a level starts at the first trade that reaches it from beyond its band: from at
  or below level - band up to at least the level, or from at or above level + band down to at
  most it. The level is then disarmed until the price is beyond its band again, on either side,
  and it is tested once at a time: reaching it again while its test is open starts no other. A
  test is identified by the trade that reached the level.
- **The outcome** is decided at the end of the second 60 s after the test began, by the close
  then (the last price at or before the end of that second): BREAK beyond the band on the far
  side, BOUNCE beyond it on the near side, NEUTRAL in between.
- A test during which the feed's trade IDs skip (a gap in the data) is discarded.
"""

from __future__ import annotations

import bisect
import hashlib
import math
from array import array
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field, replace

from ticklab import binance
from ticklab.model import Trade

STEPS_CENTS = {
    "BTC": {"MAJOR": 100_000, "MINOR": 10_000},
    "ETH": {"MAJOR": 10_000, "MINOR": 1_000},
    "SOL": {"MAJOR": 1_000, "MINOR": 100},
}
# By pair, in cents: chosen from four weeks of history (docs/data-study.md, decision note 0021);
# the same in RoundLevelRules.java.
BANDS = {"BTC": 1_650, "ETH": 77, "SOL": 5}
TIERS = ("MAJOR", "MINOR")
KINDS = ("ROUND", "RANDOM")
SEED = "ticklab-round-levels-1"
DECISION_S = 60
# Levels further than this many steps from the price are forgotten; their state is rebuilt from
# the price if it comes back.
KEEP_STEPS = 2

BELOW, ABOVE, DISARMED = 1, 2, 0


def cents(price) -> int:
    """A price in whole cents; every price of these pairs is a whole number of cents."""
    value = price * 100
    if value != int(value):
        raise ValueError(f"{price} is not a whole number of cents")
    return int(value)


def random_level(base: str, tier: str, step: int) -> int:
    """The control level in step number `step` of a tier, in cents: a random place in one of the
    minor steps inside it, at least a tenth of a minor step from either end."""
    digest = hashlib.sha256(f"{SEED}:{base}:{tier}:{step}".encode()).digest()
    first, second = int.from_bytes(digest[:8], "big"), int.from_bytes(digest[8:16], "big")
    minor = STEPS_CENTS[base]["MINOR"]
    within = first % 10 if tier == "MAJOR" else 0  # which minor step inside a major one
    offset = minor // 10 + (second * (minor * 8 // 10) >> 64)
    return step * STEPS_CENTS[base][tier] + within * minor + offset


@dataclass(frozen=True)
class LevelTest:
    base: str
    tier: str
    kind: str
    level: int  # cents
    up: bool  # reached from below
    time_us: int
    trade_id: int  # the trade that reached the level
    decide_second: int
    band: int  # cents
    close: int | None = None  # cents, at the decision
    outcome: str | None = None


def outcome(test: LevelTest, close: int) -> str:
    beyond = close - test.level if test.up else test.level - close
    if beyond >= test.band:
        return "BREAK"
    if beyond <= -test.band:
        return "BOUNCE"
    return "NEUTRAL"


@dataclass
class _Feed:
    base: str
    band: int  # cents
    second: int | None = None
    trades: list[tuple[int, int, int]] = field(default_factory=list)  # (ID, time, cents)
    price: int | None = None
    last_id: int | None = None
    close: int | None = None
    states: dict[tuple[str, str], dict[int, int]] = field(default_factory=lambda: defaultdict(dict))
    open: list[LevelTest] = field(default_factory=list)
    randoms: dict[tuple[str, int], int] = field(default_factory=dict)


class LevelTester:
    """The tests of each pair, from trades merged by time: `add` each, then `finish`."""

    def __init__(self, bands: dict[str, int]) -> None:
        self.bands = bands  # by base, in cents
        self.feeds: dict[str, _Feed] = {}
        self.decided: list[LevelTest] = []
        self.discarded = 0

    def add(self, trade: Trade) -> None:
        if trade.exchange != binance.EXCHANGE or trade.base not in self.bands:
            return
        feed = self.feeds.get(trade.base)
        if feed is None:
            feed = self.feeds[trade.base] = _Feed(trade.base, self.bands[trade.base])
        second = trade.trade_time // 1_000_000
        if feed.second is not None and second != feed.second:
            self._second(feed)
        feed.second = second
        feed.trades.append((trade.trade_id, trade.trade_time, cents(trade.price)))

    def finish(self) -> list[LevelTest]:
        """Decides the tests still open, as draining the job does, and returns every test."""
        for feed in self.feeds.values():
            if feed.trades:
                self._second(feed)
            self._decide(feed, None)
        return self.decided

    def _second(self, feed: _Feed) -> None:
        """One second of a feed: first the tests due in the quiet seconds before it, then its
        trades in ID order, then the tests due at its end."""
        assert feed.second is not None
        self._decide(feed, feed.second - 1)
        for trade_id, time_us, price in sorted(feed.trades):
            self._trade(feed, trade_id, time_us, price)
        feed.trades = []
        self._decide(feed, feed.second)

    def _decide(self, feed: _Feed, until: int | None) -> None:
        """Decides the open tests due at or before the end of second `until` (all, if None)."""
        keep = []
        for test in feed.open:
            if until is None or test.decide_second <= until:
                assert feed.close is not None
                decided = replace(test, close=feed.close, outcome=outcome(test, feed.close))
                self.decided.append(decided)
            else:
                keep.append(test)
        feed.open = keep

    def _trade(self, feed: _Feed, trade_id: int, time_us: int, price: int) -> None:
        if feed.last_id is not None and trade_id != feed.last_id + 1:
            self.discarded += len(feed.open)
            feed.open = []
        feed.last_id = trade_id
        feed.close = price
        before = feed.price if feed.price is not None else price
        feed.price = price
        if price == before:
            return
        for tier in TIERS:
            step = STEPS_CENTS[feed.base][tier]
            for kind in KINDS:
                states = feed.states[(tier, kind)]
                for level in self._levels(feed, tier, kind, before, price):
                    state = states.get(level)
                    if state is None:
                        state = _side(before, level, feed.band)
                    if state == BELOW and price >= level:
                        self._start(feed, tier, kind, level, True, time_us, trade_id)
                        state = DISARMED
                    elif state == ABOVE and price <= level:
                        self._start(feed, tier, kind, level, False, time_us, trade_id)
                        state = DISARMED
                    side = _side(price, level, feed.band)
                    states[level] = state if side == DISARMED else side
                for level in [lv for lv in states if abs(lv - price) > KEEP_STEPS * step]:
                    del states[level]

    def _levels(self, feed: _Feed, tier: str, kind: str, a: int, b: int) -> Iterator[int]:
        """The levels whose band the move from `a` to `b` touches, in increasing order."""
        low, high = min(a, b) - feed.band, max(a, b) + feed.band
        step = STEPS_CENTS[feed.base][tier]
        for k in range(low // step, high // step + 1):
            if kind == "ROUND":
                level = k * step
            else:
                level = feed.randoms.get((tier, k))
                if level is None:
                    level = feed.randoms[(tier, k)] = random_level(feed.base, tier, k)
            if low <= level <= high:
                yield level

    def _start(
        self, feed: _Feed, tier: str, kind: str, level: int, up: bool, time_us: int, trade_id: int
    ) -> None:
        if any(t.tier == tier and t.kind == kind and t.level == level for t in feed.open):
            return  # tested once at a time
        decide = (time_us + DECISION_S * 1_000_000) // 1_000_000
        feed.open.append(
            LevelTest(feed.base, tier, kind, level, up, time_us, trade_id, decide, feed.band)
        )


def _side(price: int, level: int, band: int) -> int:
    if price <= level - band:
        return BELOW
    if price >= level + band:
        return ABOVE
    return DISARMED


def wilson(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """A 95% confidence interval for a share."""
    if n == 0:
        return math.nan, math.nan
    p = successes / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return centre - half, centre + half


# The study behind the bands (docs/data-study.md): how far each pair moves in 60 s, and the
# tests' outcomes for a grid of bands, in cents.

BAND_GRID = {
    "BTC": (500, 1_000, 2_000),
    "ETH": (25, 50, 100),
    "SOL": (3, 6, 12),
}


@dataclass
class Study:
    seconds: dict[str, array] = field(default_factory=lambda: defaultdict(lambda: array("q")))
    closes: dict[str, array] = field(default_factory=lambda: defaultdict(lambda: array("q")))
    tests: dict[int, list[LevelTest]] = field(default_factory=dict)  # by place in the grid
    discarded: dict[int, int] = field(default_factory=dict)


def study(events: Iterable[tuple[int, Trade]], grid: dict[str, tuple[int, ...]]) -> Study:
    collected = Study()
    places = range(len(next(iter(grid.values()))))
    testers = [LevelTester({base: bands[i] for base, bands in grid.items()}) for i in places]
    current: dict[str, tuple[int, int, int]] = {}  # base: (second, last ID, close)
    for _, trade in events:
        for tester in testers:
            tester.add(trade)
        if trade.exchange != binance.EXCHANGE or trade.base not in grid:
            continue
        second = trade.trade_time // 1_000_000
        found = current.get(trade.base)
        if found is not None and found[0] != second:
            collected.seconds[trade.base].append(found[0])
            collected.closes[trade.base].append(found[2])
            found = None
        if found is None or trade.trade_id > found[1]:
            current[trade.base] = (second, trade.trade_id, cents(trade.price))
    for base, (second, _, close) in current.items():
        collected.seconds[base].append(second)
        collected.closes[base].append(close)
    for i, tester in enumerate(testers):
        collected.tests[i] = tester.finish()
        collected.discarded[i] = tester.discarded
    return collected


def minute_moves(seconds: array, closes: array) -> list[float]:
    """|close 60 s later against the close then|, in bps, from each second with trades (the
    close of a second without trades is the one before it)."""
    moves = []
    for i, second in enumerate(seconds):
        j = bisect.bisect_right(seconds, second + DECISION_S) - 1
        if seconds[j] > second:
            moves.append(abs(closes[j] - closes[i]) / closes[i] * 10_000)
    return moves


def _quantile(values: list[float], q: float) -> float:
    return values[min(len(values) - 1, int(q * len(values)))] if values else math.nan


def report(collected: Study, grid: dict[str, tuple[int, ...]], days: float) -> str:
    lines = [
        "### How far each pair moves in 60 s",
        "",
        "| Pair | Seconds | p25 (bps) | p50 (bps) | p75 (bps) | p50 in USDT, at the median price |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for base in sorted(grid):
        moves = sorted(minute_moves(collected.seconds[base], collected.closes[base]))
        price = sorted(collected.closes[base])[len(collected.closes[base]) // 2] / 100
        q25, q50, q75 = (_quantile(moves, q) for q in (0.25, 0.5, 0.75))
        lines.append(
            f"| {base}/USDT | {len(moves):,} | {q25:.2f} | {q50:.2f} | {q75:.2f} "
            f"| {q50 * price / 10_000:,.2f} |"
        )
    lines += [
        "",
        "### Tests a day, and how often a decided test bounced",
        "",
        "| Pair | Band (USDT) | Tier | Kind | Tests a day | Break | Bounce | Neutral "
        "| Bounce share of decided | 95% interval |",
        "|---|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for base in sorted(grid):
        for i, band in enumerate(grid[base]):
            for tier in TIERS:
                for kind in KINDS:
                    tests = [
                        t
                        for t in collected.tests[i]
                        if t.base == base and t.tier == tier and t.kind == kind
                    ]
                    counts = {
                        o: sum(1 for t in tests if t.outcome == o)
                        for o in ("BREAK", "BOUNCE", "NEUTRAL")
                    }
                    decided = counts["BREAK"] + counts["BOUNCE"]
                    low, high = wilson(counts["BOUNCE"], decided)
                    share = f"{counts['BOUNCE'] / decided:.1%}" if decided else "-"
                    lines.append(
                        f"| {base}/USDT | {band / 100:g} | {tier} | {kind} "
                        f"| {len(tests) / days:.1f} | {counts['BREAK']:,} | {counts['BOUNCE']:,} "
                        f"| {counts['NEUTRAL']:,} | {share} | {low:.1%} to {high:.1%} |"
                    )
    lines += [
        "",
        "Discarded for a gap in the data: "
        + ", ".join(f"{collected.discarded[i]}" for i in sorted(collected.discarded)),
    ]
    return "\n".join(lines)
