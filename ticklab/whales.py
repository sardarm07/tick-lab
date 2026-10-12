"""Whale aftershocks: after a very large taker order, does the price keep going, or come back? (M8)

The definitions of the analyses job's whale tracking (`WhaleTracker.java`), in plain Python, so
the job can be checked against them and findings computed over weeks of history. Binance's
three pairs: the fills of one taker order there share a time and have consecutive trade IDs.

- **Taker orders:** consecutive trades of a pair with the same time, the same taker side and
  consecutive IDs are one order. Its value is the sum of price times quantity, its price the
  value over the quantity.
- **Whales:** orders worth more than the 99.9th percentile of the pair's orders in the 60
  complete minutes before (never the trade's own minute). The percentile comes from a histogram
  whose buckets are 5% wide, so it is known to within its bucket; a whale is an order at or
  above the top of that bucket. No whale is flagged until 30 minutes of history have been seen.
- **Aftershocks:** the last price at or before 10, 30, 60 and 120 s after a whale, against the
  whale's price, in bps, signed by its side: positive when the price went on the way the whale
  pushed it. At a horizon where that is negative, the whale has *reverted*.
"""

from __future__ import annotations

import bisect
import math
import statistics
from collections import Counter
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Decimal, localcontext

from ticklab import binance
from ticklab.model import Side, Trade

PAIRS = ("BTC", "ETH", "SOL")
HORIZONS_S = (10, 30, 60, 120)
WINDOW_MINUTES = 60
WARM_UP_MINUTES = 30
# Order values are counted in units of 1e-10 USDT: prices have 2 decimals, quantities at most 8.
VALUE_UNIT = 10**10
# Bucket edges at 1.05^k USDT, k = 0..400, exact: 1.05^k is 21^k / 20^k.
EDGES = [21**k * VALUE_UNIT // 20**k for k in range(401)]
TWELVE_PLACES = Decimal("1e-12")
BPS = Decimal(10_000)


def bucket(value: int) -> int:
    """How many edges are at or below a value: bucket k holds values from edge k-1 up to edge k."""
    return bisect.bisect_right(EDGES, value)


def bps(price: Decimal, reference: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 60
        return ((price - reference) * BPS / reference).quantize(TWELVE_PLACES, ROUND_HALF_EVEN)


@dataclass(frozen=True)
class Whale:
    base: str
    time_us: int
    buy: bool
    first_id: int
    last_id: int
    fills: int
    quantity: Decimal
    value: Decimal  # USDT
    price: Decimal  # value / quantity, rounded half-even to 12 places
    threshold: Decimal  # USDT: the top of the 99.9th percentile's bucket
    window_orders: int
    moves: tuple[Decimal, ...] = ()  # bps, signed, at each horizon


@dataclass
class _Pair:
    base: str
    second: int | None = None
    trades: list[tuple[int, int, Decimal, Decimal, bool]] = field(default_factory=list)
    close: Decimal | None = None
    first_minute: int | None = None
    minute: int | None = None  # the minute whose orders are being counted
    minutes: dict[int, Counter] = field(default_factory=dict)
    window: Counter = field(default_factory=Counter)
    window_orders: int = 0
    threshold_bucket: int | None = None  # for the current minute; None: no whales yet
    pending: list[tuple[Whale, list[Decimal | None]]] = field(default_factory=list)


class WhaleTracker:
    """The whales of each pair, from trades merged by time: `add` each, then `finish`."""

    def __init__(self) -> None:
        self.pairs: dict[str, _Pair] = {}
        self.whales: list[Whale] = []

    def add(self, trade: Trade) -> None:
        if trade.exchange != binance.EXCHANGE or trade.base not in PAIRS:
            return
        pair = self.pairs.get(trade.base)
        if pair is None:
            pair = self.pairs[trade.base] = _Pair(trade.base)
        second = trade.trade_time // 1_000_000
        if pair.second is not None and second != pair.second:
            self._second(pair)
        pair.second = second
        pair.trades.append(
            (
                trade.trade_id,
                trade.trade_time,
                trade.price,
                trade.quantity,
                trade.taker_side == Side.BUY,
            )
        )

    def finish(self) -> list[Whale]:
        """Settles every whale still waiting for a horizon with the last price, as draining the
        job does, and returns them all."""
        for pair in self.pairs.values():
            if pair.trades:
                self._second(pair)
            self._settle(pair, None)
        return self.whales

    def _second(self, pair: _Pair) -> None:
        """One second of a pair: the horizons that fell in the quiet seconds before it, then its
        trades in ID order, as orders, then the horizons that fall in it."""
        assert pair.second is not None
        start_us = pair.second * 1_000_000
        self._settle(pair, start_us - 1)
        trades = sorted(pair.trades)
        pair.trades = []
        i = 0
        while i < len(trades):
            j = i + 1
            while (
                j < len(trades)
                and trades[j][0] == trades[j - 1][0] + 1
                and trades[j][1] == trades[i][1]
                and trades[j][4] == trades[i][4]
            ):
                j += 1
            self._order(pair, trades[i:j])
            i = j
        self._settle(pair, start_us + 999_999)

    def _order(self, pair: _Pair, fills: list[tuple[int, int, Decimal, Decimal, bool]]) -> None:
        time_us = fills[0][1]
        # Horizons that come before this order's time take the price before it.
        self._settle(pair, time_us - 1)
        minute = time_us // 60_000_000
        self._roll(pair, minute)
        quantity = sum((f[3] for f in fills), Decimal(0))
        value = sum((f[2] * f[3] for f in fills), Decimal(0))
        units = int(value * VALUE_UNIT)
        k = bucket(units)
        if pair.threshold_bucket is not None and k > pair.threshold_bucket:
            with localcontext() as context:
                context.prec = 60
                price = (value / quantity).quantize(TWELVE_PLACES, ROUND_HALF_EVEN)
            threshold = Decimal(EDGES[pair.threshold_bucket]) / VALUE_UNIT
            whale = Whale(
                pair.base,
                time_us,
                fills[0][4],
                fills[0][0],
                fills[-1][0],
                len(fills),
                quantity,
                value,
                price,
                threshold,
                pair.window_orders,
            )
            pair.pending.append((whale, [None] * len(HORIZONS_S)))
        pair.minutes.setdefault(minute, Counter())[k] += 1
        pair.close = fills[-1][2]

    def _roll(self, pair: _Pair, minute: int) -> None:
        """Moves the window to the 60 complete minutes before `minute`, and finds its 99.9th
        percentile once per minute."""
        if pair.first_minute is None:
            pair.first_minute = minute
        if pair.minute == minute:
            return
        if pair.minute is not None:
            done = pair.minutes.get(pair.minute)
            if done:
                pair.window.update(done)
                pair.window_orders += sum(done.values())
        for old in [m for m in pair.minutes if m < minute - WINDOW_MINUTES]:
            gone = pair.minutes.pop(old)
            pair.window.subtract(gone)
            pair.window_orders -= sum(gone.values())
        pair.minute = minute
        pair.threshold_bucket = None
        if minute - pair.first_minute >= WARM_UP_MINUTES and pair.window_orders > 0:
            rank = (999 * pair.window_orders + 999) // 1000
            seen = 0
            for k in sorted(b for b, n in pair.window.items() if n > 0):
                seen += pair.window[k]
                if seen >= rank:
                    pair.threshold_bucket = k
                    break

    def _settle(self, pair: _Pair, until_us: int | None) -> None:
        """Gives each waiting horizon at or before `until_us` (all, if None) the price now, and
        reports the whales whose last horizon is done."""
        keep = []
        for whale, prices in pair.pending:
            for i, horizon in enumerate(HORIZONS_S):
                due = whale.time_us + horizon * 1_000_000
                if prices[i] is None and (until_us is None or due <= until_us):
                    prices[i] = pair.close
            if all(p is not None for p in prices):
                sign = 1 if whale.buy else -1
                moves = tuple(sign * bps(p, whale.price) for p in prices)  # type: ignore[arg-type]
                self.whales.append(Whale(**{**whale.__dict__, "moves": moves}))
            else:
                keep.append((whale, prices))
        pair.pending = keep


def mean_interval(values: list[float], z: float = 1.96) -> tuple[float, float, float]:
    """The mean, and a 95% interval for it (normal approximation)."""
    if len(values) < 2:
        return (values[0] if values else math.nan), math.nan, math.nan
    mean = statistics.fmean(values)
    half = z * statistics.stdev(values) / math.sqrt(len(values))
    return mean, mean - half, mean + half
