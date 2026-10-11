"""The signals study: the distributions behind the alert thresholds (milestone 4).

Plain Python over history, independent of the Flink job, following the definitions:

- 1-second bars per feed: the last price (highest trade ID), volume and trade count.
- Kraken prices converted to USDT with the USDT/USD price as of the Kraken bar's second.
- Each Binance second compared with the newest converted Kraken price up to that second,
  unless it is more than `stale_after_s` old (schemas/spread.avsc).
- 1-minute bars per feed, for one-minute returns and volumes.

It then counts how often each alert would fire over a grid of thresholds, so the thresholds
can be chosen to make alerts rare but not absent (docs/data-study.md).
"""

from __future__ import annotations

import bisect
import itertools
import math
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Decimal, localcontext

from ticklab import binance, kraken
from ticklab.model import Trade

TWELVE_PLACES = Decimal("1e-12")
BPS = Decimal(10_000)


@dataclass
class Bar:
    exchange: str
    symbol: str
    base: str
    second: int  # start of the second, in seconds since the epoch
    price: Decimal = Decimal(0)
    last_id: int = -1
    volume: Decimal = Decimal(0)
    trades: int = 0


def second_bars(events: Iterable[tuple[int, Trade]]) -> Iterator[Bar]:
    """1-second bars of every feed, from trades merged by time. All bars of one second come
    together, Kraken's USDT/USD first, then Kraken's other pairs, then Binance: the order in
    which an "as of" lookup at the end of that second sees them."""
    current: dict[str, Bar] = {}
    open_second: int | None = None
    for _, trade in events:
        second = trade.trade_time // 1_000_000
        if open_second is not None and second != open_second:
            yield from _in_lookup_order(current.values())
            current = {}
        open_second = second
        feed = f"{trade.exchange}:{trade.symbol}"
        bar = current.get(feed)
        if bar is None:
            bar = current[feed] = Bar(trade.exchange, trade.symbol, trade.base, second)
        if trade.trade_id > bar.last_id:
            bar.last_id = trade.trade_id
            bar.price = trade.price
        bar.volume += trade.quantity
        bar.trades += 1
    yield from _in_lookup_order(current.values())


def _in_lookup_order(bars: Iterable[Bar]) -> list[Bar]:
    def rank(bar: Bar) -> int:
        if bar.exchange == kraken.EXCHANGE:
            return 0 if bar.base == "USDT" else 1
        return 2

    return sorted(bars, key=rank)


@dataclass(frozen=True)
class SpreadRow:
    base: str
    second: int
    binance_price: Decimal
    kraken_price: Decimal
    usdt_usd: Decimal
    kraken_price_usdt: Decimal
    spread_bps: Decimal
    kraken_age_s: int


def spreads(bars: Iterable[Bar], stale_after_s: int = 10) -> Iterator[SpreadRow]:
    rate: Decimal | None = None
    latest: dict[str, tuple[int, Decimal, Decimal]] = {}  # base: (second, price, rate)
    for bar in bars:
        if bar.exchange == kraken.EXCHANGE:
            if bar.base == "USDT":
                rate = bar.price
            elif rate is not None:
                latest[bar.base] = (bar.second, bar.price, rate)
            continue
        found = latest.get(bar.base)
        if found is None or bar.second - found[0] > stale_after_s:
            continue
        second, price, usdt_usd = found
        with localcontext() as context:
            context.prec = 60
            converted = (price / usdt_usd).quantize(TWELVE_PLACES, ROUND_HALF_EVEN)
            bps = ((converted - bar.price) * BPS / bar.price).quantize(
                TWELVE_PLACES, ROUND_HALF_EVEN
            )
        yield SpreadRow(
            bar.base, bar.second, bar.price, price, usdt_usd, converted, bps, bar.second - second
        )


@dataclass
class Minute:
    close: Decimal
    last_id: int
    volume: Decimal


@dataclass
class Collected:
    """What the study keeps: per-second spreads per base, and minute bars per feed."""

    spread_times: dict[str, list[int]] = field(default_factory=lambda: defaultdict(list))
    spread_bps: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    binance_seconds: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    minutes: dict[str, dict[int, Minute]] = field(default_factory=lambda: defaultdict(dict))


def collect(events: Iterable[tuple[int, Trade]], stale_after_s: int = 10) -> Collected:
    collected = Collected()

    def bars() -> Iterator[Bar]:
        for bar in second_bars(events):
            if bar.exchange == binance.EXCHANGE:
                collected.binance_seconds[bar.base] += 1
            if bar.base != "USDT":
                feed = f"{bar.exchange}:{bar.symbol}"
                minute = bar.second // 60 * 60
                found = collected.minutes[feed].get(minute)
                if found is None:
                    collected.minutes[feed][minute] = Minute(bar.price, bar.last_id, bar.volume)
                else:
                    if bar.last_id > found.last_id:
                        found.close, found.last_id = bar.price, bar.last_id
                    found.volume += bar.volume
            yield bar

    for row in spreads(bars(), stale_after_s):
        collected.spread_times[row.base].append(row.second)
        collected.spread_bps[row.base].append(float(row.spread_bps))
    return collected


# Alert counts over a grid of thresholds, by the rules in flink/.../AlertRules.java.


def divergence_alerts(
    times: list[int],
    bps: list[float],
    open_bps: float,
    close_bps: float,
    hold_s: int = 30,
    gap_s: int = 5,
    silence_s: int = 60,
) -> list[tuple[int, int]]:
    """(opened, closed) seconds of each alert over one base's spreads."""
    alerts = []
    above = below = opened = None
    last = None
    for t, value in zip(times, bps, strict=True):
        size = abs(value)
        if opened is not None and last is not None and t - last >= silence_s:
            alerts.append((opened, last + silence_s))
            opened = None
        if last is not None and t - last > gap_s:
            above = below = None
        last = t
        if opened is None:
            above = (above if above is not None else t) if size > open_bps else None
            if above is not None and t - above >= hold_s:
                opened, below = above + hold_s, None
        else:
            below = (below if below is not None else t) if size < close_bps else None
            if below is not None and t - below >= hold_s:
                alerts.append((opened, below + hold_s))
                opened = above = below = None
    if opened is not None:
        alerts.append((opened, -1))
    return alerts


def minute_returns(minutes: dict[int, Minute]) -> list[tuple[int, float]]:
    """(minute, log return) between consecutive minutes with trades."""
    result = []
    for before, after in itertools.pairwise(sorted(minutes)):
        if after == before + 60:
            ratio = float(minutes[after].close) / float(minutes[before].close)
            result.append((after, math.log(ratio)))
    return result


def price_move_z(
    minutes: dict[int, Minute], window: int = 60, min_returns: int = 30
) -> list[float]:
    """|z| of each minute's return against the root mean square of the previous returns."""
    zs = []
    history: list[float] = []
    for _, move in minute_returns(minutes):
        if len(history) >= min_returns:
            typical = math.sqrt(sum(r * r for r in history) / len(history))
            if typical > 0:
                zs.append(abs(move / typical))
        history = [*history, move][-window:]
    return zs


def volume_ratios(minutes: dict[int, Minute], window: int = 60) -> list[float]:
    """Each minute's volume over the average of the previous `window` minutes (empty = 0)."""
    if not minutes:
        return []
    ratios = []
    first, last = min(minutes), max(minutes)
    history: list[float] = []
    for minute in range(first, last + 60, 60):
        found = minutes.get(minute)
        volume = float(found.volume) if found else 0.0
        if found and len(history) >= window:
            average = sum(history) / len(history)
            if average > 0:
                ratios.append(volume / average)
        history = [*history, volume][-window:]
    return ratios


def quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return math.nan
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


def at_least(sorted_values: list[float], threshold: float) -> int:
    return len(sorted_values) - bisect.bisect_left(sorted_values, threshold)


def report(collected: Collected, days: float) -> str:
    lines = ["### Spread between Binance and Kraken (converted to USDT), per second", ""]
    lines += [
        "| Base | Seconds compared | Share of Binance seconds | p50 | p90 | p99 | p99.9 | Max |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for base in sorted(collected.spread_bps):
        values = sorted(abs(v) for v in collected.spread_bps[base])
        share = len(values) / max(1, collected.binance_seconds[base])
        quantiles = " | ".join(f"{quantile(values, q):.2f}" for q in (0.5, 0.9, 0.99, 0.999))
        lines.append(f"| {base} | {len(values):,} | {share:.1%} | {quantiles} | {values[-1]:.2f} |")
    lines += ["", "|spread| in basis points (1 bp = 0.01%)."]
    grid = (1.5, 2, 2.5, 3, 4, 5)
    for hold in (10, 30):
        lines += [
            "",
            f"Divergence alerts a day: |spread| above each value for {hold} s, closing below half",
            "of it for as long:",
            "",
            "| Base | " + " | ".join(f"{x:g} bps" for x in grid) + " |",
            "|---|" + "---:|" * len(grid),
        ]
        for base in sorted(collected.spread_bps):
            times, bps = collected.spread_times[base], collected.spread_bps[base]
            counts = [len(divergence_alerts(times, bps, x, x / 2, hold)) / days for x in grid]
            lines.append(f"| {base} | " + " | ".join(f"{c:.1f}" for c in counts) + " |")

    z_grid = (5, 6, 8, 10, 12)
    v_grid = (8, 10, 15, 20, 30, 50)
    lines += ["", "### One-minute moves and volumes", ""]
    lines += [
        "Minutes a day whose |z| (return over the past hour's root mean square) reaches each "
        "value, and whose volume reaches each multiple of the past hour's average minute:",
        "",
        "| Feed | Minutes | "
        + " | ".join(f"z ≥ {z}" for z in z_grid)
        + " | "
        + " | ".join(f"vol ≥ {v}x" for v in v_grid)
        + " |",
        "|---|---:|" + "---:|" * (len(z_grid) + len(v_grid)),
    ]
    for feed in sorted(collected.minutes):
        minutes = collected.minutes[feed]
        zs = sorted(price_move_z(minutes))
        ratios = sorted(volume_ratios(minutes))
        z_counts = " | ".join(f"{at_least(zs, z) / days:.1f}" for z in z_grid)
        v_counts = " | ".join(f"{at_least(ratios, v) / days:.1f}" for v in v_grid)
        lines.append(f"| {feed} | {len(minutes):,} | {z_counts} | {v_counts} |")
    return "\n".join(lines)
