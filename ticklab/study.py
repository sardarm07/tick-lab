"""The M1 data study, which sets two numbers for later milestones.

- Arrival disorder, from live recordings: how far behind the newest trade time already seen
  in its feed a trade can arrive. Flink's watermark must trail by at least that much (the
  bound B), or such trades count as late. History can't show this: it has no arrival times.
- Gaps between consecutive trades, from history: how long a feed can be quiet in normal
  trading. Silence alerts must not fire on those.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from ticklab.model import Trade


def percentile(histogram: Counter[int], q: float) -> int:
    """The smallest value with at least a share q of all observations at or below it."""
    total = sum(histogram.values())
    target = math.ceil(q * total)
    running = 0
    for value in sorted(histogram):
        running += histogram[value]
        if running >= target:
            return value
    raise ValueError("empty histogram")


@dataclass
class Disorder:
    trades: int = 0
    duplicates: int = 0
    ids_out_of_order: int = 0
    late: int = 0  # arrived with a trade time below the newest one already seen
    min_id: int | None = None
    max_id: int | None = None
    newest_time: int | None = None
    lateness_ms: Counter[int] = field(default_factory=Counter)  # rounded up, late trades only
    latency_ms: Counter[int] = field(default_factory=Counter)  # received - trade time
    seen: set[int] = field(default_factory=set)

    @property
    def missing_ids(self) -> int:
        if self.min_id is None or self.max_id is None:
            return 0
        return self.max_id - self.min_id + 1 - self.trades


def measure_disorder(events: Iterable[tuple[int, Trade]]) -> dict[str, Disorder]:
    """Per feed, in arrival order. Repeats of a trade already seen are counted, then skipped:
    deduplication removes them before any window sees them."""
    feeds: dict[str, Disorder] = defaultdict(Disorder)
    for received, trade in events:
        d = feeds[trade.symbol]
        if trade.trade_id in d.seen:
            d.duplicates += 1
            continue
        d.seen.add(trade.trade_id)
        d.trades += 1
        if d.max_id is not None and trade.trade_id < d.max_id:
            d.ids_out_of_order += 1
        d.min_id = trade.trade_id if d.min_id is None else min(d.min_id, trade.trade_id)
        d.max_id = trade.trade_id if d.max_id is None else max(d.max_id, trade.trade_id)
        if d.newest_time is not None and trade.trade_time < d.newest_time:
            d.late += 1
            d.lateness_ms[math.ceil((d.newest_time - trade.trade_time) / 1000)] += 1
        if d.newest_time is None or trade.trade_time > d.newest_time:
            d.newest_time = trade.trade_time
        d.latency_ms[(received - trade.trade_time) // 1000] += 1
    return dict(feeds)


@dataclass
class Gaps:
    trades: int = 0
    first_time: int | None = None
    last_time: int | None = None
    last_id: int | None = None
    id_breaks: int = 0
    gap_ms: Counter[int] = field(default_factory=Counter)
    longest: tuple[int, int] = (0, 0)  # (gap in microseconds, time it ended)
    per_hour: Counter[datetime] = field(default_factory=Counter)


def measure_gaps(trades: Iterable[Trade]) -> dict[str, Gaps]:
    """Per feed: the time between each trade and the next, with trades in ID order."""
    feeds: dict[str, Gaps] = defaultdict(Gaps)
    for trade in trades:
        g = feeds[trade.symbol]
        if g.last_id is not None and trade.trade_id != g.last_id + 1:
            g.id_breaks += 1
        if g.last_time is not None:
            gap = trade.trade_time - g.last_time
            g.gap_ms[gap // 1000] += 1
            if gap > g.longest[0]:
                g.longest = (gap, trade.trade_time)
        else:
            g.first_time = trade.trade_time
        g.last_id, g.last_time = trade.trade_id, trade.trade_time
        g.trades += 1
        g.per_hour[_hour(trade.trade_time)] += 1
    return dict(feeds)


def _hour(time_us: int) -> datetime:
    return datetime.fromtimestamp(time_us // 3_600_000_000 * 3600, UTC)


def _iso(time_us: int) -> str:
    return datetime.fromtimestamp(time_us / 1e6, UTC).strftime("%Y-%m-%d %H:%M:%S")


def disorder_report(feeds: dict[str, Disorder], label: str) -> str:
    lines = [
        f"### Arrival disorder ({label})",
        "",
        "| Feed | Trades | Repeats | Missing IDs | IDs out of order | Late "
        "| Lateness p99 / p99.9 / max (ms) | Received - trade time p50 / p99 / max (ms) |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for symbol, d in sorted(feeds.items()):
        if d.late:
            lateness = (
                " / ".join(str(percentile(d.lateness_ms, q)) for q in (0.99, 0.999))
                + f" / {max(d.lateness_ms)}"
            )
        else:
            lateness = "none"
        latency = " / ".join(str(percentile(d.latency_ms, q)) for q in (0.5, 0.99))
        lines.append(
            f"| {symbol} | {d.trades:,} | {d.duplicates:,} | {d.missing_ids:,} "
            f"| {d.ids_out_of_order:,} | {d.late:,} | {lateness} "
            f"| {latency} / {max(d.latency_ms)} |"
        )
    return "\n".join(lines)


def gaps_report(feeds: dict[str, Gaps], first: date, last: date) -> str:
    lines = [
        f"### Gaps between consecutive trades ({first} to {last}, UTC)",
        "",
        "| Feed | Trades | ID breaks | p50 | p99 | p99.9 | p99.99 | Longest (ms) | Ended at "
        "| Gaps ≥ 10 s | ≥ 30 s | ≥ 60 s |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|",
    ]
    for symbol, g in sorted(feeds.items()):
        quantiles = " | ".join(str(percentile(g.gap_ms, q)) for q in (0.5, 0.99, 0.999, 0.9999))
        over = [sum(n for ms, n in g.gap_ms.items() if ms >= s * 1000) for s in (10, 30, 60)]
        lines.append(
            f"| {symbol} | {g.trades:,} | {g.id_breaks} | {quantiles} "
            f"| {g.longest[0] // 1000:,} | {_iso(g.longest[1])} "
            f"| {over[0]:,} | {over[1]:,} | {over[2]:,} |"
        )
    lines += ["", "Gap quantiles are in milliseconds.", "", "#### Busiest hours (UTC)", ""]
    lines += ["| Feed | Busiest hours, trades |", "|---|---|"]
    for symbol, g in sorted(feeds.items()):
        top = ", ".join(f"{hour:%m-%d %H}:00 {n:,}" for hour, n in g.per_hour.most_common(3))
        lines.append(f"| {symbol} | {top} |")
    totals: Counter[datetime] = Counter()
    for g in feeds.values():
        totals.update(g.per_hour)
    top = ", ".join(f"{hour:%Y-%m-%d %H}:00 {n:,}" for hour, n in totals.most_common(3))
    lines += [f"| all | {top} |"]
    return "\n".join(lines)
