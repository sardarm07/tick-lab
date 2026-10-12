"""Who moves first: price moves on each exchange, paired across the two (milestone 6).

The definitions of the Flink analyses job (`MoveDetector.java`, `LeadLagMatcher.java`), in plain
Python, so the job can be checked against them and the thresholds chosen from history:

- A feed's *close* for a second is the price of its last trade (the highest trade ID) in that
  second. Its *reference* is the close of its last earlier second with trades, if that second
  began at most `stale_after_s` before; an older price is too stale to compare against, and the
  second only sets a new reference. Kraken's USDT/USD is a rate, not a market compared here.
- The second is a *move* when its close differs from the reference by more than the base's
  threshold, in basis points. The move's time is that of the first trade in the second (by
  trade ID) already beyond the threshold in the move's direction: when that exchange's price
  first showed the move.
- Moves of the same base on the two exchanges, in the same direction and at most `window_us`
  apart, are paired one-to-one in event-time order: each move takes the earliest unpaired move
  of the other exchange within reach. The earlier move of a pair leads. A move that no move of
  the other exchange follows within the window is reported unpaired.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Decimal, localcontext

from ticklab import binance, kraken
from ticklab.model import Side, Trade

TWELVE_PLACES = Decimal("1e-12")
BPS = Decimal(10_000)
STALE_AFTER_S = 10
WINDOW_US = 2_000_000
EXCHANGES = (binance.EXCHANGE, kraken.EXCHANGE)


def bps(price: Decimal, reference: Decimal) -> Decimal:
    """(price - reference) / reference in basis points, rounded half-even to 12 places, as
    `Decimals.divide` does in the job."""
    with localcontext() as context:
        context.prec = 60
        return ((price - reference) * BPS / reference).quantize(TWELVE_PLACES, ROUND_HALF_EVEN)


@dataclass(frozen=True)
class Second:
    """One second of one feed that has a fresh reference to compare with."""

    exchange: str
    base: str
    second: int  # start of the second, in seconds since the epoch
    reference: Decimal
    bps: Decimal  # the close against the reference
    trades: tuple[tuple[int, Decimal], ...]  # (trade time in µs, price), by trade ID


@dataclass(frozen=True)
class Move:
    exchange: str
    base: str
    second: int
    time_us: int
    up: bool
    bps: Decimal


@dataclass(frozen=True)
class LeadLag:
    """A pair of moves, or a move no other exchange followed (no follower)."""

    base: str
    up: bool
    leader: str
    leader_time_us: int
    leader_bps: Decimal
    follower_time_us: int | None = None
    follower_bps: Decimal | None = None

    @property
    def paired(self) -> bool:
        return self.follower_time_us is not None

    @property
    def lead_us(self) -> int | None:
        if self.follower_time_us is None:
            return None
        return self.follower_time_us - self.leader_time_us


@dataclass
class _Feed:
    exchange: str
    base: str
    second: int | None = None
    trades: list[tuple[int, int, Decimal]] = field(default_factory=list)  # (ID, time, price)
    reference: Decimal | None = None
    reference_second: int | None = None


def seconds(
    events: Iterable[tuple[int, Trade]], stale_after_s: int = STALE_AFTER_S
) -> Iterator[Second]:
    """Each second with trades of each feed that has a fresh reference, from trades merged by
    time. A feed's second is complete once a later trade of that feed arrives, so the seconds
    of different feeds come out of order."""
    feeds: dict[str, _Feed] = {}
    for _, trade in events:
        if trade.base == "USDT":
            continue
        key = f"{trade.exchange}:{trade.symbol}"
        feed = feeds.get(key)
        if feed is None:
            feed = feeds[key] = _Feed(trade.exchange, trade.base)
        second = trade.trade_time // 1_000_000
        if (
            feed.second is not None
            and second != feed.second
            and (done := _close(feed, stale_after_s)) is not None
        ):
            yield done
        feed.second = second
        feed.trades.append((trade.trade_id, trade.trade_time, trade.price))
    for feed in feeds.values():
        if feed.trades and (done := _close(feed, stale_after_s)) is not None:
            yield done


def _close(feed: _Feed, stale_after_s: int) -> Second | None:
    assert feed.second is not None
    trades = sorted(feed.trades)
    feed.trades = []
    close = trades[-1][2]
    reference, reference_second = feed.reference, feed.reference_second
    feed.reference, feed.reference_second = close, feed.second
    if reference is None or reference_second is None:
        return None
    if feed.second - reference_second > stale_after_s:
        return None
    return Second(
        feed.exchange,
        feed.base,
        feed.second,
        reference,
        bps(close, reference),
        tuple((time, price) for _, time, price in trades),
    )


def move(second: Second, threshold: Decimal) -> Move | None:
    """The second as a move, if its close is more than `threshold` bps from its reference."""
    if abs(second.bps) <= threshold:
        return None
    up = second.bps > 0
    for time_us, price in second.trades:
        change = bps(price, second.reference)
        if (change > threshold) if up else (change < -threshold):
            return Move(second.exchange, second.base, second.second, time_us, up, second.bps)
    raise AssertionError("the closing trade itself is beyond the threshold")


def moves(
    events: Iterable[tuple[int, Trade]],
    thresholds: dict[str, Decimal],
    stale_after_s: int = STALE_AFTER_S,
) -> list[Move]:
    """Every move, in the order the job handles them: by second, then time, then exchange."""
    found = [
        m
        for s in seconds(events, stale_after_s)
        if s.base in thresholds and (m := move(s, thresholds[s.base])) is not None
    ]
    return sorted(found, key=order)


def order(m: Move) -> tuple[int, int, str]:
    return m.second, m.time_us, m.exchange


def match(moves_in_order: Iterable[Move], window_us: int = WINDOW_US) -> list[LeadLag]:
    """Pairs and unpaired moves, from moves sorted by `order`.

    Each second's moves are handled first, then the moves no later move can reach any more are
    reported unpaired, as the job's timers do at the end of each second."""
    found: list[LeadLag] = []
    pending: dict[str, dict[str, list[Move]]] = defaultdict(lambda: {e: [] for e in EXCHANGES})
    last_second: dict[str, int] = {}

    def expire(base: str, horizon_us: int | None) -> None:
        for exchange, waiting in pending[base].items():
            keep = []
            for m in waiting:
                if horizon_us is None or m.time_us + window_us < horizon_us:
                    found.append(LeadLag(base, m.up, exchange, m.time_us, m.bps))
                else:
                    keep.append(m)
            pending[base][exchange] = keep

    for m in moves_in_order:
        if m.base in last_second and m.second != last_second[m.base]:
            expire(m.base, (last_second[m.base] + 1) * 1_000_000)
        last_second[m.base] = m.second
        other = pending[m.base][_other(m.exchange)]
        partner = next(
            (p for p in other if p.up == m.up and m.time_us - p.time_us <= window_us), None
        )
        if partner is None:
            pending[m.base][m.exchange].append(m)
            continue
        other.remove(partner)
        found.append(
            LeadLag(m.base, m.up, partner.exchange, partner.time_us, partner.bps, m.time_us, m.bps)
        )
    for base in pending:
        expire(base, None)
    return found


def _other(exchange: str) -> str:
    return kraken.EXCHANGE if exchange == binance.EXCHANGE else binance.EXCHANGE


# The study behind the thresholds (docs/data-study.md).

# Binance's 99.9th percentile of 1-second moves in the week of 2026-10-04, rounded up to a whole
# basis point (docs/data-study.md, decision note 0020); the same in LeadLagRules.java.
THRESHOLDS = {"BTC": Decimal(5), "ETH": Decimal(7), "SOL": Decimal(9)}

GRID = {
    "BTC": (Decimal(2), Decimal(3), Decimal(5), Decimal(8), Decimal(10)),
    "ETH": (Decimal(3), Decimal(5), Decimal(7), Decimal(10), Decimal(15)),
    "SOL": (Decimal(5), Decimal(8), Decimal(9), Decimal(15), Decimal(20)),
}


@dataclass
class Study:
    """Collected in one pass, per exchange and base: 1-second returns, the bid-ask bounce, and
    the moves at each threshold of the grid."""

    returns: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    bounce: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    moves: dict[tuple[str, Decimal], list[Move]] = field(default_factory=lambda: defaultdict(list))


def study(events: Iterable[tuple[int, Trade]], grid: dict[str, tuple[Decimal, ...]]) -> Study:
    collected = Study()
    last: dict[str, tuple[Side, Decimal]] = {}

    def with_bounce() -> Iterator[tuple[int, Trade]]:
        """The price step between consecutive trades taken from opposite sides of the book: a
        buy at the ask, then a sell at the bid, steps by about the spread."""
        for item in events:
            trade = item[1]
            if trade.base != "USDT":
                feed = f"{trade.exchange}:{trade.base}"
                before = last.get(feed)
                if before is not None and before[0] != trade.taker_side:
                    collected.bounce[feed].append(abs(float(bps(trade.price, before[1]))))
                last[feed] = (trade.taker_side, trade.price)
            yield item

    for s in seconds(with_bounce()):
        collected.returns[f"{s.exchange}:{s.base}"].append(abs(float(s.bps)))
        for threshold in grid.get(s.base, ()):
            if (m := move(s, threshold)) is not None:
                collected.moves[(s.base, threshold)].append(m)
    return collected


def _quantile(values: list[float], q: float) -> float:
    return values[min(len(values) - 1, int(q * len(values)))] if values else math.nan


def report(collected: Study, grid: dict[str, tuple[Decimal, ...]], days: float) -> str:
    lines = [
        "### 1-second returns and bid-ask bounce",
        "",
        "| Exchange:base | Seconds | p50 | p90 | p99 | p99.9 | p99.99 | Bounce p50 | Bounce p90 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for feed in sorted(collected.returns):
        values = sorted(collected.returns[feed])
        bounce = sorted(collected.bounce[feed])
        cells = " | ".join(f"{_quantile(values, q):.2f}" for q in (0.5, 0.9, 0.99, 0.999, 0.9999))
        lines.append(
            f"| {feed} | {len(values):,} | {cells} | {_quantile(bounce, 0.5):.2f} "
            f"| {_quantile(bounce, 0.9):.2f} |"
        )
    lines += [
        "",
        "Returns: |close against the reference| of each second with a fresh reference, in bps.",
        "Bounce: |price step| between consecutive trades taken from opposite sides, in bps.",
        "",
        "### Moves and pairs a day, per threshold",
        "",
        "| Base | Threshold (bps) | Binance moves | Kraken moves | Pairs | Led by Binance "
        "| Led by Kraken | Ties | Median lead, Binance first (ms) | Kraken first (ms) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for base in sorted(grid):
        for threshold in grid[base]:
            found = sorted(collected.moves.get((base, threshold), []), key=order)
            pairs = [r for r in match(found) if r.paired]
            moved = {e: sum(1 for m in found if m.exchange == e) for e in EXCHANGES}
            led = {e: sum(1 for r in pairs if r.lead_us and r.leader == e) for e in EXCHANGES}
            ties = sum(1 for r in pairs if r.lead_us == 0)
            shares = [f"{led[e] / len(pairs):.0%}" if pairs else "-" for e in EXCHANGES]
            medians = []
            for e in EXCHANGES:
                leads = [r.lead_us / 1000 for r in pairs if r.lead_us and r.leader == e]
                medians.append(f"{statistics.median(leads):.0f}" if leads else "-")
            lines.append(
                f"| {base} | {threshold} | {moved[binance.EXCHANGE] / days:.1f} "
                f"| {moved[kraken.EXCHANGE] / days:.1f} | {len(pairs) / days:.1f} "
                f"| {shares[0]} | {shares[1]} | {ties} | {medians[0]} | {medians[1]} |"
            )
    return "\n".join(lines)
