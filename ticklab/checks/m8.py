"""`make check-m8`: whale aftershocks (milestone 8), with its evidence.

1. Two days of Binance history (2026-10-07 and 08) replayed through the analyses job alone, in an
   empty replay stack: every trade delivered and stored once.
2. Stream equals batch: every whale the job reported equals the plain-Python recomputation from
   the same history (`ticklab/whales.py`), and none is missing or extra.
3. The findings, from four weeks of history (2026-09-12 to 10-10) by the same definitions: per
   pair, the average move after a whale at each horizon, and the share that reverted, each with
   its 95% interval.
4. The "Analyses" dashboard shows the replayed days: every query succeeds, and the whale tables
   have rows.

About 1.5 hours: the replay, then four weeks of trades in plain Python.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from ticklab import binance, history, replay, whales
from ticklab.checks.common import OUT_DIR, Report, disk_line, make, say, stamp, write
from ticklab.checks.m6 import Feeds, check_dashboard, check_delivery, settle
from ticklab.clickhouse import ClickHouse
from ticklab.model import Trade
from ticklab.round_levels import wilson
from ticklab.settings import Settings, feeds_for
from ticklab.whales import HORIZONS_S, Whale, WhaleTracker, mean_interval

REPLAYED = (date(2026, 10, 7), date(2026, 10, 8))
FINDINGS = (date(2026, 9, 12), date(2026, 10, 10))
OUT = OUT_DIR / "m8"

Row = tuple
WHALE_PANELS = {"Whale aftershocks", "Latest whales"}


def main() -> int:
    settings = Settings.from_env()
    report = Report("M8")
    say(f"# M8 check, started {stamp()} UTC\n\ndisk: {disk_line()}\n")
    first, last = REPLAYED
    make("down")
    make("replay-up", EXCHANGES="BINANCE", JOBS="analyses")
    started = time.monotonic()
    make("replay-history", FROM=first.isoformat(), TO=last.isoformat(), EXCHANGES="BINANCE")
    say(f"[{stamp()}] replayed in {(time.monotonic() - started) / 60:.0f} min")
    make("replay-drain")
    clickhouse = ClickHouse(settings)
    settle(clickhouse, "whales")

    say(f"[{stamp()}] recomputing the replayed days in plain Python")
    feeds, found = recompute(settings, first, last)
    check_delivery(report, clickhouse, feeds, "1. two days replayed through the analyses job")
    check_stream_against_batch(report, stream_rows(clickhouse), Counter(row(w) for w in found))
    start = datetime.combine(first, datetime.min.time(), UTC)
    end = datetime.combine(last + timedelta(days=1), datetime.min.time(), UTC)
    check_dashboard(
        report, start, end, "4. the Analyses dashboard shows the replayed days", WHALE_PANELS
    )
    make("replay-down")

    say(f"[{stamp()}] four weeks of history in plain Python, for the findings")
    _, found = recompute(settings, *FINDINGS)
    findings(report, found)
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def recompute(settings: Settings, first: date, last: date) -> tuple[Feeds, list[Whale]]:
    feeds: dict[tuple[str, str], list[int]] = {}

    def events() -> Iterator[Trade]:
        for _, trade in replay.history_trades(
            settings.history_dir, feeds_for(binance.EXCHANGE), history.days(first, last)
        ):
            seen = feeds.setdefault((trade.exchange, trade.symbol), [0, trade.trade_id, 0])
            seen[0] += 1
            seen[1] = min(seen[1], trade.trade_id)
            seen[2] = max(seen[2], trade.trade_id)
            yield trade

    tracker = WhaleTracker()
    for trade in events():
        tracker.add(trade)
    found = tracker.finish()
    return {feed: (n, low, high) for feed, (n, low, high) in feeds.items()}, found


def row(w: Whale) -> Row:
    return (
        w.base,
        w.time_us,
        "BUY" if w.buy else "SELL",
        w.first_id,
        w.last_id,
        w.fills,
        w.quantity,
        w.value,
        w.price,
        w.threshold,
        w.window_orders,
        *w.moves,
    )


def stream_rows(clickhouse: ClickHouse) -> Counter[Row]:
    rows = clickhouse.rows(
        """SELECT base, toUnixTimestamp64Micro(time) AS time_us, toString(side) AS side,
                  first_id, last_id, fills, quantity, value, price, threshold, window_orders,
                  move_10s, move_30s, move_60s, move_120s
           FROM ticklab.whales FINAL"""
    )
    return Counter(
        (
            r["base"],
            int(r["time_us"]),
            r["side"],
            int(r["first_id"]),
            int(r["last_id"]),
            int(r["fills"]),
            Decimal(r["quantity"]),
            Decimal(r["value"]),
            Decimal(r["price"]),
            Decimal(r["threshold"]),
            int(r["window_orders"]),
            *(Decimal(r[f"move_{h}s"]) for h in HORIZONS_S),
        )
        for r in rows
    )


def check_stream_against_batch(report: Report, stream: Counter[Row], batch: Counter[Row]) -> None:
    """Counts as well as rows: a whale twice in either is a difference."""
    missing = sorted((batch - stream).elements(), key=str)
    extra = sorted((stream - batch).elements(), key=str)
    say(f"whales: {stream.total():,} from the job, {batch.total():,} from the recomputation")
    for r in missing[:10]:
        say(f"- only in the recomputation: {r}")
    for r in extra[:10]:
        say(f"- only in the job's output: {r}")
    report.record(
        "2. stream equals batch: the job's whales equal the plain-Python recomputation",
        bool(stream) and not missing and not extra,
        f"({stream.total():,} whales, {len(missing)} missing, {len(extra)} extra)",
    )


def findings(report: Report, found: list[Whale]) -> None:
    lines = [
        "| Pair | Whales | Horizon | Average move (bps) | 95% interval | Reverted | 95% interval |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for base in whales.PAIRS:
        mine = [w for w in found if w.base == base]
        for i, horizon in enumerate(HORIZONS_S):
            moves = [float(w.moves[i]) for w in mine]
            mean, low, high = mean_interval(moves)
            reverted = sum(1 for w in mine if w.moves[i] < 0)
            r_low, r_high = wilson(reverted, len(mine))
            share = f"{reverted / len(mine):.0%}" if mine else "-"
            lines.append(
                f"| {base}/USDT | {len(mine):,} | {horizon} s | {mean:+.2f} "
                f"| {low:+.2f} to {high:+.2f} | {share} | {r_low:.0%} to {r_high:.0%} |"
            )
    first, last = FINDINGS
    say(f"\nWhale aftershocks, Binance, {first} to {last}:\n")
    for line in lines:
        say(line)
    write(OUT / "findings.md", "\n".join(lines) + "\n")
    report.record(
        "3. the findings from four weeks: average moves and reverted shares, with 95% intervals",
        bool(found),
        f"({len(found):,} whales over {(last - first).days + 1} days)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
