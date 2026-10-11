"""Replays recordings or history into Kafka, through the same normaliser as live data."""

from __future__ import annotations

import heapq
import logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ticklab import binance, history, recorder
from ticklab.kafka import TradePublisher
from ticklab.model import Trade
from ticklab.settings import Feed

log = logging.getLogger(__name__)


@dataclass
class Unparsed:
    """Frames that couldn't be normalised; the live producer skips them the same way."""

    count: int = 0


def recorded_trades(
    paths: Iterable[Path], feeds: list[Feed], start_us: int, end_us: int, unparsed: Unparsed
) -> Iterator[tuple[int, Trade]]:
    """(arrival time, trade) for each trade in recorded Binance frames that arrived in
    [start_us, end_us), in arrival order."""
    by_native = {feed.native: feed for feed in feeds}
    urls: dict[tuple[str, int], str] = {}
    for session, line in recorder.read_files(paths):
        if line["kind"] == "open":
            urls[(session, line["conn"])] = line["url"]
        elif line["kind"] == "frame" and start_us <= line["recv_us"] < end_us:
            scale = binance.time_scale(urls[(session, line["conn"])])
            try:
                frame = binance.parse_frame(line["raw"], line["recv_us"], by_native, scale)
            except Exception:
                unparsed.count += 1
                continue
            for trade in frame.trades:
                yield line["recv_us"], trade


def history_trades(
    history_dir: Path, feeds: list[Feed], replay_days: list[date]
) -> Iterator[tuple[int, Trade]]:
    """(trade time, trade) for each row of the daily files, all markets merged by trade time."""

    def one_feed(feed: Feed) -> Iterator[tuple[int, Trade]]:
        for day in replay_days:
            scale = binance.history_time_scale(day)
            for row in history.read_rows(history.local_path(history_dir, feed, day)):
                trade = binance.parse_history_row(row, feed, scale)
                yield trade.trade_time, trade

    return heapq.merge(*(one_feed(feed) for feed in feeds), key=lambda item: item[0])


@dataclass
class ReplayResult:
    trades: int
    seconds: float
    undelivered: int
    failed: int


def run(
    events: Iterable[tuple[int, Trade]], publisher: TradePublisher, speed: float | None
) -> ReplayResult:
    """Publish every trade. With a speed, event time runs `speed` times faster than the
    clock (1 = as it happened); without one, as fast as possible."""
    started = time.monotonic()
    first_event: int | None = None
    count = 0
    next_report = started + 10
    for event_us, trade in events:
        if speed:
            if first_event is None:
                first_event = event_us
            due = started + (event_us - first_event) / 1e6 / speed
            while (wait := due - time.monotonic()) > 0:
                publisher.poll(min(wait, 0.1))
        publisher.publish(trade)
        count += 1
        if (now := time.monotonic()) >= next_report:
            log.info("%d trades replayed, %.0f a second", count, count / (now - started))
            next_report = now + 10
    undelivered = publisher.flush(120)
    return ReplayResult(count, time.monotonic() - started, undelivered, publisher.failed)
