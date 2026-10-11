"""The Binance producer: live trades to Kafka, and every raw frame to the recorder.

`record` mode runs the same feed without Kafka, so recordings can be made without the
rest of the stack.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from collections import Counter

from prometheus_client import start_http_server
from websockets.asyncio.client import ClientConnection

from ticklab import binance, metrics
from ticklab.kafka import TradePublisher
from ticklab.model import Frame, Trade
from ticklab.recorder import Recorder, session_name
from ticklab.settings import Feed, Settings, feeds_for
from ticklab.stream import LiveStream

log = logging.getLogger(__name__)


class BinanceAdapter:
    exchange = binance.EXCHANGE

    def __init__(self, ws_base: str, feeds: list[Feed]) -> None:
        self._url = binance.stream_url(ws_base, feeds)
        self._feeds = {feed.native: feed for feed in feeds}

    def url(self) -> str:
        return self._url

    async def on_open(self, ws: ClientConnection) -> None:
        """The streams are named in the URL, so there is nothing to subscribe to."""

    def parse(self, raw: str, received_us: int, url: str) -> Frame:
        return binance.parse_frame(raw, received_us, self._feeds, binance.time_scale(url))


async def run_binance(settings: Settings, publish: bool) -> None:
    feeds = feeds_for(binance.EXCHANGE)
    publisher = TradePublisher(settings, client_id="producer-binance") if publish else None
    recorder = Recorder(
        settings.recordings_dir, binance.EXCHANGE, session_name("live" if publish else "record")
    )
    counts: Counter[str] = Counter()

    def on_trades(trades: tuple[Trade, ...]) -> None:
        for trade in trades:
            if publisher:
                publisher.publish(trade)
            counts[trade.symbol] += 1
            metrics.TRADES.labels(trade.exchange, trade.symbol).inc()
            metrics.LAST_TRADE_TIME.labels(trade.exchange, trade.symbol).set(trade.trade_time / 1e6)

    stream = LiveStream(
        BinanceAdapter(settings.binance_ws_url, feeds),
        on_trades,
        recorder,
        max_age_s=settings.binance_max_connection_age_s,
        stale_after_s=settings.stale_after_s,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    if publish:
        start_http_server(settings.metrics_port)
    log.info("recording to %s (session %s)", settings.recordings_dir, recorder.session)
    background = [asyncio.create_task(_report(counts, recorder, publisher, stop))]
    if publisher:
        background.append(asyncio.create_task(_serve_deliveries(publisher, stop)))
    try:
        await stream.run(stop)
    finally:
        for task in background:
            task.cancel()
        if publisher:
            left = publisher.flush(30)
            log.info(
                "kafka: %d delivered, %d failed, %d undelivered at exit",
                publisher.delivered,
                publisher.failed,
                left,
            )
        recorder.close()
        log.info("stopped")


async def _serve_deliveries(publisher: TradePublisher, stop: asyncio.Event) -> None:
    """Run delivery callbacks even when no trades are arriving."""
    while not stop.is_set():
        publisher.poll(0)
        await asyncio.sleep(0.1)


async def _report(
    counts: Counter[str], recorder: Recorder, publisher: TradePublisher | None, stop: asyncio.Event
) -> None:
    """One progress line a minute."""
    started = time.monotonic()
    while not stop.is_set():
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=60)
        minutes = (time.monotonic() - started) / 60
        trades = ", ".join(f"{symbol} {n}" for symbol, n in sorted(counts.items()))
        kafka = (
            f"; kafka delivered {publisher.delivered}, failed {publisher.failed}"
            if publisher
            else ""
        )
        log.info(
            "%.0f min: trades %s; recorder lines %d%s",
            minutes,
            trades or "none",
            recorder.lines,
            kafka,
        )
