"""The producers: live trades to Kafka, and every raw frame to the recorder.

One process per exchange (Binance, Kraken), each with one WebSocket connection for all of its
pairs. `record` mode runs the same feed without Kafka, so recordings can be made without the
rest of the stack.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import signal
import time
from collections import Counter

from prometheus_client import start_http_server
from websockets.asyncio.client import ClientConnection

from ticklab import binance, kraken, metrics
from ticklab.kafka import TradePublisher
from ticklab.model import Frame, Trade
from ticklab.recorder import Recorder, session_name
from ticklab.settings import Feed, Settings, feeds_for
from ticklab.stream import LiveStream

log = logging.getLogger(__name__)


class BinanceAdapter:
    exchange = binance.EXCHANGE

    def __init__(self, settings: Settings, feeds: list[Feed]) -> None:
        self._url = binance.stream_url(settings.binance_ws_url, feeds)
        self._feeds = {feed.native: feed for feed in feeds}
        # Binance closes connections after 24 hours, and its streams send nothing while the
        # markets are quiet.
        self.max_age_s = settings.binance_max_connection_age_s
        self.stale_after_s = settings.binance_stale_after_s

    def url(self) -> str:
        return self._url

    async def on_open(self, ws: ClientConnection) -> None:
        """The streams are named in the URL, so there is nothing to subscribe to."""

    def parse(self, raw: str, received_us: int, url: str) -> Frame:
        return binance.parse_frame(raw, received_us, self._feeds, binance.time_scale(url))


class KrakenAdapter:
    """Kraken documents no limit on a connection's age, so connections aren't replaced on a
    schedule. Liveness comes from its heartbeats: one a second when there is nothing else to
    send, so a connection that delivers nothing for a few seconds is dead.

    Kraken allows about 150 connection attempts per IP in any 10 minutes (more means a 10-minute
    ban). The backoff keeps far below that: at most about 25 attempts in 10 minutes of
    failures."""

    exchange = kraken.EXCHANGE
    max_age_s = math.inf

    def __init__(self, settings: Settings, feeds: list[Feed]) -> None:
        self._url = settings.kraken_ws_url
        self._subscribe = kraken.subscribe_message(feeds)
        self._feeds = {feed.native: feed for feed in feeds}
        self.stale_after_s = settings.kraken_stale_after_s

    def url(self) -> str:
        return self._url

    async def on_open(self, ws: ClientConnection) -> None:
        """Subscribes with a snapshot: the last 50 trades of each pair, which overlap what an
        earlier connection already delivered. A short reconnect leaves no hole (a 60 s network
        cut in milestone 4 left none); gap repair fills longer ones."""
        await ws.send(self._subscribe)

    def parse(self, raw: str, received_us: int, url: str) -> Frame:
        return kraken.parse_frame(raw, received_us, self._feeds)


def adapter_for(
    exchange: str, settings: Settings, feeds: list[Feed]
) -> BinanceAdapter | KrakenAdapter:
    if exchange == binance.EXCHANGE:
        return BinanceAdapter(settings, feeds)
    if exchange == kraken.EXCHANGE:
        return KrakenAdapter(settings, feeds)
    raise ValueError(f"no producer for {exchange}")


async def run_feed(settings: Settings, exchange: str, publish: bool) -> None:
    feeds = feeds_for(exchange)
    client_id = f"producer-{exchange.lower()}"
    publisher = TradePublisher(settings, client_id=client_id) if publish else None
    recorder = Recorder(
        settings.recordings_dir, exchange, session_name("live" if publish else "record")
    )
    counts: Counter[str] = Counter()

    def on_trades(trades: tuple[Trade, ...]) -> None:
        for trade in trades:
            if publisher:
                publisher.publish(trade)
            counts[trade.symbol] += 1
            metrics.TRADES.labels(trade.exchange, trade.symbol).inc()
            metrics.LAST_TRADE_TIME.labels(trade.exchange, trade.symbol).set(trade.trade_time / 1e6)

    adapter = adapter_for(exchange, settings, feeds)
    stream = LiveStream(
        adapter,
        on_trades,
        recorder,
        max_age_s=adapter.max_age_s,
        stale_after_s=adapter.stale_after_s,
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
