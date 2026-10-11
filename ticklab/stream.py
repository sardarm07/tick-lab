"""A WebSocket feed that stays connected.

- After an error, it reconnects with exponential backoff and jitter.
- It replaces its connection before the exchange's time limit, and as soon as the exchange
  announces a shutdown. The new connection opens before the old one closes ("make before
  break"), so no trade falls between the two. Trades delivered by both connections are
  duplicates, which the pipeline removes downstream.
- It sends a ping every 20 seconds and answers the server's pings; the websockets library
  does both. A connection that delivers nothing for `stale_after_s` is replaced.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from websockets.asyncio.client import ClientConnection, connect

from ticklab import metrics
from ticklab.model import Frame, Trade
from ticklab.recorder import Recorder

log = logging.getLogger(__name__)

# A connection that lasted this long counts as healthy, which resets the backoff.
HEALTHY_AFTER_S = 60.0
# During a handover, wait this long for the new connection's first trade...
HANDOVER_TIMEOUT_S = 10.0
# ...and keep both connections open this much longer, so the old one delivers everything
# that happened before the new one started.
HANDOVER_OVERLAP_S = 2.0


def now_us() -> int:
    return time.time_ns() // 1000


class Backoff:
    """Exponential backoff with "equal jitter": half of each delay is fixed, half random.

    The randomness spreads reconnects out, so many clients cut off at the same moment
    don't all come back at the same instant.
    """

    def __init__(self, base_s: float = 1.0, cap_s: float = 60.0, rng: random.Random | None = None):
        self.base_s = base_s
        self.cap_s = cap_s
        self.attempt = 0
        self._rng = rng or random.Random()

    def next(self) -> float:
        ceiling = min(self.cap_s, self.base_s * 2**self.attempt)
        self.attempt += 1
        return ceiling / 2 + self._rng.uniform(0, ceiling / 2)

    def reset(self) -> None:
        self.attempt = 0


class Adapter(Protocol):
    """What a feed needs from one exchange."""

    exchange: str

    def url(self) -> str: ...

    async def on_open(self, ws: ClientConnection) -> None: ...

    def parse(self, raw: str, received_us: int, url: str) -> Frame: ...


@dataclass(eq=False)
class Connection:
    conn_id: int
    url: str
    ws: ClientConnection
    rotate_at: float
    opened_at: float = field(default_factory=time.monotonic)
    last_frame_at: float = field(default_factory=time.monotonic)
    shutdown_announced: asyncio.Event = field(default_factory=asyncio.Event)
    first_trade: asyncio.Event = field(default_factory=asyncio.Event)
    reader: asyncio.Task | None = None


class LiveStream:
    def __init__(
        self,
        adapter: Adapter,
        on_trades: Callable[[tuple[Trade, ...]], None] | None,
        recorder: Recorder | None,
        max_age_s: float,
        stale_after_s: float,
        backoff: Backoff | None = None,
    ) -> None:
        self._adapter = adapter
        self._exchange = adapter.exchange
        self._on_trades = on_trades
        self._recorder = recorder
        self._max_age_s = max_age_s
        self._stale_after_s = stale_after_s
        self._backoff = backoff or Backoff()
        self._ids = itertools.count(1)

    async def run(self, stop: asyncio.Event) -> None:
        conn: Connection | None = None
        reason = "startup"
        try:
            while not stop.is_set():
                if conn is None:
                    conn = await self._connect(reason, stop)
                    if conn is None:
                        return
                event = await self._watch(conn, stop)
                if event == "stop":
                    return
                if event in ("planned", "server_shutdown"):
                    conn = await self._replace(conn, event)
                    continue
                # "error" or "stale": the connection is gone or can't be trusted.
                lasted = time.monotonic() - conn.opened_at
                await self._close(conn, event)
                conn = None
                if lasted >= HEALTHY_AFTER_S:
                    self._backoff.reset()
                reason = event
                await self._pause(stop)
        finally:
            if conn is not None:
                await self._close(conn, "stop")

    async def _connect(self, reason: str, stop: asyncio.Event) -> Connection | None:
        while not stop.is_set():
            try:
                return await self._open(reason)
            except Exception as exc:
                log.warning("%s: could not connect: %r", self._exchange, exc)
                reason = "error"
                await self._pause(stop)
        return None

    async def _open(self, reason: str) -> Connection:
        url = self._adapter.url()
        ws = await connect(url, open_timeout=15, ping_interval=20, ping_timeout=20, close_timeout=5)
        try:
            await self._adapter.on_open(ws)
        except BaseException:
            await ws.close()
            raise
        conn = Connection(next(self._ids), url, ws, rotate_at=time.monotonic() + self._max_age_s)
        if self._recorder:
            self._recorder.open_conn(conn.conn_id, now_us(), url)
        conn.reader = asyncio.create_task(self._read(conn))
        metrics.CONNECTS.labels(self._exchange, reason).inc()
        metrics.CONNECTED.labels(self._exchange).inc()
        log.info("%s: connection %d open (%s)", self._exchange, conn.conn_id, reason)
        return conn

    async def _read(self, conn: Connection) -> None:
        async for message in conn.ws:
            received = now_us()
            conn.last_frame_at = time.monotonic()
            raw = message if isinstance(message, str) else message.decode()
            self._handle(conn, raw, received)

    def _handle(self, conn: Connection, raw: str, received: int) -> None:
        if self._recorder:
            self._recorder.frame(conn.conn_id, received, raw)
            metrics.RECORDED_LINES.labels(self._exchange).inc()
        try:
            frame = self._adapter.parse(raw, received, conn.url)
        except Exception:
            metrics.PARSE_ERRORS.labels(self._exchange).inc()
            log.exception("%s: could not normalise %.300s", self._exchange, raw)
            return
        if frame.shutdown:
            metrics.FRAMES.labels(self._exchange, "shutdown").inc()
            log.warning(
                "%s: connection %d: server shutdown announced", self._exchange, conn.conn_id
            )
            conn.shutdown_announced.set()
        elif frame.block_trade:
            metrics.FRAMES.labels(self._exchange, "block_trade").inc()
        elif frame.trades:
            metrics.FRAMES.labels(self._exchange, "trade").inc()
            conn.first_trade.set()
            if self._on_trades:
                self._on_trades(frame.trades)
        else:
            metrics.FRAMES.labels(self._exchange, "other").inc()

    async def _watch(self, conn: Connection, stop: asyncio.Event) -> str:
        """Wait until something needs doing about the connection, and say what."""
        assert conn.reader is not None
        while True:
            if stop.is_set():
                return "stop"
            if conn.reader.done():
                return "error"
            if conn.shutdown_announced.is_set():
                return "server_shutdown"
            now = time.monotonic()
            if now >= conn.rotate_at:
                return "planned"
            if now - conn.last_frame_at >= self._stale_after_s:
                return "stale"
            if self._recorder:
                self._recorder.tick(now_us())
            await asyncio.wait([conn.reader], timeout=0.5)

    async def _replace(self, old: Connection, reason: str) -> Connection:
        """Open a new connection, let the two overlap, then close the old one."""
        try:
            new = await self._open(reason)
        except Exception as exc:
            delay = self._backoff.next()
            log.warning(
                "%s: replacement failed (%r); keeping connection %d, retrying in %.1fs",
                self._exchange,
                exc,
                old.conn_id,
                delay,
            )
            old.shutdown_announced.clear()
            old.rotate_at = time.monotonic() + delay
            return old
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(new.first_trade.wait(), timeout=HANDOVER_TIMEOUT_S)
        await asyncio.sleep(HANDOVER_OVERLAP_S)
        await self._close(old, reason)
        self._backoff.reset()
        return new

    async def _close(self, conn: Connection, reason: str) -> None:
        with contextlib.suppress(Exception):
            await conn.ws.close()
        if conn.reader is not None:
            outcome = (await asyncio.gather(conn.reader, return_exceptions=True))[0]
            if isinstance(outcome, Exception):
                log.warning("%s: connection %d ended: %r", self._exchange, conn.conn_id, outcome)
        if self._recorder:
            self._recorder.close_conn(conn.conn_id, now_us(), reason)
        metrics.CONNECTED.labels(self._exchange).dec()
        log.info("%s: connection %d closed (%s)", self._exchange, conn.conn_id, reason)

    async def _pause(self, stop: asyncio.Event) -> None:
        delay = self._backoff.next()
        log.info("%s: reconnecting in %.1fs", self._exchange, delay)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)
