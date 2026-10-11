"""Gap repair: fetches the trades a feed is missing and publishes them late.

The core job reports a gap on the `gaps` topic once it is sure the trades are missing, not just
reordered (DETECTED). Gap repair asks the exchange's REST API for those trade IDs and publishes
them to `trades` with source REST. They arrive late, but within the candles' lateness budget, so
the affected candles are emitted again, complete. Then it reports the gap REPAIRED, or FAILED,
on the `gaps` topic under the same key.

- Binance: `historicalTrades` from an ID. Binance allows 6,000 request weight a minute per IP
  and the endpoint costs 25. Gap repair spends at most 600 a minute (24 requests) by default
  (GAP_REPAIR_WEIGHT_PER_MINUTE). On 429 or 418 it sends no more Binance requests until the
  response's Retry-After has passed; gaps detected meanwhile fail. Against the fake exchange,
  tests lift the budget.
- Kraken: `Trades` pages from a time, not an ID. The time comes from the trade just before the
  gap, which is already stored in ClickHouse. At most one request a second.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import threading
import time
from collections import deque
from datetime import UTC, datetime

import httpx
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Histogram

from ticklab import avro, binance_rest, kraken, kraken_rest
from ticklab.clickhouse import ClickHouse
from ticklab.kafka import TradePublisher
from ticklab.model import Trade
from ticklab.settings import Feed, Settings
from ticklab.stream import now_us

log = logging.getLogger(__name__)

WEIGHT_PER_MINUTE = 600
HISTORICAL_TRADES_WEIGHT = 25
KRAKEN_REQUEST_INTERVAL_S = 1.0

GAPS = Counter("ticklab_gap_repair_gaps", "Gaps handled, by outcome", ["symbol", "outcome"])
FETCHED = Counter("ticklab_gap_repair_trades", "Missing trades fetched and published", ["symbol"])
DELAY = Histogram(
    "ticklab_gap_repair_seconds",
    "From the gap being detected to its trades being published",
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)


class WeightLimiter:
    """Spends request weight evenly: never more than the budget in any 60 s window."""

    def __init__(self, per_minute: int) -> None:
        self._budget = per_minute
        self._spent: deque[tuple[float, int]] = deque()

    def spend(self, weight: int) -> None:
        while True:
            now = time.monotonic()
            while self._spent and now - self._spent[0][0] >= 60:
                self._spent.popleft()
            if sum(w for _, w in self._spent) + weight <= self._budget:
                self._spent.append((now, weight))
                return
            time.sleep(60 - (now - self._spent[0][0]))


class GapRepair:
    def __init__(self, settings: Settings, feeds: list[Feed]) -> None:
        self._settings = settings
        self._feeds = {(feed.exchange, feed.symbol): feed for feed in feeds}
        self._registry = avro.Registry(settings.schema_registry_url)
        self._client = httpx.Client(timeout=30)
        budget = int(os.environ.get("GAP_REPAIR_WEIGHT_PER_MINUTE", WEIGHT_PER_MINUTE))
        self._limiter = WeightLimiter(budget)
        self._binance_blocked_until = 0.0  # monotonic time; set by 429 and 418 answers
        self._kraken_next_request = 0.0  # monotonic time
        self._clickhouse = ClickHouse(settings)
        self._trades = TradePublisher(settings, client_id="gap-repair")
        self._status = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "client.id": "gap-repair-status",
                "acks": "all",
            }
        )
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": "gap-repair",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

    def run(self, stop: threading.Event) -> None:
        self._consumer.subscribe(["gaps"])
        try:
            while not stop.is_set():
                message = self._consumer.poll(1.0)
                if message is None or message.error():
                    continue
                gap = self._registry.decode(message.value())
                if gap["status"] == "DETECTED":
                    self.repair(gap)
        finally:
            self._consumer.close()
            self._trades.flush()
            self._status.flush()

    def repair(self, gap: dict) -> None:
        feed = self._feeds.get((gap["exchange"], gap["symbol"]))
        first, last = gap["first_missing_id"], gap["last_missing_id"]
        if feed is None:
            log.warning("no repair source for %s %s", gap["exchange"], gap["symbol"])
            return
        wanted = last - first + 1
        log.info("%s: fetching %d missing trades %d..%d", feed.symbol, wanted, first, last)
        try:
            if feed.exchange == kraken.EXCHANGE:
                found = self._fetch_kraken(feed, first, last)
            else:
                found = self._fetch_binance(feed, first, last)
        except (httpx.HTTPError, RuntimeError) as exc:
            log.warning("%s: repair of %d..%d failed: %r", feed.symbol, first, last, exc)
            GAPS.labels(feed.symbol, "failed").inc()
            self._report(gap, "FAILED", 0, repr(exc)[:200])
            return
        received = now_us()
        for trade in found:
            self._trades.publish(dataclasses.replace(trade, received_time=received))
        self._trades.flush(30)
        FETCHED.labels(feed.symbol).inc(len(found))
        detected = gap["updated_at"]
        DELAY.observe(max(0.0, time.time() - detected.timestamp()))
        outcome = "REPAIRED" if len(found) == wanted else "FAILED"
        GAPS.labels(feed.symbol, outcome.lower()).inc()
        self._report(gap, outcome, len(found), f"{len(found)} of {wanted} trades found")
        log.info("%s: %d..%d %s (%d of %d)", feed.symbol, first, last, outcome, len(found), wanted)

    def _fetch_binance(self, feed: Feed, first: int, last: int) -> list[Trade]:
        wait = self._binance_blocked_until - time.monotonic()
        if wait > 0:
            raise RuntimeError(f"Binance rate limit: no requests for another {wait:.0f} s")
        found: list[Trade] = []
        next_id = first
        while next_id <= last:
            self._limiter.spend(HISTORICAL_TRADES_WEIGHT)
            try:
                page = binance_rest.historical_trades(
                    self._client,
                    self._settings.binance_rest_url,
                    feed,
                    next_id,
                    min(1000, last - next_id + 1),
                )
            except binance_rest.RateLimited as exc:
                self._binance_blocked_until = time.monotonic() + exc.retry_after_s
                raise
            if not page:
                break
            found += [t for t in page if t.trade_id <= last]
            next_id = page[-1].trade_id + 1
        return found

    def _fetch_kraken(self, feed: Feed, first: int, last: int) -> list[Trade]:
        """Kraken pages trades by time: start at the trade before the gap and page forward.

        `since` is in nanoseconds. Stored times are microseconds: from the WebSocket, Kraken's
        own nanosecond times truncated; from REST, possibly 1 µs off, since REST sends times as
        binary floating-point seconds. Starting 1 µs before the stored time covers both.
        """
        since = (self._time_before(feed, first) - 1) * 1000
        return kraken_rest.trades_between_ids(
            self._client, self._settings.kraken_rest_url, feed, since, first, last, self._pace
        )

    def _pace(self) -> None:
        """At most one Kraken request a second."""
        pause = self._kraken_next_request - time.monotonic()
        if pause > 0:
            time.sleep(pause)
        self._kraken_next_request = time.monotonic() + KRAKEN_REQUEST_INTERVAL_S

    def _time_before(self, feed: Feed, first: int, tries: int = 30) -> int:
        """Trade time, in microseconds, of the newest stored trade before ID `first`.

        ClickHouse reads `trades` on its own, in blocks a few seconds apart, and may not have
        stored that trade yet when the gap is reported, so the lookup is retried every second
        for half a minute."""
        for attempt in range(tries):
            rows = self._clickhouse.rows(
                """SELECT toUnixTimestamp64Micro(trade_time) AS t FROM ticklab.trades
                WHERE exchange = {exchange:String} AND symbol = {symbol:String}
                  AND trade_id < {first:Int64}
                ORDER BY trade_id DESC LIMIT 1""",
                exchange=feed.exchange,
                symbol=feed.symbol,
                first=first,
            )
            if rows:
                return int(rows[0]["t"])
            if attempt < tries - 1:
                time.sleep(1)
        raise RuntimeError(f"no stored trade before ID {first}, so no time to start from")

    def _report(self, gap: dict, status: str, found: int, detail: str) -> None:
        schema_id, schema = self._registry.latest("gaps-value")
        record = {
            **gap,
            "status": status,
            "trades_found": found,
            "detail": detail,
            "updated_at": datetime.now(UTC),
        }
        key = f"{gap['exchange']}:{gap['symbol']}:{gap['first_missing_id']}".encode()
        self._status.produce("gaps", key=key, value=avro.encode(record, schema_id, schema))
        self._status.flush(10)
