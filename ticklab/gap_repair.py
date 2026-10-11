"""Gap repair: fetches the trades a feed is missing and publishes them late.

The core job reports a gap on the `gaps` topic once it is sure the trades are missing, not just
reordered (DETECTED). Gap repair asks the exchange's REST API for those trade IDs and publishes
them to `trades` with source REST. They arrive late, but within the candles' lateness budget, so
the affected candles are emitted again, complete. Then it reports the gap REPAIRED, or FAILED,
on the `gaps` topic under the same key.

Binance allows 6,000 request weight a minute per IP and `historicalTrades` costs 25. Gap repair
spends at most 600 a minute (24 requests) by default (GAP_REPAIR_WEIGHT_PER_MINUTE), and on 429
or 418 stops and waits. Against the fake exchange, tests lift the budget.
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

from ticklab import avro, binance, binance_rest
from ticklab.kafka import TradePublisher
from ticklab.settings import Feed, Settings
from ticklab.stream import now_us

log = logging.getLogger(__name__)

WEIGHT_PER_MINUTE = 600
HISTORICAL_TRADES_WEIGHT = 25

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
        if feed is None or feed.exchange != binance.EXCHANGE:
            log.warning("no repair source for %s %s", gap["exchange"], gap["symbol"])
            return
        wanted = last - first + 1
        log.info("%s: fetching %d missing trades %d..%d", feed.symbol, wanted, first, last)
        try:
            found = []
            next_id = first
            while next_id <= last:
                self._limiter.spend(HISTORICAL_TRADES_WEIGHT)
                page = binance_rest.historical_trades(
                    self._client,
                    self._settings.binance_rest_url,
                    feed,
                    next_id,
                    min(1000, last - next_id + 1),
                )
                if not page:
                    break
                found += [t for t in page if t.trade_id <= last]
                next_id = page[-1].trade_id + 1
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
