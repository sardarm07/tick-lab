import threading
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from ticklab import binance_rest, gap_repair
from ticklab.gap_repair import MAX_TRADES, GapRepair, WeightLimiter
from ticklab.model import Side, Source, Trade
from ticklab.settings import Feed

BTC = Feed("BINANCE", "BTC/USDT", "BTCUSDT", 0)


def trade(trade_id: int) -> Trade:
    return Trade(
        "BINANCE",
        "BTC/USDT",
        "BTC",
        "USDT",
        trade_id,
        Decimal(1),
        Decimal(1),
        Side.BUY,
        1_791_700_000_000_000,
        None,
        Source.REST,
    )


def gap(first: int, last: int) -> dict:
    return {
        "exchange": "BINANCE",
        "symbol": "BTC/USDT",
        "first_missing_id": first,
        "last_missing_id": last,
        "status": "DETECTED",
        "updated_at": datetime.now(UTC),
    }


class Publisher:
    def __init__(self) -> None:
        self.published: list[Trade] = []

    def publish(self, trade: Trade) -> None:
        self.published.append(trade)

    def flush(self, timeout: float | None = None) -> int:
        return 0


def repairer(pages: list) -> tuple[GapRepair, list]:
    """Gap repair without its clients: Binance answers with `pages`, where an exception is
    raised in its turn, and reports are collected."""
    repair = object.__new__(GapRepair)
    repair._feeds = {(BTC.exchange, BTC.symbol): BTC}
    repair._trades = Publisher()
    reports: list = []
    repair._report = lambda gap, status, found, detail: reports.append((status, found, detail))

    def fetch(feed: Feed, first: int, last: int):
        for page in pages:
            if isinstance(page, Exception):
                raise page
            yield page

    repair._fetch_binance = fetch
    return repair, reports


def test_the_limiter_waits_once_the_minute_budget_is_spent(monkeypatch):
    clock = {"now": 1000.0}
    slept = []
    monkeypatch.setattr(gap_repair.time, "monotonic", lambda: clock["now"])

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(gap_repair.time, "sleep", sleep)
    limiter = WeightLimiter(per_minute=100)
    for _ in range(4):
        limiter.spend(25)  # 100 in the first minute: no waiting
    assert slept == []
    clock["now"] += 10
    limiter.spend(25)  # the oldest spend leaves the window 60 s after it was made
    assert slept == [50.0]


def test_binance_rate_limit_answers_carry_their_retry_after():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "7"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    feed = Feed("BINANCE", "BTC/USDT", "BTCUSDT", 0)
    with pytest.raises(binance_rest.RateLimited) as caught:
        binance_rest.historical_trades(client, "http://binance", feed, 1, 10)
    assert caught.value.retry_after_s == 7


def test_a_complete_repair_is_reported_repaired():
    repair, reports = repairer([[trade(1), trade(2)], [trade(3)]])
    repair.repair(gap(1, 3))
    assert [t.trade_id for t in repair._trades.published] == [1, 2, 3]
    assert reports == [("REPAIRED", 3, "3 of 3 trades found")]


def test_pages_are_published_as_they_come_so_a_failure_keeps_them():
    repair, reports = repairer([[trade(1), trade(2)], [trade(3)], RuntimeError("cut")])
    repair.repair(gap(1, 5))
    assert [t.trade_id for t in repair._trades.published] == [1, 2, 3]
    assert reports == [("FAILED", 3, "3 of 5 found, then RuntimeError('cut')")]


def test_a_gap_from_downtime_fails_at_once_without_a_request():
    repair, reports = repairer([AssertionError("no request should be made")])
    repair.repair(gap(1, MAX_TRADES + 1))
    assert repair._trades.published == []
    detail = f"{MAX_TRADES + 1} missing, more than the {MAX_TRADES} fetched"
    assert reports == [("FAILED", 0, detail)]


def test_a_gap_counts_as_handled_only_once_its_repair_is_over():
    events: list[str] = []
    stop = threading.Event()

    class Message:
        def __init__(self, status: str) -> None:
            self.status = status

        def value(self) -> str:
            return self.status

        def error(self) -> None:
            return None

    class Consumer:
        def __init__(self, statuses: list[str]) -> None:
            self.messages = [Message(s) for s in statuses]

        def subscribe(self, topics: list[str]) -> None:
            pass

        def poll(self, timeout: float):
            if not self.messages:
                stop.set()
                return None
            return self.messages.pop(0)

        def store_offsets(self, message: Message) -> None:
            events.append(f"stored {message.status}")

        def close(self) -> None:
            pass

    class Registry:
        def decode(self, value: str) -> dict:
            return {"status": value}

    repair = object.__new__(GapRepair)
    repair._consumer = Consumer(["DETECTED", "CLOSED"])
    repair._registry = Registry()
    repair._trades = repair._status = Publisher()
    repair.repair = lambda gap: events.append("repaired")
    repair.run(stop)
    assert events == ["repaired", "stored DETECTED", "stored CLOSED"]
