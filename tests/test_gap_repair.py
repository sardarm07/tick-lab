import httpx
import pytest

from ticklab import binance_rest, gap_repair
from ticklab.gap_repair import WeightLimiter
from ticklab.settings import Feed


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
