from decimal import Decimal

from ticklab import whales
from ticklab.model import Side, Source, Trade
from ticklab.whales import EDGES, VALUE_UNIT, WhaleTracker

S = 1_791_700_020  # a whole minute, in seconds since the epoch


def trade(trade_id: int, at_us: int, price: str, quantity: str, buy: bool = True) -> Trade:
    """A Binance BTC trade `at_us` microseconds after S."""
    return Trade(
        "BINANCE",
        "BTC/USDT",
        "BTC",
        "USDT",
        trade_id,
        Decimal(price),
        Decimal(quantity),
        Side.BUY if buy else Side.SELL,
        S * 1_000_000 + at_us,
        None,
        Source.HISTORY,
    )


def calm(minutes: int) -> list[Trade]:
    """One $100 order a minute, for `minutes` minutes."""
    return [trade(m + 1, m * 60_000_000, "100", "1") for m in range(minutes)]


def run(*trades: Trade) -> list[whales.Whale]:
    tracker = WhaleTracker()
    for t in trades:
        tracker.add(t)
    return tracker.finish()


def test_bucket_edges_are_exact_powers_of_one_point_zero_five():
    assert EDGES[:3] == [VALUE_UNIT, 105 * VALUE_UNIT // 100, 11025 * VALUE_UNIT // 10000]
    assert whales.bucket(VALUE_UNIT - 1) == 0
    assert whales.bucket(105 * VALUE_UNIT // 100) == 2


def test_fills_of_one_taker_order_are_grouped():
    big = 32 * 60_000_000
    found = run(
        *calm(32),
        # One order of four fills: same time, same side, consecutive IDs.
        *(trade(100 + i, big, "100", "2.5") for i in range(4)),
        trade(104, big, "100", "1", buy=False),  # another side: another order
    )
    (whale,) = found
    assert (whale.first_id, whale.last_id, whale.fills) == (100, 103, 4)
    assert whale.value == Decimal(1000) and whale.price == Decimal(100)


def test_a_whale_needs_thirty_minutes_of_history_and_an_order_above_the_percentile():
    early = run(*calm(20), trade(100, 20 * 60_000_000, "100", "50"))
    assert early == []
    (whale,) = run(*calm(31), trade(100, 31 * 60_000_000 + 30_000_000, "100", "10"))
    # 31 orders of $100 in the window: the 99.9th percentile is in the $100 bucket, and its top
    # is the threshold.
    assert whale.window_orders == 31
    assert whale.threshold == Decimal(EDGES[whales.bucket(100 * VALUE_UNIT)]) / VALUE_UNIT


def test_orders_of_the_whales_own_minute_are_not_in_its_window():
    at = 31 * 60_000_000
    found = run(*calm(31), trade(100, at, "100", "10"), trade(101, at + 1_000_000, "100", "10"))
    assert [w.window_orders for w in found] == [31, 31]


def test_aftershocks_are_signed_by_the_whales_side():
    t = 31 * 60_000_000 + 30_000_000
    (whale,) = run(
        *calm(31),
        trade(100, t, "100", "10"),  # a buy whale at 100
        trade(101, t + 10_000_000, "101", "1"),  # at +10 s: up 1%, the way it pushed
        trade(102, t + 25_000_000, "99.5", "1"),  # the last price at or before +30 s
        trade(103, t + 60_000_001, "100", "1"),  # just after +60 s: not yet seen at +60
        trade(104, t + 200_000_000, "98", "1"),
    )
    assert whale.moves == (Decimal(100), Decimal(-50), Decimal(-50), Decimal(0))
