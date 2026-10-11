from decimal import Decimal

from ticklab import chaos
from ticklab.model import Side, Source, Trade


def trades(symbol: str, count: int, first_id: int = 1000) -> list[tuple[int, Trade]]:
    out = []
    for n in range(count):
        time = 1_791_590_400_000_000 + n * 50_000  # one trade every 50 ms
        trade = Trade(
            "BINANCE",
            symbol,
            symbol[:3],
            "USDT",
            first_id + n,
            Decimal("1.5"),
            Decimal("2"),
            Side.BUY,
            time,
            None,
            Source.HISTORY,
        )
        out.append((time, trade))
    return out


def test_the_same_seed_gives_the_same_chaos():
    events = trades("BTC/USDT", 2000) + trades("SOL/USDT", 1000, first_id=5000)
    events.sort(key=lambda e: e[0])
    first, injected_a = chaos.apply(events, seed=3)
    second, injected_b = chaos.apply(events, seed=3)
    assert first == second
    assert injected_a == injected_b
    other, _ = chaos.apply(events, seed=4)
    assert other != first


def test_dropped_ranges_never_arrive_and_everything_else_does():
    events = trades("BTC/USDT", 3000)
    arrivals, injected = chaos.apply(events, seed=9)
    ranges = injected.dropped["BTC/USDT"]
    assert len(ranges) == chaos.DROPPED_RANGES
    dropped = {i for first, last in ranges for i in range(first, last + 1)}
    assert all(last - first + 1 == chaos.DROPPED_LENGTH for first, last in ranges)
    arrived = [trade.trade_id for _, trade in arrivals]
    assert dropped.isdisjoint(arrived)
    assert set(arrived) == {t.trade_id for _, t in events} - dropped
    assert len(arrived) == len(events) - len(dropped) + injected.duplicates


def test_arrivals_are_in_order_and_never_before_the_trade():
    arrivals, injected = chaos.apply(trades("ETH/USDT", 3000), seed=5)
    times = [arrival for arrival, _ in arrivals]
    assert times == sorted(times)
    assert all(arrival >= trade.trade_time for arrival, trade in arrivals)
    late = [a - t.trade_time for a, t in arrivals if a - t.trade_time > 2_000_000]
    assert len(late) >= injected.delayed  # delayed trades and their repeats
    assert all(delay <= 121_000_000 for delay in late)  # all within the lateness budget
