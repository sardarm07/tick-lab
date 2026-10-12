from decimal import Decimal

from ticklab import lead_lag
from ticklab.lead_lag import LeadLag, Move
from ticklab.model import Side, Source, Trade

S = 1_791_700_000  # a whole second, in seconds since the epoch


def trade(exchange: str, trade_id: int, at: float, price: str, base: str = "BTC") -> Trade:
    """A trade at `at` seconds after S."""
    quote = "USDT" if exchange == "BINANCE" else "USD"
    return Trade(
        exchange,
        f"{base}/{quote}",
        base,
        quote,
        trade_id,
        Decimal(price),
        Decimal(1),
        Side.BUY,
        round((S + at) * 1_000_000),
        None,
        Source.HISTORY,
    )


def events(*trades: Trade) -> list[tuple[int, Trade]]:
    return sorted(((t.trade_time, t) for t in trades), key=lambda e: e[0])


def test_a_second_closes_at_its_highest_trade_id_against_the_previous_close():
    found = list(
        lead_lag.seconds(
            events(
                trade("BINANCE", 1, 0.1, "100"),
                trade("BINANCE", 3, 1.2, "101"),  # the second's close: the highest ID
                trade("BINANCE", 2, 1.2, "99"),
            )
        )
    )
    assert len(found) == 1
    assert found[0].second == S + 1
    assert found[0].reference == Decimal(100)
    assert found[0].bps == Decimal(100)  # 101 against 100
    assert [time for time, _ in found[0].trades] == [S * 1_000_000 + 1_200_000] * 2


def test_the_reference_carries_over_quiet_seconds_until_it_is_stale():
    found = list(
        lead_lag.seconds(
            events(
                trade("KRAKEN", 1, 0.5, "100"),
                trade("KRAKEN", 2, 10.5, "101"),  # 10 s later: still fresh
                trade("KRAKEN", 3, 21.5, "102"),  # 11 s after that: stale, only a new reference
                trade("KRAKEN", 4, 22.5, "103"),
            )
        )
    )
    assert [(s.second - S, s.reference) for s in found] == [(10, 100), (22, 102)]


def test_usdt_usd_is_a_rate_not_a_market():
    found = lead_lag.seconds(
        events(trade("KRAKEN", 1, 0.5, "1", "USDT"), trade("KRAKEN", 2, 1.5, "2", "USDT"))
    )
    assert list(found) == []


def test_a_move_is_timed_by_its_first_trade_beyond_the_threshold_in_its_direction():
    prices = ("100.06", "99.97", "99.9", "95")
    trades = tuple((10 * (i + 1), Decimal(price)) for i, price in enumerate(prices))
    second = lead_lag.Second("BINANCE", "BTC", S, Decimal(100), Decimal(-500), trades)
    m = lead_lag.move(second, Decimal(5))
    # 100.06 is beyond 5 bps, but upwards; 99.97 is only 3 bps down; 99.9 is the first beyond
    assert m == Move("BINANCE", "BTC", S, 30, False, Decimal(-500))


def test_a_close_exactly_at_the_threshold_is_no_move():
    trades = ((1, Decimal("100.05")),)
    second = lead_lag.Second("BINANCE", "BTC", S, Decimal(100), Decimal(5), trades)
    assert lead_lag.move(second, Decimal(5)) is None


def m(exchange: str, at_us: int, up: bool = True) -> Move:
    """A move `at_us` microseconds after S."""
    time_us = S * 1_000_000 + at_us
    return Move(exchange, "BTC", time_us // 1_000_000, time_us, up, Decimal(10 if up else -10))


def matched(*moves: Move) -> list[LeadLag]:
    return lead_lag.match(sorted(moves, key=lead_lag.order))


def test_moves_pair_with_the_earliest_unpaired_move_of_the_other_exchange():
    rows = matched(m("BINANCE", 200_000), m("BINANCE", 700_000), m("KRAKEN", 1_500_000))
    pairs = [r for r in rows if r.paired]
    assert [(r.leader, r.lead_us) for r in pairs] == [("BINANCE", 1_300_000)]
    unpaired = [r for r in rows if not r.paired]
    assert [(r.leader, r.leader_time_us - S * 1_000_000) for r in unpaired] == [
        ("BINANCE", 700_000)
    ]


def test_only_moves_in_the_same_direction_within_two_seconds_pair():
    rows = matched(
        m("KRAKEN", 0),
        m("BINANCE", 500_000, up=False),  # opposite direction
        m("BINANCE", 2_000_000),  # exactly 2 s after Kraken's: within the window
        m("KRAKEN", 5_000_000),
        m("BINANCE", 7_000_001),  # just over 2 s
    )
    pairs = [(r.leader, r.lead_us) for r in rows if r.paired]
    assert pairs == [("KRAKEN", 2_000_000)]
    assert sum(1 for r in rows if not r.paired) == 3


def test_moves_on_the_same_exchange_never_pair():
    rows = matched(m("BINANCE", 100_000), m("BINANCE", 200_000))
    assert [r.paired for r in rows] == [False, False]


def test_moves_at_the_same_moment_are_a_tie():
    rows = matched(m("KRAKEN", 300_000), m("BINANCE", 300_000))
    assert [(r.paired, r.lead_us) for r in rows] == [(True, 0)]
