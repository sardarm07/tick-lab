from collections import Counter
from decimal import Decimal

from ticklab import study
from ticklab.model import Side, Source, Trade


def trade(trade_id: int, time: int, symbol: str = "BTC/USDT") -> Trade:
    return Trade(
        "BINANCE",
        symbol,
        "BTC",
        "USDT",
        trade_id,
        Decimal(1),
        Decimal(1),
        Side.BUY,
        time,
        None,
        Source.HISTORY,
    )


def test_percentile_on_a_histogram():
    histogram = Counter({0: 90, 5: 9, 100: 1})
    assert study.percentile(histogram, 0.5) == 0
    assert study.percentile(histogram, 0.99) == 5
    assert study.percentile(histogram, 1.0) == 100


def test_disorder_counts_late_arrivals_repeats_and_missing_ids():
    arrivals = [  # (received, trade): trade 3 arrives after 4, trade 4 arrives twice
        (1_000, trade(1, 100)),
        (2_000, trade(2, 1_100)),
        (3_000, trade(4, 2_600)),
        (3_500, trade(3, 2_100)),
        (4_000, trade(4, 2_600)),
        (5_000, trade(6, 4_000)),
    ]
    (feed,) = study.measure_disorder(arrivals).values()
    assert feed.trades == 5
    assert feed.duplicates == 1
    assert feed.ids_out_of_order == 1
    assert feed.late == 1
    assert feed.lateness_ms == Counter({1: 1})  # 500 µs behind, rounded up to 1 ms
    assert feed.missing_ids == 1  # trade 5


def test_gaps_between_trades_and_busiest_hour():
    hour = 3_600_000_000
    trades = [trade(1, 0), trade(2, 1_000), trade(3, 61_000_000), trade(5, hour + 1)]
    (feed,) = study.measure_gaps(trades).values()
    assert feed.trades == 4
    assert feed.id_breaks == 1
    assert feed.gap_ms == Counter({1: 1, 60_999: 1, 3_539_000: 1})
    assert feed.longest == (hour + 1 - 61_000_000, hour + 1)
    assert feed.per_hour.most_common(1)[0][1] == 3
