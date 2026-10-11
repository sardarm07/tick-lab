"""The signals study on made-up trades: the same rules as the Flink job, in plain Python."""

from decimal import Decimal

from ticklab import signal_study
from ticklab.model import Side, Source, Trade


def trade(exchange: str, symbol: str, trade_id: int, ms: int, price: str) -> Trade:
    base, quote = symbol.split("/")
    return Trade(
        exchange=exchange,
        symbol=symbol,
        base=base,
        quote=quote,
        trade_id=trade_id,
        price=Decimal(price),
        quantity=Decimal("1"),
        taker_side=Side.BUY,
        trade_time=ms * 1000,
        received_time=None,
        source=Source.HISTORY,
    )


def test_spreads_follow_the_flink_jobs_rules():
    # The same scenario as SignalsPipelineTest.java, and the same expected prices.
    trades = [
        trade("KRAKEN", "USDT/USD", 1, 500, "0.9990"),
        trade("KRAKEN", "BTC/USD", 10, 700, "83000.0"),
        trade("BINANCE", "BTC/USDT", 100, 900, "83050.00"),
        trade("KRAKEN", "BTC/USD", 11, 1_100, "83010.0"),
        trade("KRAKEN", "BTC/USD", 13, 1_150, "83020.0"),
        trade("KRAKEN", "BTC/USD", 12, 1_200, "83015.0"),  # later, but not the highest ID
        trade("BINANCE", "BTC/USDT", 101, 1_500, "83060.00"),
        trade("KRAKEN", "USDT/USD", 2, 2_200, "0.9995"),
        trade("BINANCE", "BTC/USDT", 102, 2_500, "83070.00"),
        trade("BINANCE", "BTC/USDT", 103, 14_000, "83080.00"),
    ]
    events = ((t.trade_time, t) for t in trades)
    rows = list(signal_study.spreads(signal_study.second_bars(events)))
    seen = [(r.second, r.binance_price, r.kraken_price, r.usdt_usd, r.kraken_age_s) for r in rows]
    assert seen == [
        (0, Decimal("83050.00"), Decimal("83000.0"), Decimal("0.9990"), 0),
        (1, Decimal("83060.00"), Decimal("83020.0"), Decimal("0.9990"), 0),
        (2, Decimal("83070.00"), Decimal("83020.0"), Decimal("0.9990"), 1),
    ]
    converted = (Decimal("83000.0") / Decimal("0.9990")).quantize(Decimal("1e-12"))
    assert rows[0].kraken_price_usdt == converted


def test_divergence_alerts_open_after_holding_and_close_after_holding_below():
    times = list(range(0, 100))
    bps = [12.0] * 60 + [3.0] * 40
    assert signal_study.divergence_alerts(times, bps, open_bps=10, close_bps=5) == [(30, 90)]


def test_volume_ratios_count_minutes_without_trades_as_zero():
    minutes = {m * 60: signal_study.Minute(Decimal("1"), m, Decimal("2")) for m in range(0, 60, 2)}
    minutes[60 * 60] = signal_study.Minute(Decimal("1"), 60, Decimal("9"))
    assert signal_study.volume_ratios(minutes) == [9.0]
