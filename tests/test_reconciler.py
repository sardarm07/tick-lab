import json
from datetime import UTC, datetime
from decimal import Decimal

from ticklab.binance_rest import parse_klines as parse_binance_klines
from ticklab.candles import Candle, compare
from ticklab.reconciler import Result, summary
from ticklab.settings import Feed

BTC = Feed("BINANCE", "BTC/USDT", "BTCUSDT", 0)

# Binance's official SOLUSDT candle for 2026-10-10 00:00 UTC, from data-api.binance.vision
# (the same row is in flink/src/test/resources/fixtures/binance-2026-10-10T0000-3m.klines.csv).
KLINE = [
    1791590400000,
    "109.14000000",
    "109.19000000",
    "109.10000000",
    "109.11000000",
    "808.78900000",
    1791590459999,
    "88284.17808000",
    339,
    "639.65200000",
    "69820.23502000",
    "0",
]


def official() -> Candle:
    return parse_binance_klines(json.dumps([KLINE]))[datetime(2026, 10, 10, tzinfo=UTC)]


def test_klines_are_parsed_exactly():
    candle = official()
    assert candle.open == Decimal("109.14")
    assert candle.quote_volume == Decimal("88284.17808")
    assert candle.trade_count == 339
    assert candle.taker_buy_quote_volume == Decimal("69820.23502")


def test_equal_values_match_whatever_their_trailing_zeros():
    ours = Candle(
        Decimal("109.140000000000"),
        Decimal("109.19"),
        Decimal("109.1"),
        Decimal("109.11"),
        Decimal("808.789"),
        Decimal("88284.17808"),
        Decimal("639.652"),
        Decimal("69820.23502"),
        339,
    )
    assert compare(ours, official()) == []


def test_every_differing_field_is_reported():
    theirs = official()
    ours = Candle(
        theirs.open,
        theirs.high,
        theirs.low,
        Decimal("109.12"),
        theirs.volume,
        theirs.quote_volume,
        theirs.taker_buy_volume,
        theirs.taker_buy_quote_volume,
        338,
    )
    assert compare(ours, theirs) == ["close: 109.12 / 109.11000000", "trade_count: 338 / 339"]


def test_fields_the_exchange_does_not_publish_are_skipped():
    theirs = official()
    without_quote = Candle(**{**theirs.__dict__, "quote_volume": None})
    ours = Candle(**{**theirs.__dict__, "quote_volume": Decimal("1")})
    assert compare(ours, without_quote) == []


def test_a_missing_candle_counts_as_zero_trades():
    theirs = official()
    assert compare(None, theirs) == ["trade_count: 0 / 339"]
    empty = Candle(**{**theirs.__dict__, "trade_count": 0})
    assert compare(None, empty) == []


def test_summary_counts_matches_and_causes():
    minute = datetime(2026, 10, 10, tzinfo=UTC)
    results = [Result(BTC, minute, [], "", 1), Result(BTC, minute, ["x"], "producer down", 1)]
    table = summary(results)
    assert "| BTC/USDT | 2 | 1 | 1 | producer down 1 |" in table
