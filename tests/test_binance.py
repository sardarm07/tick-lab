"""The Binance normaliser on frames in the documented format. Real recorded frames are
covered by test_golden_binance.py."""

import json
from datetime import date
from decimal import Decimal

import pytest

from ticklab import binance
from ticklab.model import Side, Source
from ticklab.settings import Feed

BTC = Feed("BINANCE", "BTC/USDT", "BTCUSDT", 0)
FEEDS = {"BTCUSDT": BTC}
US_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade&timeUnit=MICROSECOND"
MS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"


def trade_frame(m: bool, time: int = 1791675558429090) -> str:
    data = {
        "e": "trade",
        "E": time + 163,
        "s": "BTCUSDT",
        "t": 6752423791,
        "p": "83040.01000000",
        "q": "0.00013000",
        "T": time,
        "m": m,
        "M": True,
    }
    return json.dumps({"stream": "btcusdt@trade", "data": data})


def test_stream_url_asks_for_microseconds():
    feeds = [BTC, Feed("BINANCE", "ETH/USDT", "ETHUSDT", 1)]
    url = binance.stream_url("wss://stream.binance.com:9443", feeds)
    assert url == (
        "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
        "&timeUnit=MICROSECOND"
    )
    assert binance.time_scale(url) == 1
    assert binance.time_scale(MS_URL) == 1000


def test_trade_frame_is_normalised():
    frame = binance.parse_frame(trade_frame(m=True), 1791675558592190, FEEDS, scale=1)
    (trade,) = frame.trades
    assert not frame.shutdown
    assert (trade.exchange, trade.symbol, trade.base, trade.quote) == (
        "BINANCE",
        "BTC/USDT",
        "BTC",
        "USDT",
    )
    assert trade.trade_id == 6752423791
    assert trade.price == Decimal("83040.01")
    assert trade.quantity == Decimal("0.00013")
    assert trade.trade_time == 1791675558429090
    assert trade.received_time == 1791675558592190
    assert trade.source is Source.WS


def test_taker_side_comes_from_the_maker_flag():
    # m = "the buyer is the maker": then the seller took liquidity.
    taker_sold = binance.parse_frame(trade_frame(m=True), 0, FEEDS, 1).trades[0]
    taker_bought = binance.parse_frame(trade_frame(m=False), 0, FEEDS, 1).trades[0]
    assert taker_sold.taker_side is Side.SELL
    assert taker_bought.taker_side is Side.BUY


def test_millisecond_streams_are_converted_to_microseconds():
    frame = binance.parse_frame(trade_frame(m=True, time=1791675558429), 0, FEEDS, scale=1000)
    assert frame.trades[0].trade_time == 1791675558429000


def test_server_shutdown_is_recognised():
    raw = json.dumps(
        {"stream": "!serverShutdown", "data": {"e": "serverShutdown", "E": 1770123456789}}
    )
    assert binance.parse_frame(raw, 0, FEEDS, 1).shutdown


def test_other_messages_carry_no_trades():
    frame = binance.parse_frame('{"result": null, "id": 1}', 0, FEEDS, 1)
    assert frame.trades == () and not frame.shutdown


def test_a_maker_flag_that_is_not_a_boolean_is_rejected():
    raw = trade_frame(m=True).replace('"m": true', '"m": "false"')
    with pytest.raises(ValueError):
        binance.parse_frame(raw, 0, FEEDS, 1)


def test_unknown_symbols_are_rejected():
    raw = trade_frame(m=True).replace("BTCUSDT", "DOGEUSDT")
    with pytest.raises(KeyError):
        binance.parse_frame(raw, 0, FEEDS, 1)


def test_history_row_is_normalised():
    row = [
        "2075332080",
        "109.61000000",
        "0.18500000",
        "20.27785000",
        "1791504000142804",
        "True",
        "True",
    ]
    sol = Feed("BINANCE", "SOL/USDT", "SOLUSDT", 2)
    trade = binance.parse_history_row(row, sol, scale=1)
    assert trade.trade_id == 2075332080
    assert trade.price == Decimal("109.61")
    assert trade.quantity == Decimal("0.185")
    assert trade.taker_side is Side.SELL
    assert trade.trade_time == 1791504000142804
    assert trade.received_time is None
    assert trade.source is Source.HISTORY


def test_history_rows_must_be_well_formed():
    sol = Feed("BINANCE", "SOL/USDT", "SOLUSDT", 2)
    with pytest.raises(ValueError):
        binance.parse_history_row(["1", "2"], sol, 1)
    with pytest.raises(ValueError):
        binance.parse_history_row(["1", "2", "3", "6", "4", "yes", "True"], sol, 1)


def test_history_files_switched_to_microseconds_in_2025():
    assert binance.history_time_scale(date(2024, 12, 31)) == 1000
    assert binance.history_time_scale(date(2025, 1, 1)) == 1
