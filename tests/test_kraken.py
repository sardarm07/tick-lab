"""The Kraken normaliser on messages in the documented format (docs.kraken.com, WebSocket v2
`trade` channel, whose examples use MATIC/USD; here with a pair Tick Lab follows)."""

import json
from decimal import Decimal

import pytest

from ticklab import kraken
from ticklab.model import Side, Source
from ticklab.settings import Feed

BTC = Feed("KRAKEN", "BTC/USD", "BTC/USD", 3)
FEEDS = {"BTC/USD": BTC}

SNAPSHOT = """{"channel": "trade", "type": "snapshot", "data": [
  {"symbol": "BTC/USD", "side": "buy", "price": 0.5147, "qty": 6423.46326, "ord_type": "limit",
   "trade_id": 4665846, "timestamp": "2023-09-25T07:48:36.925533Z"},
  {"symbol": "BTC/USD", "side": "buy", "price": 0.5147, "qty": 1136.19677815, "ord_type": "limit",
   "trade_id": 4665847, "timestamp": "2023-09-25T07:49:36.925603Z"}]}"""
UPDATE = """{"channel": "trade", "type": "update", "data": [
  {"symbol": "BTC/USD", "side": "sell", "price": 0.5117, "qty": 40.0, "ord_type": "market",
   "trade_id": 4665906, "timestamp": "2023-09-25T07:49:37.708706Z"}]}"""


def test_subscribing_asks_for_a_snapshot():
    message = json.loads(kraken.subscribe_message([BTC]))
    assert message == {
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": ["BTC/USD"], "snapshot": True},
    }


def test_one_message_can_carry_several_trades():
    frame = kraken.parse_frame(SNAPSHOT, 1_000, FEEDS)
    first, second = frame.trades
    assert (first.trade_id, second.trade_id) == (4665846, 4665847)
    assert first.source is Source.WS_SNAPSHOT
    assert second.quantity == Decimal("1136.19677815")  # exact, never a float


def test_update_trades_are_normalised():
    (trade,) = kraken.parse_frame(UPDATE, 1_000, FEEDS).trades
    assert trade.exchange == "KRAKEN" and trade.symbol == "BTC/USD"
    assert trade.price == Decimal("0.5117")
    assert trade.quantity == Decimal("40.0")
    assert trade.taker_side is Side.SELL
    assert trade.trade_time == 1695628177708706
    assert trade.received_time == 1_000
    assert trade.source is Source.WS


def test_heartbeats_and_status_carry_no_trades():
    assert kraken.parse_frame('{"channel": "heartbeat"}', 0, FEEDS).trades == ()
    status = '{"channel": "status", "type": "update", "data": [{"system": "online"}]}'
    assert kraken.parse_frame(status, 0, FEEDS).trades == ()


def test_rest_rows_are_normalised():
    # A real row (BTC/USD, 2026-10-11).
    row = ["83007.70000", "0.00006177", Decimal("1791708499.9517841"), "s", "l", "", 110963980]
    trade = kraken.parse_rest_row(row, BTC)
    assert trade.trade_id == 110963980
    assert trade.price == Decimal("83007.7") and trade.quantity == Decimal("0.00006177")
    assert trade.taker_side is Side.SELL
    assert trade.trade_time == 1791708499951784
    assert trade.source is Source.REST and trade.received_time is None


# Real, BTC/USD on 2026-10-11: each trade's WebSocket timestamp, its exact time in nanoseconds
# (REST `last` after a one-trade page) and its REST `time`.
AGREE = ("2026-10-11T09:14:01.977337Z", 1791710041977337018, "1791710041.9773371")
DIFFER = ("2026-10-11T09:14:02.286643Z", 1791710042286643934, "1791710042.286644")


def test_websocket_times_are_kraken_times_truncated_to_microseconds():
    for timestamp, exact_ns, _ in (AGREE, DIFFER):
        assert kraken.rfc3339_micros(timestamp) == exact_ns // 1000


def test_rest_times_are_floats_so_truncating_them_can_be_1_microsecond_off():
    timestamp, _, rest_time = AGREE
    assert kraken.seconds_micros(rest_time) == kraken.rfc3339_micros(timestamp)
    timestamp, _, rest_time = DIFFER
    assert kraken.seconds_micros(rest_time) == kraken.rfc3339_micros(timestamp) + 1


def test_bad_values_are_rejected():
    with pytest.raises(ValueError):
        kraken.parse_frame(UPDATE.replace('"sell"', '"short"'), 0, FEEDS)
    with pytest.raises(ValueError):
        kraken.parse_frame(UPDATE.replace("4665906", "4665906.5"), 0, FEEDS)
    with pytest.raises(ValueError):
        kraken.rfc3339_micros("2023-09-25T07:49:37.708706")
