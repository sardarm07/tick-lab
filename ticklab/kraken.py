"""Kraken spot: WebSocket v2 trade messages and REST Trades rows, normalised to Trade.

Sources (checked 2026-10-11): docs.kraken.com, the WebSocket v2 `trade` channel and the REST
`Trades` and `OHLC` endpoints, plus live checks.

- Subscribing sends {"method": "subscribe", "params": {"channel": "trade", "symbol": [...],
  "snapshot": true}}. The snapshot is the 50 most recent trades of each pair; after a reconnect
  it overlaps what was already received, and deduplication removes the overlap.
- Trade messages are {"channel": "trade", "type": "snapshot" or "update", "data": [...]}, and
  one message can hold several trades. Each has symbol, side (the taker's), price and qty (JSON
  numbers), ord_type, trade_id (a sequence number per pair) and timestamp (RFC 3339, µs).
- Prices and quantities are JSON numbers, so the parser reads them straight into Decimal from
  their text; they never pass through a float.
- {"channel": "heartbeat"} arrives about once a second when nothing else is sent.
- REST `Trades` rows are [price, volume, time, b/s, m/l, misc, trade_id], with price and volume
  as decimal strings and time as seconds with a fraction.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from ticklab.model import Frame, Side, Source, Trade, to_decimal
from ticklab.settings import Feed

EXCHANGE = "KRAKEN"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def subscribe_message(feeds: list[Feed]) -> str:
    return json.dumps(
        {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [feed.native for feed in feeds],
                "snapshot": True,
            },
        }
    )


def parse_frame(raw: str, received_us: int, feeds: dict[str, Feed]) -> Frame:
    """Normalise one frame. `feeds` maps the WebSocket symbol (BTC/USD) to its feed."""
    message = json.loads(raw, parse_float=Decimal)
    if message.get("channel") != "trade":
        return Frame()  # heartbeat, status, or a reply to a request
    source = Source.WS_SNAPSHOT if message.get("type") == "snapshot" else Source.WS
    return Frame(trades=tuple(_ws_trade(d, received_us, feeds, source) for d in message["data"]))


def _ws_trade(data: dict, received_us: int, feeds: dict[str, Feed], source: Source) -> Trade:
    feed = feeds[data["symbol"]]
    trade_id = data["trade_id"]
    if type(trade_id) is not int:
        raise ValueError(f"trade_id must be an integer: {data!r}")
    return Trade(
        exchange=EXCHANGE,
        symbol=feed.symbol,
        base=feed.base,
        quote=feed.quote,
        trade_id=trade_id,
        price=_number(data["price"]),
        quantity=_number(data["qty"]),
        taker_side=_side(data["side"]),
        trade_time=rfc3339_micros(data["timestamp"]),
        received_time=received_us,
        source=source,
    )


def parse_rest_row(row: list, feed: Feed, source: Source = Source.REST) -> Trade:
    """One row of REST `Trades`: [price, volume, time, b/s, m/l, misc, trade_id]."""
    price, volume, time, side, _ord_type, _misc, trade_id = row
    return Trade(
        exchange=EXCHANGE,
        symbol=feed.symbol,
        base=feed.base,
        quote=feed.quote,
        trade_id=int(trade_id),
        price=to_decimal(price),
        quantity=to_decimal(volume),
        taker_side={"b": Side.BUY, "s": Side.SELL}[side],
        trade_time=seconds_micros(str(time)),
        received_time=None,
        source=source,
    )


def rfc3339_micros(text: str) -> int:
    """'2026-10-11T07:49:37.708706Z' as microseconds since the epoch, exactly."""
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        raise ValueError(f"timestamp without a time zone: {text!r}")
    delta = moment - EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def seconds_micros(text: str) -> int:
    """'1791676042.5123456' (seconds, as REST sends them) as whole microseconds, truncated.

    Kraken keeps trade times in nanoseconds. The WebSocket truncates them to microseconds, while
    REST sends binary floating-point seconds, within 0.12 µs of them. So a truncated REST time is
    the WebSocket's value for most trades and 1 µs off for the rest (43 of 50 and 7 of 50 in a
    check on 2026-10-11). A trade could only land in the wrong minute if it happened within
    0.12 µs of the minute's end."""
    whole, _, fraction = text.partition(".")
    return int(whole) * 1_000_000 + int((fraction + "000000")[:6])


def _number(value: Decimal | int) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"not a number: {value!r}")
    return to_decimal(str(value))


def _side(text: str) -> Side:
    if text == "buy":
        return Side.BUY
    if text == "sell":
        return Side.SELL
    raise ValueError(f"unknown side {text!r}")
