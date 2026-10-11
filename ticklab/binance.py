"""Binance spot: live `@trade` frames and daily history rows, normalised to Trade.

Sources (checked 2026-10-11): github.com/binance/binance-spot-api-docs, files
web-socket-streams.md and CHANGELOG.md, plus the daily files on data.binance.vision.

- A combined stream wraps every event as {"stream": ..., "data": ...}.
- `@trade` events carry e, E, s, t (trade ID), p, q (decimal strings), T (trade time),
  m (the buyer is the maker) and M (to be ignored).
- Times are milliseconds unless the URL asks for `timeUnit=MICROSECOND`. The live stream
  asks for microseconds, the unit of the daily history files.
- A `serverShutdown` event means the server is about to close the connection.
- History CSV columns, without a header row: trade ID, price, qty, quoteQty, time,
  isBuyerMaker, isBestMatch. `time` is in microseconds from 2025-01-01 onwards.
"""

from __future__ import annotations

import json
from datetime import date
from urllib.parse import parse_qs, urlsplit

from ticklab.model import Frame, Side, Source, Trade, to_decimal
from ticklab.settings import Feed

EXCHANGE = "BINANCE"
HISTORY_MICROSECONDS_FROM = date(2025, 1, 1)


def stream_url(base: str, feeds: list[Feed]) -> str:
    """One combined stream for all pairs, with times in microseconds."""
    streams = "/".join(f"{feed.native.lower()}@trade" for feed in feeds)
    return f"{base}/stream?streams={streams}&timeUnit=MICROSECOND"


def time_scale(url: str) -> int:
    """Microseconds per unit of the stream's time fields (1 for microseconds, else 1000)."""
    unit = parse_qs(urlsplit(url).query).get("timeUnit", ["MILLISECOND"])[0]
    return 1 if unit.upper() == "MICROSECOND" else 1000


def parse_frame(raw: str, received_us: int, feeds: dict[str, Feed], scale: int) -> Frame:
    """Normalise one frame. `feeds` maps the native symbol (BTCUSDT) to its feed."""
    message = json.loads(raw)
    data = message.get("data", message)  # combined streams wrap the payload; raw ones don't
    event = data.get("e") if isinstance(data, dict) else None
    if event == "trade":
        return Frame(trades=(_ws_trade(data, received_us, feeds, scale),))
    if event == "serverShutdown":
        return Frame(shutdown=True)
    return Frame()  # for example a reply to a request: nothing to publish


def _ws_trade(data: dict, received_us: int, feeds: dict[str, Feed], scale: int) -> Trade:
    feed = feeds[data["s"]]
    trade_id, trade_time, buyer_is_maker = data["t"], data["T"], data["m"]
    if type(trade_id) is not int or type(trade_time) is not int:
        raise ValueError(f"t and T must be integers: {data!r}")
    if type(buyer_is_maker) is not bool:
        raise ValueError(f"m must be a boolean: {data!r}")
    return Trade(
        exchange=EXCHANGE,
        symbol=feed.symbol,
        base=feed.base,
        quote=feed.quote,
        trade_id=trade_id,
        price=to_decimal(data["p"]),
        quantity=to_decimal(data["q"]),
        # When the buyer is the maker, the seller took liquidity.
        taker_side=Side.SELL if buyer_is_maker else Side.BUY,
        trade_time=trade_time * scale,
        received_time=received_us,
        source=Source.WS,
    )


def history_time_scale(day: date) -> int:
    return 1 if day >= HISTORY_MICROSECONDS_FROM else 1000


def parse_history_row(row: list[str], feed: Feed, scale: int) -> Trade:
    """Normalise one row of a daily trade file."""
    if len(row) != 7:
        raise ValueError(f"expected 7 columns, got {len(row)}: {row!r}")
    trade_id, price, qty, _quote_qty, time, is_buyer_maker, _is_best_match = row
    return Trade(
        exchange=EXCHANGE,
        symbol=feed.symbol,
        base=feed.base,
        quote=feed.quote,
        trade_id=int(trade_id),
        price=to_decimal(price),
        quantity=to_decimal(qty),
        taker_side=Side.SELL if _parse_bool(is_buyer_maker) else Side.BUY,
        trade_time=int(time) * scale,
        received_time=None,
        source=Source.HISTORY,
    )


def _parse_bool(text: str) -> bool:
    if text == "True":
        return True
    if text == "False":
        return False
    raise ValueError(f"expected True or False, got {text!r}")
