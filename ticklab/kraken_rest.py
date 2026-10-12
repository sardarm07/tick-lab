"""Kraken REST: trades since a time (gap repair, history) and 1-minute OHLC (reconciliation).

Checked live 2026-10-11:

- With `assetVersion=1`, pairs use display names (BTC/USD), the same as the WebSocket, and the
  response is keyed by them.
- `GET /0/public/Trades?pair&since&count`: up to 1,000 rows of [price, volume, time, b/s, m/l,
  misc, trade_id], in time order, plus `last`, a time in nanoseconds to pass back as `since`.
- Kraken keeps trade times in nanoseconds. `since` is inclusive: a trade at exactly `since` is
  returned, so consecutive pages share their boundary trade. `last` is the exact time of the
  page's last trade. Each row's `time` is binary floating-point seconds, within 0.12 µs of the
  exact time (see `kraken.seconds_micros`). The JSON is parsed with exact decimals all the same.
- `GET /0/public/OHLC?pair&interval=1`: the 721 most recent minutes as [time, open, high, low,
  close, vwap, volume, count]; the last row is the current, unfinished minute. Kraken publishes
  candles with a count of 0 for minutes without trades, and no quote volume.
- Public calls at about one per second per pair are safe.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from decimal import Decimal

import httpx

from ticklab import kraken
from ticklab.candles import Candle
from ticklab.model import Trade
from ticklab.settings import Feed

BASE_URL = "https://api.kraken.com"


def trades_page(client: httpx.Client, base: str, feed: Feed, since_ns: int) -> tuple[str, str]:
    """One raw page of up to 1,000 trades after `since_ns`, with its URL."""
    response = client.get(
        f"{base}/0/public/Trades",
        params={"pair": feed.native, "since": str(since_ns), "count": 1000, "assetVersion": 1},
    )
    response.raise_for_status()
    return str(response.url), response.text


def parse_trades_page(text: str, feed: Feed) -> tuple[list[Trade], int]:
    """The trades of a page, and its `last` value: the `since` of the next page."""
    body = json.loads(text, parse_float=Decimal)
    if body.get("error"):
        raise RuntimeError(f"Kraken: {body['error']}")
    result = body["result"]
    return [kraken.parse_rest_row(row, feed) for row in result[feed.native]], int(result["last"])


def trades_since(
    client: httpx.Client, base: str, feed: Feed, since_ns: int
) -> tuple[list[Trade], int]:
    """Trades after `since_ns`, and the `last` value for the next page."""
    _, text = trades_page(client, base, feed, since_ns)
    return parse_trades_page(text, feed)


def pages_between_ids(
    client: httpx.Client,
    base: str,
    feed: Feed,
    since_ns: int,
    first: int,
    last: int,
    wait: Callable[[], None],
) -> Iterator[list[Trade]]:
    """Every trade with first <= ID <= last, a page at a time, paging forward from `since_ns`,
    which must not be later than trade `first`. `wait` runs before each request, to pace them.
    Pages overlap by their boundary trade (`since` is inclusive), so each trade is yielded once,
    by ID."""
    seen: set[int] = set()
    while True:
        wait()
        trades, next_since = trades_since(client, base, feed, since_ns)
        page = {t.trade_id: t for t in trades if first <= t.trade_id <= last}
        if fresh := [page[trade_id] for trade_id in sorted(page.keys() - seen)]:
            seen.update(page)
            yield fresh
        if not trades or trades[-1].trade_id >= last or next_since <= since_ns:
            return
        since_ns = next_since


def ohlc(client: httpx.Client, base: str, feed: Feed) -> tuple[str, str]:
    """The 1-minute OHLC response, raw (for the recorder), with its URL."""
    response = client.get(
        f"{base}/0/public/OHLC", params={"pair": feed.native, "interval": 1, "assetVersion": 1}
    )
    response.raise_for_status()
    return str(response.url), response.text


def parse_ohlc(text: str, feed: Feed) -> dict[datetime, Candle]:
    """Finished minutes only: the newest row is still changing."""
    body = json.loads(text, parse_float=Decimal)
    if body.get("error"):
        raise RuntimeError(f"Kraken: {body['error']}")
    return dict(ohlc_row(row) for row in body["result"][feed.native][:-1])


def ohlc_row(row: list) -> tuple[datetime, Candle]:
    """One OHLC row, [time, open, high, low, close, vwap, volume, count], as a candle."""
    time, open_, high, low, close, _vwap, volume, count = row
    return datetime.fromtimestamp(int(time), UTC), Candle(
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
        quote_volume=None,
        taker_buy_volume=None,
        taker_buy_quote_volume=None,
        trade_count=int(count),
    )
