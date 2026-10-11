"""Binance REST: older trades, used to check stored trades field by field.

`GET /api/v3/historicalTrades` takes `symbol`, `fromId` and `limit` (up to 1,000), needs no
API key, and costs 25 request weight out of 6,000 per minute. The `X-MBX-TIME-UNIT` header
switches its `time` field to microseconds, the unit used everywhere else here.
"""

from __future__ import annotations

import dataclasses
import json
import time
from datetime import UTC, datetime
from decimal import Decimal

import httpx

from ticklab import binance
from ticklab.candles import Candle
from ticklab.model import Source, Trade
from ticklab.settings import Feed


class RateLimited(RuntimeError):
    """Binance answered 429 (too many requests) or 418 (banned for ignoring 429s)."""

    def __init__(self, status: int, retry_after_s: float) -> None:
        super().__init__(f"Binance rate limit ({status}): retry after {retry_after_s:.0f} s")
        self.retry_after_s = retry_after_s


def _check_rate_limit(response: httpx.Response) -> None:
    if response.status_code in (418, 429):
        # Retry-After is in seconds; without it, wait a minute (the weight window).
        raise RateLimited(response.status_code, float(response.headers.get("Retry-After", 60)))


def historical_trades(
    client: httpx.Client, base: str, feed: Feed, from_id: int, limit: int
) -> list[Trade]:
    response = client.get(
        f"{base}/api/v3/historicalTrades",
        params={"symbol": feed.native, "fromId": from_id, "limit": limit},
        headers={"X-MBX-TIME-UNIT": "MICROSECOND"},
    )
    _check_rate_limit(response)
    response.raise_for_status()
    trades = []
    for row in response.json():
        # The same columns as the daily files, as JSON.
        as_csv_row = [
            str(row["id"]),
            row["price"],
            row["qty"],
            row["quoteQty"],
            str(row["time"]),
            str(row["isBuyerMaker"]),
            str(row["isBestMatch"]),
        ]
        trade = binance.parse_history_row(as_csv_row, feed, scale=1)
        trades.append(dataclasses.replace(trade, source=Source.REST))
    return trades


def trades_between_ids(
    client: httpx.Client, base: str, feed: Feed, first: int, last: int
) -> list[Trade]:
    """Every trade with first <= ID <= last, one request per 1,000."""
    trades: list[Trade] = []
    next_id = first
    while next_id <= last:
        page = historical_trades(client, base, feed, next_id, min(1000, last - next_id + 1))
        if not page:
            break
        trades.extend(page)
        next_id = page[-1].trade_id + 1
        time.sleep(0.5)  # stays far below the weight limit
    return trades


KLINES_URL = "https://data-api.binance.vision/api/v3/klines"


def klines(client: httpx.Client, feed: Feed, start_ms: int, end_ms: int) -> tuple[str, str]:
    """Official 1-minute candles opening in [start_ms, end_ms], as (request URL, raw JSON text).

    Weight 2 per request, up to 1,000 candles. Each row is [open time, open, high, low, close,
    volume, close time, quote volume, trades, taker buy volume, taker buy quote volume, -].
    """
    response = client.get(
        KLINES_URL,
        params={
            "symbol": feed.native,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000,
        },
    )
    _check_rate_limit(response)
    response.raise_for_status()
    return str(response.url), response.text


def parse_klines(text: str) -> dict[datetime, Candle]:
    """`klines` rows by minute: [open time, open, high, low, close, volume, close time, quote
    volume, trades, taker buy volume, taker buy quote volume, ignore]."""
    candles = {}
    for row in json.loads(text):
        candles[datetime.fromtimestamp(row[0] / 1000, UTC)] = Candle(
            open=Decimal(row[1]),
            high=Decimal(row[2]),
            low=Decimal(row[3]),
            close=Decimal(row[4]),
            volume=Decimal(row[5]),
            quote_volume=Decimal(row[7]),
            trade_count=int(row[8]),
            taker_buy_volume=Decimal(row[9]),
            taker_buy_quote_volume=Decimal(row[10]),
        )
    return candles
