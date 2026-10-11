"""Binance REST: older trades, used to check stored trades field by field.

`GET /api/v3/historicalTrades` takes `symbol`, `fromId` and `limit` (up to 1,000), needs no
API key, and costs 25 request weight out of 6,000 per minute. The `X-MBX-TIME-UNIT` header
switches its `time` field to microseconds, the unit used everywhere else here.
"""

from __future__ import annotations

import dataclasses
import time

import httpx

from ticklab import binance
from ticklab.model import Source, Trade
from ticklab.settings import Feed


def historical_trades(
    client: httpx.Client, base: str, feed: Feed, from_id: int, limit: int
) -> list[Trade]:
    response = client.get(
        f"{base}/api/v3/historicalTrades",
        params={"symbol": feed.native, "fromId": from_id, "limit": limit},
        headers={"X-MBX-TIME-UNIT": "MICROSECOND"},
    )
    if response.status_code in (418, 429):
        raise RuntimeError(f"Binance rate limit ({response.status_code}); stop and wait")
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
    if response.status_code in (418, 429):
        raise RuntimeError(f"Binance rate limit ({response.status_code}); stop and wait")
    response.raise_for_status()
    return str(response.url), response.text
