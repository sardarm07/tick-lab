"""A stand-in for Binance's REST API, serving one fixture, for tests and CI.

Answers the two calls the pipeline makes, in Binance's format:

- `GET /api/v3/historicalTrades?symbol&fromId&limit`, with times in microseconds when the
  request has `X-MBX-TIME-UNIT: MICROSECOND` (as gap repair sends), else milliseconds;
- `GET /api/v3/klines?symbol&interval=1m&startTime&endTime&limit`.

So gap repair and the reconciler run unchanged against recorded data, and CI never touches a
live exchange.
"""

from __future__ import annotations

import bisect
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from ticklab.fixture import Fixture

log = logging.getLogger(__name__)


class FakeBinance:
    def __init__(self, fixture: Fixture) -> None:
        self._trades = {native: rows for native, rows in fixture.trades_by_market().items()}
        self._ids = {native: [int(r[1]) for r in rows] for native, rows in self._trades.items()}
        self._klines = fixture.klines_by_market()
        self.requests = 0

    def historical_trades(self, symbol: str, from_id: int, limit: int, micros: bool) -> list:
        rows = self._trades.get(symbol, [])
        start = bisect.bisect_left(self._ids.get(symbol, []), from_id)
        out = []
        for row in rows[start : start + min(limit, 1000)]:
            _, trade_id, price, qty, quote_qty, time_us, buyer_maker, best = row
            out.append(
                {
                    "id": int(trade_id),
                    "price": price,
                    "qty": qty,
                    "quoteQty": quote_qty,
                    "time": int(time_us) if micros else int(time_us) // 1000,
                    "isBuyerMaker": buyer_maker == "True",
                    "isBestMatch": best == "True",
                }
            )
        return out

    def klines(self, symbol: str, start_ms: int, end_ms: int, limit: int) -> list:
        rows = [k for k in self._klines.get(symbol, []) if start_ms <= int(k[0]) <= end_ms]
        return [
            [int(k[0]), k[1], k[2], k[3], k[4], k[5], int(k[6]), k[7], int(k[8]), k[9], k[10], "0"]
            for k in rows[: min(limit, 1000)]
        ]


def serve(fixture: Fixture, port: int) -> None:
    exchange = FakeBinance(fixture)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            url = urlsplit(self.path)
            query = {k: v[0] for k, v in parse_qs(url.query).items()}
            exchange.requests += 1
            if url.path == "/api/v3/historicalTrades":
                micros = self.headers.get("X-MBX-TIME-UNIT", "").upper() == "MICROSECOND"
                body = exchange.historical_trades(
                    query["symbol"],
                    int(query.get("fromId", 0)),
                    int(query.get("limit", 500)),
                    micros,
                )
            elif url.path == "/api/v3/klines":
                body = exchange.klines(
                    query["symbol"],
                    int(query["startTime"]),
                    int(query["endTime"]),
                    int(query.get("limit", 500)),
                )
            elif url.path == "/api/v3/ping":
                body = {}
            else:
                self.send_error(404)
                return
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: object) -> None:
            log.info("%s", format % args)

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    log.info("serving fixture %s on port %d", fixture.name, port)
    server.serve_forever()
