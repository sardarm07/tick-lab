"""A stand-in for an exchange's REST API, serving one fixture, for tests and CI.

For a Binance fixture, it answers the two calls the pipeline makes, in Binance's format:

- `GET /api/v3/historicalTrades?symbol&fromId&limit`, with times in microseconds when the
  request has `X-MBX-TIME-UNIT: MICROSECOND` (as gap repair sends), else milliseconds;
- `GET /api/v3/klines?symbol&interval=1m&startTime&endTime&limit`.

For a Kraken fixture, in Kraken's format:

- `GET /0/public/Trades?pair&since&count`: rows at or after `since` (inclusive, nanoseconds),
  with `last` the time of the page's last row. Kraken's own times are finer than the rows'
  times; the fake uses the rows' times, so trades of one match share a time here;
- `GET /0/public/OHLC?pair&interval=1`: the fixture's candles, plus a row for the minute after
  them, standing for the unfinished minute that Kraken always puts last.

So gap repair and the reconciler run unchanged against recorded data, and CI never touches a
live exchange.
"""

from __future__ import annotations

import bisect
import json
import logging
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from ticklab import kraken
from ticklab.fixture import Fixture, kraken_row_json

log = logging.getLogger(__name__)


class FakeBinance:
    def __init__(self, fixture: Fixture) -> None:
        self._trades = {native: rows for native, rows in fixture.trades_by_market().items()}
        self._ids = {native: [int(r[1]) for r in rows] for native, rows in self._trades.items()}
        self._klines = fixture.klines_by_market()

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


class FakeKraken:
    def __init__(self, fixture: Fixture) -> None:
        self._rows = fixture.kraken_rows()
        self._ns = {
            pair: [int(Decimal(row[2]) * 10**9) for row in rows]
            for pair, rows in self._rows.items()
        }
        self._ohlc = fixture.ohlc_rows()

    def trades(self, pair: str, since_ns: int, count: int) -> str:
        rows, times = self._rows.get(pair, []), self._ns.get(pair, [])
        start = bisect.bisect_left(times, since_ns)
        page = rows[start : start + min(count, 1000)]
        last = times[start + len(page) - 1] if page else since_ns
        body = ", ".join(kraken_row_json(row) for row in page)
        return f'{{"error": [], "result": {{{json.dumps(pair)}: [{body}], "last": "{last}"}}}}'

    def ohlc(self, pair: str) -> str:
        rows = self._ohlc.get(pair, [])
        unfinished = [rows[-1][0] + 60, *rows[-1][1:5], "0.0", "0.00000000", 0] if rows else []
        out = [[r[0], *[str(v) for v in r[1:7]], r[7]] for r in rows] + [unfinished]
        return json.dumps({"error": [], "result": {pair: out, "last": out[-1][0]}})


def serve(fixture: Fixture, port: int) -> None:
    if fixture.exchange == kraken.EXCHANGE:
        _serve(fixture, port, kraken_exchange=FakeKraken(fixture))
    else:
        _serve(fixture, port, binance_exchange=FakeBinance(fixture))


def _serve(
    fixture: Fixture,
    port: int,
    binance_exchange: FakeBinance | None = None,
    kraken_exchange: FakeKraken | None = None,
) -> None:

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            url = urlsplit(self.path)
            query = {k: v[0] for k, v in parse_qs(url.query).items()}
            if url.path == "/api/v3/ping":  # the health check, whichever the exchange
                text = "{}"
            elif binance_exchange and url.path == "/api/v3/historicalTrades":
                micros = self.headers.get("X-MBX-TIME-UNIT", "").upper() == "MICROSECOND"
                text = json.dumps(
                    binance_exchange.historical_trades(
                        query["symbol"],
                        int(query.get("fromId", 0)),
                        int(query.get("limit", 500)),
                        micros,
                    )
                )
            elif binance_exchange and url.path == "/api/v3/klines":
                text = json.dumps(
                    binance_exchange.klines(
                        query["symbol"],
                        int(query["startTime"]),
                        int(query["endTime"]),
                        int(query.get("limit", 500)),
                    )
                )
            elif kraken_exchange and url.path == "/0/public/Trades":
                text = kraken_exchange.trades(
                    query["pair"], int(query.get("since", 0)), int(query.get("count", 1000))
                )
            elif kraken_exchange and url.path == "/0/public/OHLC":
                text = kraken_exchange.ohlc(query["pair"])
            else:
                self.send_error(404)
                return
            data = text.encode()
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
