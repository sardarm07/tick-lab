"""Fixtures: up to 15 minutes of real trades, complete by trade ID, with the official candles.

A fixture is a directory under tests/fixtures/e2e/ and is committed to the repository:

- `trades.csv.gz`: every trade of the window, as rows of Binance's daily history files with the
  market in front: native, trade ID, price, qty, quoteQty, time (µs), isBuyerMaker, isBestMatch;
- `klines.csv`: Binance's official 1-minute candles for the window;
- `fixture.json`: where it came from.

The end-to-end test replays a fixture with chaos, the fake exchange serves its trades to gap
repair and its candles to the checks, and the result must equal the official candles.
"""

from __future__ import annotations

import csv
import dataclasses
import gzip
import heapq
import io
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from ticklab import binance, binance_rest, history, recorder
from ticklab.model import Trade
from ticklab.settings import ROOT, Feed, Settings

FIXTURES = ROOT / "tests" / "fixtures" / "e2e"
MAX_MINUTES = 15
TRADE_HEADER = "native,trade_id,price,qty,quote_qty,time_us,is_buyer_maker,is_best_match"
KLINE_HEADER = (
    "native,open_time_ms,open,high,low,close,volume,close_time_ms,quote_volume,count,"
    "taker_buy_volume,taker_buy_quote_volume"
)


@dataclass(frozen=True)
class Fixture:
    path: Path

    @property
    def name(self) -> str:
        return self.path.name

    def meta(self) -> dict:
        return json.loads((self.path / "fixture.json").read_text())

    def trades_by_market(self) -> dict[str, list[list[str]]]:
        rows: dict[str, list[list[str]]] = {}
        with gzip.open(self.path / "trades.csv.gz", "rt", newline="") as f:
            for row in csv.reader(line for line in f if not line.startswith("#")):
                rows.setdefault(row[0], []).append(row)
        return rows

    def klines_by_market(self) -> dict[str, list[list[str]]]:
        rows: dict[str, list[list[str]]] = {}
        with open(self.path / "klines.csv", newline="") as f:
            for row in csv.reader(line for line in f if not line.startswith("#")):
                rows.setdefault(row[0], []).append(row[1:])
        return rows

    def trades(self, feeds: dict[str, Feed]) -> Iterator[tuple[int, Trade]]:
        """(trade time, trade) for every trade, all markets merged by trade time."""

        def one(native: str, rows: list[list[str]]) -> Iterator[tuple[int, Trade]]:
            for row in rows:
                trade = binance.parse_history_row(row[1:], feeds[native], scale=1)
                yield trade.trade_time, trade

        streams = [one(native, rows) for native, rows in self.trades_by_market().items()]
        return heapq.merge(*streams, key=lambda item: item[0])


def load(name: str) -> Fixture:
    return Fixture(FIXTURES / name)


def cut(
    settings: Settings, feeds: list[Feed], source: str, start: datetime, minutes: int, name: str
) -> Fixture:
    """Cut a fixture from history (exact rows) or from recordings (normalised frames, with any
    IDs the recording missed fetched from REST, so the fixture is complete)."""
    if not 0 < minutes <= MAX_MINUTES:
        raise ValueError(f"fixtures are 1 to {MAX_MINUTES} minutes long")
    start = start.replace(second=0, microsecond=0)
    end = start + timedelta(minutes=minutes)
    start_us, end_us = _us(start), _us(end)
    rows: list[list[str]] = []
    klines: list[str] = []
    with httpx.Client(timeout=30) as client:
        for feed in feeds:
            if source == "history":
                path = history.local_path(settings.history_dir, feed, start.date())
                market = [r for r in history.read_rows(path) if start_us <= int(r[4]) < end_us]
            else:
                market = _from_recordings(settings, client, feed, start_us, end_us)
            ids = [int(r[0]) for r in market]
            if ids != list(range(ids[0], ids[0] + len(ids))):
                raise ValueError(f"{feed.native}: trade IDs are not contiguous")
            rows += [[feed.native, *r] for r in market]
            _, text = binance_rest.klines(client, feed, _us(start) // 1000, end_us // 1000 - 1)
            for k in json.loads(text):
                klines.append(",".join(str(v) for v in [feed.native, *k[:11]]))
    folder = FIXTURES / name
    folder.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    buffer.write(f"# {TRADE_HEADER}\n")
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    # mtime=0 keeps the gzip bytes identical when the same fixture is cut again.
    with (
        open(folder / "trades.csv.gz", "wb") as raw,
        gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as f,
    ):
        f.write(buffer.getvalue().encode())
    (folder / "klines.csv").write_text(f"# {KLINE_HEADER}\n" + "\n".join(klines) + "\n")
    meta = {
        "name": name,
        "source": source,
        "start": start.isoformat(),
        "minutes": minutes,
        "markets": {feed.native: sum(1 for r in rows if r[0] == feed.native) for feed in feeds},
        "candles": len(klines),
    }
    (folder / "fixture.json").write_text(json.dumps(meta, indent=1) + "\n")
    return Fixture(folder)


def _from_recordings(
    settings: Settings, client: httpx.Client, feed: Feed, start_us: int, end_us: int
) -> list[list[str]]:
    """Trades of one market from recorded frames, completed from REST where frames are missing."""
    margin = 60_000_000  # frames arrive after their trades: read a little past the window
    files = recorder.find_files(settings.recordings_dir, feed.exchange, start_us, end_us + margin)
    seen: dict[int, Trade] = {}
    urls: dict[tuple[str, int], str] = {}
    by_native = {feed.native: feed}
    for session, line in recorder.read_files(files):
        if line["kind"] == "open":
            urls[(session, line["conn"])] = line["url"]
        elif line["kind"] == "frame" and line["raw"].find(f'"s":"{feed.native}"') >= 0:
            scale = binance.time_scale(urls[(session, line["conn"])])
            for trade in binance.parse_frame(line["raw"], line["recv_us"], by_native, scale).trades:
                if start_us <= trade.trade_time < end_us:
                    seen.setdefault(trade.trade_id, trade)
    if not seen:
        raise ValueError(f"{feed.native}: no recorded trades in the window")
    first, last = min(seen), max(seen)
    missing = [i for i in range(first, last + 1) if i not in seen]
    if missing:
        for trade in binance_rest.trades_between_ids(
            client, settings.binance_rest_url, feed, missing[0], missing[-1]
        ):
            seen.setdefault(trade.trade_id, dataclasses.replace(trade, received_time=None))
    return [_row(seen[i]) for i in range(first, last + 1)]


def _row(trade: Trade) -> list[str]:
    quote = (trade.price * trade.quantity).normalize()
    return [
        str(trade.trade_id),
        str(trade.price),
        str(trade.quantity),
        f"{quote:f}",
        str(trade.trade_time),
        str(trade.taker_side.value == "SELL"),
        "True",
    ]


def _us(moment: datetime) -> int:
    return int(moment.astimezone(UTC).timestamp() * 1_000_000)
