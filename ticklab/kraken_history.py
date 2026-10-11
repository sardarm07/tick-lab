"""Kraken trade history, from REST `Trades` pages, kept raw.

Kraken publishes no daily files, so history is fetched page by page: up to 1,000 trades after a
time, then the next page from the `last` value each page returns. The pages are kept exactly as
received, in gzip JSON lines like the recorder's, so replays read them through the normaliser
like any other raw data:

    <history dir>/kraken/<BASE-QUOTE>/<from>_<to>.jsonl.gz

About one request a second per pair is safe; a week of four pairs is about 1,500 requests.
"""

from __future__ import annotations

import dataclasses
import gzip
import itertools
import json
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx

from ticklab import kraken_rest
from ticklab.model import Source, Trade
from ticklab.settings import Feed
from ticklab.stream import now_us

NAME_FORMAT = "%Y%m%dT%H%M"


def path_for(history_dir: Path, feed: Feed, start: datetime, end: datetime) -> Path:
    name = f"{start:{NAME_FORMAT}}_{end:{NAME_FORMAT}}.jsonl.gz"
    return history_dir / "kraken" / feed.symbol.replace("/", "-") / name


def find(history_dir: Path, feed: Feed, start: datetime, end: datetime) -> Path | None:
    """The smallest downloaded file that covers [start, end), if any."""
    covering = []
    for path in (history_dir / "kraken" / feed.symbol.replace("/", "-")).glob("*.jsonl.gz"):
        first, _, last = path.name.removesuffix(".jsonl.gz").partition("_")
        try:
            file_start = datetime.strptime(first, NAME_FORMAT).replace(tzinfo=UTC)
            file_end = datetime.strptime(last, NAME_FORMAT).replace(tzinfo=UTC)
        except ValueError:
            continue
        if file_start <= start and end <= file_end:
            covering.append((file_end - file_start, path))
    return min(covering)[1] if covering else None


def download(
    client: httpx.Client,
    base: str,
    history_dir: Path,
    feed: Feed,
    start: datetime,
    end: datetime,
    pause_s: float = 1.0,
) -> tuple[Path, int]:
    """Fetch every page covering [start, end), unless a downloaded file already covers it.
    Returns the file and the number of requests."""
    if (covering := find(history_dir, feed, start, end)) is not None:
        return covering, 0
    path = path_for(history_dir, feed, start, end)
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    end_us = _us(end)
    since = _us(start) * 1000  # nanoseconds
    requests = 0
    with gzip.open(partial, "wt") as out:
        while True:
            started = time.monotonic()
            url, text = kraken_rest.trades_page(client, base, feed, since)
            requests += 1
            out.write(json.dumps({"since": since, "recv_us": now_us(), "url": url, "raw": text}))
            out.write("\n")
            trades, last = kraken_rest.parse_trades_page(text, feed)
            if not trades or trades[-1].trade_time >= end_us or last <= since:
                break
            since = last
            time.sleep(max(0.0, pause_s - (time.monotonic() - started)))  # one request a second
    partial.rename(path)
    return path, requests


def read(path: Path, feed: Feed, start: datetime, end: datetime) -> Iterator[Trade]:
    """Trades of [start, end) in ID order, each once (consecutive pages can overlap)."""
    start_us, end_us = _us(start), _us(end)
    seen: set[int] = set()
    with gzip.open(path, "rt") as f:
        for line in f:
            trades, _ = kraken_rest.parse_trades_page(json.loads(line)["raw"], feed)
            for trade in trades:
                if start_us <= trade.trade_time < end_us and trade.trade_id not in seen:
                    seen.add(trade.trade_id)
                    yield dataclasses.replace(trade, source=Source.HISTORY)


def contiguous(trades: list[Trade]) -> list[str]:
    """Problems with the trade IDs: every ID should follow the one before by exactly one."""
    problems = []
    for before, after in itertools.pairwise(trades):
        if after.trade_id != before.trade_id + 1:
            problems.append(f"after ID {before.trade_id} comes {after.trade_id}")
    return problems


def _us(moment: datetime) -> int:
    return int(moment.astimezone(UTC).timestamp() * 1_000_000)
