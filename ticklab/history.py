"""Binance's daily trade files from data.binance.vision: download, verify, read.

Each day and market is a zip holding one CSV, published the day after, with a `.CHECKSUM`
file next to it in `sha256sum` format. Files are kept exactly as downloaded, so replays
read them through the normaliser like any other raw data.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx

from ticklab import binance
from ticklab.settings import Feed

log = logging.getLogger(__name__)

DAY_US = 86_400_000_000


def file_name(feed: Feed, day: date) -> str:
    return f"{feed.native}-trades-{day:%Y-%m-%d}.zip"


def file_url(base: str, feed: Feed, day: date) -> str:
    return f"{base}/data/spot/daily/trades/{feed.native}/{file_name(feed, day)}"


def local_path(history_dir: Path, feed: Feed, day: date) -> Path:
    return history_dir / "binance" / feed.native / file_name(feed, day)


def days(first: date, last: date) -> list[date]:
    return [first + timedelta(n) for n in range((last - first).days + 1)]


def parse_checksum(text: str) -> tuple[str, str]:
    """`<sha256 hex>  <file name>`, as written by sha256sum."""
    match = re.fullmatch(r"([0-9a-fA-F]{64}) [ *]?(\S+)\s*", text)
    if not match:
        raise ValueError(f"not a sha256sum line: {text!r}")
    return match.group(1).lower(), match.group(2)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def download_day(client: httpx.Client, base: str, history_dir: Path, feed: Feed, day: date) -> str:
    """Download one file unless a verified copy exists. Returns "downloaded" or "kept"."""
    url = file_url(base, feed, day)
    path = local_path(history_dir, feed, day)
    response = client.get(url + ".CHECKSUM")
    response.raise_for_status()
    checksum_text = response.text
    expected, name = parse_checksum(checksum_text)
    if name != path.name:
        raise ValueError(f"{url}.CHECKSUM names {name!r}")
    checksum_path = path.with_name(path.name + ".CHECKSUM")
    if path.exists() and sha256_file(path) == expected:
        checksum_path.write_text(checksum_text)
        return "kept"
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    digest = hashlib.sha256()
    with client.stream("GET", url) as response, open(partial, "wb") as f:
        response.raise_for_status()
        for chunk in response.iter_bytes(1 << 20):
            f.write(chunk)
            digest.update(chunk)
    if digest.hexdigest() != expected:
        partial.unlink()
        raise ValueError(f"{url}: SHA-256 {digest.hexdigest()} does not match {expected}")
    partial.rename(path)
    checksum_path.write_text(checksum_text)
    return "downloaded"


def read_rows(path: Path) -> Iterator[list[str]]:
    with zipfile.ZipFile(path) as archive:
        (name,) = archive.namelist()  # exactly one CSV per file
        with archive.open(name) as raw:
            yield from csv.reader(io.TextIOWrapper(raw, encoding="ascii", newline=""))


@dataclass
class FileCheck:
    feed: Feed
    day: date
    path: Path
    checksum_ok: bool = False
    rows: int = 0
    first_id: int | None = None
    last_id: int | None = None
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.checksum_ok and not self.problems


def check_file(history_dir: Path, feed: Feed, day: date) -> FileCheck:
    """Checksum, row shape, contiguous trade IDs and times inside the day."""
    path = local_path(history_dir, feed, day)
    check = FileCheck(feed, day, path)
    if not path.exists():
        check.problems.append("missing")
        return check
    expected, _ = parse_checksum(path.with_name(path.name + ".CHECKSUM").read_text())
    check.checksum_ok = sha256_file(path) == expected
    if not check.checksum_ok:
        return check  # its contents can't be trusted, so they aren't read
    scale = binance.history_time_scale(day)
    day_start = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * 1_000_000
    previous = None
    for row in read_rows(path):
        trade = binance.parse_history_row(row, feed, scale)  # raises on a malformed row
        room = len(check.problems) < 5  # report the first few problems only
        if room and previous is not None and trade.trade_id != previous + 1:
            check.problems.append(f"after ID {previous} comes {trade.trade_id}")
        elif room and not day_start <= trade.trade_time < day_start + DAY_US:
            check.problems.append(f"ID {trade.trade_id} is outside the day")
        if check.first_id is None:
            check.first_id = trade.trade_id
        previous = check.last_id = trade.trade_id
        check.rows += 1
    return check
