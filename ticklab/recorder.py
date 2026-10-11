"""The recorder: raw WebSocket frames, exactly as received, in hourly gzip files.

Frames are written before anything parses them. A bug in a normaliser can then be fixed
and the same data replayed through the corrected code. Each line is one JSON object:

    {"kind": "open",  "conn": 3, "recv_us": ..., "url": "wss://..."}
    {"kind": "frame", "conn": 3, "recv_us": ..., "raw": "<the frame, as text>"}
    {"kind": "close", "conn": 3, "recv_us": ..., "reason": "planned"}

`recv_us` is the local clock in microseconds when the frame arrived. Files live in
<root>/<exchange>/<YYYY-MM-DD>/<exchange>-<YYYYMMDD>T<HH>-<session>.jsonl.gz, where the date
and hour are UTC and the session names the process that wrote them. A new file starts at
every hour and with every new process, and repeats the `open` line of each connection that
is still open, so any file can be replayed on its own. Files are never appended to: after a
crash, the file stays readable up to its last flush (at most a second before the crash).
"""

from __future__ import annotations

import gzip
import heapq
import json
import os
import zlib
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path

HOUR_US = 3_600_000_000


def session_name(kind: str) -> str:
    """A name for one recording process, such as live-20261011T001500Z-12."""
    return f"{kind}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{os.getpid()}"


class Recorder:
    def __init__(self, root: Path, exchange: str, session: str, flush_every_s: float = 1.0):
        self._dir = root / exchange.lower()
        self._prefix = exchange.lower()
        self.session = session
        self._flush_every_us = int(flush_every_s * 1_000_000)
        self._file: gzip.GzipFile | None = None
        self._hour: int | None = None
        self._open_lines: dict[int, dict] = {}
        self._last_flush_us = 0
        self.lines = 0
        self.files: list[Path] = []

    def open_conn(self, conn: int, recv_us: int, url: str) -> None:
        line = {"kind": "open", "conn": conn, "recv_us": recv_us, "url": url}
        self._write(line)
        self._open_lines[conn] = line

    def frame(self, conn: int, recv_us: int, raw: str) -> None:
        self._write({"kind": "frame", "conn": conn, "recv_us": recv_us, "raw": raw})

    def close_conn(self, conn: int, recv_us: int, reason: str) -> None:
        self._open_lines.pop(conn, None)
        self._write({"kind": "close", "conn": conn, "recv_us": recv_us, "reason": reason})

    def tick(self, now_us: int) -> None:
        """Flush if a second has passed, even when no frames arrive."""
        if self._file and now_us - self._last_flush_us >= self._flush_every_us:
            self._flush(now_us)

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    def _write(self, line: dict) -> None:
        hour = line["recv_us"] // HOUR_US
        if hour != self._hour:
            self._start_file(hour)
        assert self._file is not None
        self._file.write(_encode(line))
        self.lines += 1
        self.tick(line["recv_us"])

    def _start_file(self, hour: int) -> None:
        self.close()
        start = datetime.fromtimestamp(hour * 3600, UTC)
        folder = self._dir / f"{start:%Y-%m-%d}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{self._prefix}-{start:%Y%m%dT%H}-{self.session}.jsonl.gz"
        repeat = 0
        while path.exists():  # the clock went back into an hour this session already wrote
            repeat += 1
            path = folder / f"{self._prefix}-{start:%Y%m%dT%H}-{self.session}.{repeat}.jsonl.gz"
        # "x": never overwrite or append. The file stays open across calls, closed by close().
        self._file = gzip.open(path, "xb")  # noqa: SIM115
        self._hour = hour
        self.files.append(path)
        for line in self._open_lines.values():
            self._file.write(_encode(line))

    def _flush(self, now_us: int) -> None:
        assert self._file is not None
        self._file.flush()  # a sync flush: everything so far can be decompressed
        self._last_flush_us = now_us


def _encode(line: dict) -> bytes:
    return json.dumps(line, separators=(",", ":")).encode() + b"\n"


def read_file(path: Path) -> Iterator[dict]:
    """Every complete line of one recording file, in the order written."""
    for raw_line in _gzip_lines(path):
        if raw_line:
            yield json.loads(raw_line)


def _gzip_lines(path: Path) -> Iterator[bytes]:
    """Lines of a gzip file. A file cut short by a crash yields every complete line; the
    partial line at the end is dropped."""
    decompressor = zlib.decompressobj(wbits=31)
    pending = b""
    with open(path, "rb") as f:
        while chunk := f.read(1 << 16):
            data = decompressor.decompress(chunk)
            while decompressor.eof and decompressor.unused_data:  # another gzip member
                rest = decompressor.unused_data
                decompressor = zlib.decompressobj(wbits=31)
                data += decompressor.decompress(rest)
            *lines, pending = (pending + data).split(b"\n")
            yield from lines


def session_of(path: Path) -> str:
    """The session part of a recording file name."""
    name = path.name.removesuffix(".jsonl.gz")
    return name.split("-", 2)[2]


def find_files(
    root: Path, exchange: str, start_us: int, end_us: int, session_prefix: str | None = None
) -> list[Path]:
    """Recording files with data in [start_us, end_us), optionally from one session."""
    found = []
    for hour in range(start_us // HOUR_US, (end_us - 1) // HOUR_US + 1):
        start = datetime.fromtimestamp(hour * 3600, UTC)
        pattern = f"{exchange.lower()}-{start:%Y%m%dT%H}-*.jsonl.gz"
        for path in sorted((root / exchange.lower() / f"{start:%Y-%m-%d}").glob(pattern)):
            if session_prefix is None or session_of(path).startswith(session_prefix):
                found.append(path)
    return found


def read_files(paths: Iterable[Path]) -> Iterator[tuple[str, dict]]:
    """(session, line) from several files, merged by arrival time.

    Each file is already in arrival order; heapq.merge keeps that order and interleaves
    files written at the same time, such as two recorders running side by side.
    """
    streams = [((session_of(path), line) for line in read_file(path)) for path in paths]
    yield from heapq.merge(*streams, key=lambda item: item[1]["recv_us"])
