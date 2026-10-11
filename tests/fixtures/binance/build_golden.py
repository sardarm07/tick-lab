"""Builds the golden fixtures for tests/test_golden_binance.py. Run it from the repo root:

    PYTHONPATH=. uv run python tests/fixtures/binance/build_golden.py <recording.jsonl.gz> <day>

It takes the first 10 trade frames per pair from a recording and the first 10 rows per pair
from that day's history files (which `make history` must have downloaded). The expected
values come from an independent source: Binance's REST API (`historicalTrades`, with times in
microseconds), asked for the same trade IDs. The test then checks the normaliser against
Binance itself, not against an earlier run of the same code.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import httpx

from ticklab import binance, binance_rest, history, recorder
from ticklab.settings import Settings, feeds_for

HERE = Path(__file__).parent
PER_PAIR = 10


def main(recording: Path, day: date) -> None:
    settings = Settings.from_env()
    feeds = {feed.native: feed for feed in feeds_for(binance.EXCHANGE)}
    frames, taken = [], dict.fromkeys(feeds, 0)
    url = None
    for line in recorder.read_file(recording):
        if line["kind"] == "open":
            url = line["url"]
        if line["kind"] != "frame":
            continue
        native = json.loads(line["raw"])["data"]["s"]
        if taken[native] < PER_PAIR:
            taken[native] += 1
            frames.append({"recv_us": line["recv_us"], "raw": line["raw"]})
        if all(n == PER_PAIR for n in taken.values()):
            break
    rows = []
    for native, feed in feeds.items():
        path = history.local_path(settings.history_dir, feed, day)
        for n, row in enumerate(history.read_rows(path)):
            if n == PER_PAIR:
                break
            rows.append({"native": native, "day": day.isoformat(), "row": row})

    wanted: dict[str, set[int]] = {native: set() for native in feeds}
    for item in frames:
        data = json.loads(item["raw"])["data"]
        wanted[data["s"]].add(data["t"])
    for item in rows:
        wanted[item["native"]].add(int(item["row"][0]))
    reference = {}
    with httpx.Client(timeout=30) as client:
        for native, ids in wanted.items():
            for first, last in _clusters(sorted(ids)):
                for trade in binance_rest.trades_between_ids(
                    client, settings.binance_rest_url, feeds[native], first, last
                ):
                    if trade.trade_id in ids:
                        reference[(native, trade.trade_id)] = trade

    expected = []
    for item in frames:
        data = json.loads(item["raw"])["data"]
        ref = reference[(data["s"], data["t"])]
        expected.append({**ref.to_json(), "received_time": item["recv_us"], "source": "WS"})
    for item in rows:
        ref = reference[(item["native"], int(item["row"][0]))]
        expected.append({**ref.to_json(), "received_time": None, "source": "HISTORY"})

    _write_jsonl(HERE / "ws-frames.jsonl", frames, header={"url": url})
    _write_jsonl(HERE / "history-rows.jsonl", rows)
    (HERE / "expected.json").write_text(json.dumps(expected, indent=1) + "\n")
    print(f"{len(frames)} frames, {len(rows)} history rows, {len(expected)} expected trades")


def _clusters(ids: list[int], max_gap: int = 1000) -> list[tuple[int, int]]:
    """Group sorted IDs into ranges, so distant IDs (history days ago, live today) are
    fetched as separate small ranges rather than everything in between."""
    ranges = [[ids[0], ids[0]]]
    for trade_id in ids[1:]:
        if trade_id - ranges[-1][1] > max_gap:
            ranges.append([trade_id, trade_id])
        else:
            ranges[-1][1] = trade_id
    return [(first, last) for first, last in ranges]


def _write_jsonl(path: Path, items: list[dict], header: dict | None = None) -> None:
    lines = [json.dumps(header)] if header else []
    lines += [json.dumps(item) for item in items]
    path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]), date.fromisoformat(sys.argv[2]))
