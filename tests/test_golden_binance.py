"""Golden test: real Binance frames and history rows, normalised, must equal what Binance's
REST API reports for the same trade IDs (see fixtures/binance/build_golden.py)."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from ticklab import binance
from ticklab.settings import Feed

FIXTURES = Path(__file__).parent / "fixtures" / "binance"
FEEDS = {
    "BTCUSDT": Feed("BINANCE", "BTC/USDT", "BTCUSDT", 0),
    "ETHUSDT": Feed("BINANCE", "ETH/USDT", "ETHUSDT", 1),
    "SOLUSDT": Feed("BINANCE", "SOL/USDT", "SOLUSDT", 2),
}


def load_jsonl(name: str) -> list[dict]:
    return [json.loads(line) for line in (FIXTURES / name).read_text().splitlines() if line]


def test_live_frames_and_history_rows_match_binance_rest():
    header, *frames = load_jsonl("ws-frames.jsonl")
    scale = binance.time_scale(header["url"])
    normalised = []
    for item in frames:
        frame = binance.parse_frame(item["raw"], item["recv_us"], FEEDS, scale)
        normalised += [trade.to_json() for trade in frame.trades]
    for item in load_jsonl("history-rows.jsonl"):
        day = date.fromisoformat(item["day"])
        feed = FEEDS[item["native"]]
        trade = binance.parse_history_row(item["row"], feed, binance.history_time_scale(day))
        normalised.append(trade.to_json())
    expected = json.loads((FIXTURES / "expected.json").read_text())
    assert len(normalised) == len(expected) == 60
    for mine, ref in zip(normalised, expected, strict=True):
        # The same value can be written with different trailing zeros ("0.1", "0.10000000").
        for field in ("price", "quantity"):
            assert Decimal(mine.pop(field)) == Decimal(ref.pop(field))
        assert mine == ref
