"""Recorder -> replay round trip: replayed trades equal what the live producer published."""

import json

from ticklab import binance, replay
from ticklab.recorder import Recorder
from ticklab.settings import Feed

FEEDS = [Feed("BINANCE", "BTC/USDT", "BTCUSDT", 0), Feed("BINANCE", "ETH/USDT", "ETHUSDT", 1)]
BY_NATIVE = {feed.native: feed for feed in FEEDS}
URL = binance.stream_url("wss://stream.binance.com:9443", FEEDS)
T0 = 1791675558000000


def frame(symbol: str, trade_id: int, time: int) -> str:
    data = {
        "e": "trade",
        "E": time + 100,
        "s": symbol,
        "t": trade_id,
        "p": "2508.00000000",
        "q": "0.06410000",
        "T": time,
        "m": trade_id % 2 == 0,
        "M": True,
    }
    return json.dumps({"stream": f"{symbol.lower()}@trade", "data": data})


def record_session(tmp_path):
    """Two overlapping connections, as during a planned reconnect."""
    rec = Recorder(tmp_path, "BINANCE", "live-test")
    live = []  # what the producer published, in order
    rec.open_conn(1, T0, URL)
    for n in range(6):
        received = T0 + 1000 * n
        raw = frame("ETHUSDT" if n % 3 else "BTCUSDT", 100 + n, received - 300)
        rec.frame(1, received, raw)
        live += [(received, t) for t in binance.parse_frame(raw, received, BY_NATIVE, 1).trades]
    rec.open_conn(2, T0 + 6000, URL)
    for n in (5, 6):  # trade 105 arrives on both connections
        received = T0 + 6000 + n
        raw = frame("ETHUSDT", 100 + n, T0 + 5000)
        rec.frame(2, received, raw)
        live += [(received, t) for t in binance.parse_frame(raw, received, BY_NATIVE, 1).trades]
    rec.close_conn(1, T0 + 7000, "planned")
    rec.close()
    return rec.files, live


def test_replay_yields_exactly_what_was_published_live(tmp_path):
    files, live = record_session(tmp_path)
    unparsed = replay.Unparsed()
    replayed = list(replay.recorded_trades(files, FEEDS, 0, 2**62, unparsed))
    assert replayed == live
    assert unparsed.count == 0
    assert len([t for _, t in replayed if t.trade_id == 105]) == 2  # duplicates are kept


def test_replay_respects_the_arrival_window(tmp_path):
    files, live = record_session(tmp_path)
    window = list(replay.recorded_trades(files, FEEDS, T0 + 1000, T0 + 3000, replay.Unparsed()))
    assert window == [item for item in live if T0 + 1000 <= item[0] < T0 + 3000]


def test_frames_that_cannot_be_parsed_are_counted_and_skipped(tmp_path):
    rec = Recorder(tmp_path, "BINANCE", "live-test")
    rec.open_conn(1, T0, URL)
    rec.frame(1, T0 + 1, "not json")
    rec.frame(1, T0 + 2, frame("BTCUSDT", 7, T0))
    rec.close()
    unparsed = replay.Unparsed()
    trades = list(replay.recorded_trades(rec.files, FEEDS, 0, 2**62, unparsed))
    assert [t.trade_id for _, t in trades] == [7]
    assert unparsed.count == 1
