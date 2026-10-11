import shutil

from ticklab import recorder
from ticklab.recorder import HOUR_US, Recorder

T0 = 1791675558429090  # 2026-10-10 23:39:18 UTC
URL = "wss://example/stream?streams=btcusdt@trade&timeUnit=MICROSECOND"


def test_frames_come_back_exactly_in_order(tmp_path):
    rec = Recorder(tmp_path, "BINANCE", "live-test")
    rec.open_conn(1, T0, URL)
    frames = [f'{{"n": {n}, "text": "caf\\u00e9 \\"quoted\\""}}' for n in range(100)]
    for n, raw in enumerate(frames):
        rec.frame(1, T0 + n, raw)
    rec.close_conn(1, T0 + 100, "stop")
    rec.close()
    (path,) = rec.files
    assert path.name == "binance-20261010T23-live-test.jsonl.gz"
    lines = list(recorder.read_file(path))
    assert [line["kind"] for line in lines] == ["open"] + ["frame"] * 100 + ["close"]
    assert [line["raw"] for line in lines[1:-1]] == frames


def test_a_new_hour_starts_a_new_file_that_repeats_open_connections(tmp_path):
    rec = Recorder(tmp_path, "BINANCE", "live-test")
    next_hour = (T0 // HOUR_US + 1) * HOUR_US
    rec.open_conn(1, T0, URL)
    rec.frame(1, T0 + 1, "a")
    rec.frame(1, next_hour + 5, "b")
    rec.close()
    _, second = rec.files
    assert second.name == "binance-20261011T00-live-test.jsonl.gz"
    second_lines = list(recorder.read_file(second))
    assert second_lines[0] == {"kind": "open", "conn": 1, "recv_us": T0, "url": URL}
    assert second_lines[1]["raw"] == "b"
    assert recorder.session_of(second) == "live-test"


def test_a_file_cut_short_by_a_crash_keeps_every_flushed_line(tmp_path):
    rec = Recorder(tmp_path, "BINANCE", "live-test", flush_every_s=0)
    rec.open_conn(1, T0, URL)
    for n in range(50):
        rec.frame(1, T0 + n, f"frame {n}")
    (path,) = rec.files
    crashed = tmp_path / "crashed.jsonl.gz"
    shutil.copy(path, crashed)  # the file as a crash would leave it: never closed
    lines = list(recorder.read_file(crashed))
    assert [line["raw"] for line in lines[1:]] == [f"frame {n}" for n in range(50)]
    rec.close()


def test_files_are_found_by_time_and_session(tmp_path):
    for session in ("live-a", "record-b"):
        rec = Recorder(tmp_path, "BINANCE", session)
        rec.open_conn(1, T0, URL)
        rec.frame(1, T0 + 2 * HOUR_US, "later")
        rec.close()
    everything = recorder.find_files(tmp_path, "BINANCE", T0, T0 + 3 * HOUR_US)
    assert len(everything) == 4
    first_hour = recorder.find_files(tmp_path, "BINANCE", T0, T0 + 1)
    assert len(first_hour) == 2
    live_only = recorder.find_files(tmp_path, "BINANCE", T0, T0 + 3 * HOUR_US, "live-")
    assert [recorder.session_of(p) for p in live_only] == ["live-a", "live-a"]


def test_files_written_side_by_side_merge_by_arrival_time(tmp_path):
    a = Recorder(tmp_path, "BINANCE", "live-a")
    b = Recorder(tmp_path, "BINANCE", "live-b")
    a.open_conn(1, T0, URL)
    b.open_conn(1, T0 + 1, URL)
    for n in range(2, 10, 2):
        a.frame(1, T0 + n, f"a{n}")
        b.frame(1, T0 + n + 1, f"b{n + 1}")
    a.close()
    b.close()
    merged = [line.get("raw") for _, line in recorder.read_files(a.files + b.files)]
    assert merged == [None, None, "a2", "b3", "a4", "b5", "a6", "b7", "a8", "b9"]
