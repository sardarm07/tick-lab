import hashlib
import zipfile
from datetime import date

import pytest

from ticklab import history
from ticklab.settings import Feed

SOL = Feed("BINANCE", "SOL/USDT", "SOLUSDT", 2)
DAY = date(2026, 10, 9)
DAY_START = 1791504000000000  # 2026-10-09 00:00:00 UTC, in microseconds

# The first rows of the real SOLUSDT file for 2026-10-09.
ROWS = [
    "2075332079,109.62000000,2.00100000,219.34962000,1791504000042368,False,True",
    "2075332080,109.61000000,0.18500000,20.27785000,1791504000142804,True,True",
    "2075332081,109.61000000,3.26200000,357.54782000,1791504000142804,True,True",
]


def write_day(tmp_path, rows):
    path = history.local_path(tmp_path, SOL, DAY)
    path.parent.mkdir(parents=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("SOLUSDT-trades-2026-10-09.csv", "\n".join(rows) + "\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + ".CHECKSUM").write_text(f"{digest}  {path.name}\n")
    return path


def test_names_and_urls_follow_data_binance_vision():
    assert history.file_name(SOL, DAY) == "SOLUSDT-trades-2026-10-09.zip"
    assert history.file_url("https://data.binance.vision", SOL, DAY) == (
        "https://data.binance.vision/data/spot/daily/trades/SOLUSDT/SOLUSDT-trades-2026-10-09.zip"
    )


def test_checksum_lines_are_parsed():
    line = (
        "ac4db3eda0ae767e0fbc2f7619717841d0bf19b62ee2ca9c770372b6768d5ad5"
        "  SOLUSDT-trades-2026-10-09.zip\n"
    )
    assert history.parse_checksum(line) == (
        "ac4db3eda0ae767e0fbc2f7619717841d0bf19b62ee2ca9c770372b6768d5ad5",
        "SOLUSDT-trades-2026-10-09.zip",
    )
    with pytest.raises(ValueError):
        history.parse_checksum("not a checksum")


def test_a_good_file_passes(tmp_path):
    write_day(tmp_path, ROWS)
    check = history.check_file(tmp_path, SOL, DAY)
    assert check.ok, check.problems
    assert (check.rows, check.first_id, check.last_id) == (3, 2075332079, 2075332081)


def test_missing_ids_are_reported(tmp_path):
    write_day(tmp_path, [ROWS[0], ROWS[2]])
    check = history.check_file(tmp_path, SOL, DAY)
    assert check.problems == ["after ID 2075332079 comes 2075332081"]


def test_a_changed_file_fails_its_checksum(tmp_path):
    path = write_day(tmp_path, ROWS)
    path.write_bytes(path.read_bytes() + b"\0")
    assert not history.check_file(tmp_path, SOL, DAY).checksum_ok


def test_trades_outside_the_day_are_reported(tmp_path):
    late = f"2075332082,109.61000000,1.00000000,109.61000000,{DAY_START + 86_400_000_000},True,True"
    write_day(tmp_path, [*ROWS, late])
    assert history.check_file(tmp_path, SOL, DAY).problems == ["ID 2075332082 is outside the day"]
