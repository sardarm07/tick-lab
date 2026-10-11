import gzip
import json

from ticklab.fake_exchange import FakeBinance
from ticklab.fixture import Fixture

ROWS = [
    "SOLUSDT,2075332079,109.62000000,2.00100000,219.34962000,1791504000042368,False,True",
    "SOLUSDT,2075332080,109.61000000,0.18500000,20.27785000,1791504000142804,True,True",
    "SOLUSDT,2075332081,109.61000000,3.26200000,357.54782000,1791504000142804,True,True",
]
KLINE = (
    "SOLUSDT,1791504000000,109.62000000,109.62000000,109.61000000,109.61000000,"
    "5.44800000,1791504059999,597.17529000,3,2.00100000,219.34962000"
)


def fixture(tmp_path) -> Fixture:
    folder = tmp_path / "f"
    folder.mkdir()
    with gzip.open(folder / "trades.csv.gz", "wt") as f:
        f.write("# header\n" + "\n".join(ROWS) + "\n")
    (folder / "klines.csv").write_text("# header\n" + KLINE + "\n")
    (folder / "fixture.json").write_text(json.dumps({"name": "f"}))
    return Fixture(folder)


def test_historical_trades_from_an_id_with_microseconds_or_milliseconds(tmp_path):
    exchange = FakeBinance(fixture(tmp_path))
    page = exchange.historical_trades("SOLUSDT", 2075332080, 1000, micros=True)
    assert [t["id"] for t in page] == [2075332080, 2075332081]
    assert page[0] == {
        "id": 2075332080,
        "price": "109.61000000",
        "qty": "0.18500000",
        "quoteQty": "20.27785000",
        "time": 1791504000142804,
        "isBuyerMaker": True,
        "isBestMatch": True,
    }
    assert exchange.historical_trades("SOLUSDT", 0, 1, micros=False)[0]["time"] == 1791504000042
    assert exchange.historical_trades("BTCUSDT", 0, 10, micros=True) == []


def test_klines_in_a_time_range(tmp_path):
    exchange = FakeBinance(fixture(tmp_path))
    (row,) = exchange.klines("SOLUSDT", 1791504000000, 1791504059999, 1000)
    assert row[0] == 1791504000000 and row[8] == 3 and row[7] == "597.17529000"
    assert exchange.klines("SOLUSDT", 1791504060000, 1791504119999, 1000) == []
