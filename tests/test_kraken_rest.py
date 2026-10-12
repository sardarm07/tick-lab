"""Kraken REST parsing and paging, on responses in the shape Kraken returned live on 2026-10-11.
The first and last OHLC rows are real; the middle one is made up, to test a minute without
trades. The trade rows are real."""

import gzip
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx

from ticklab import kraken, kraken_history, kraken_rest
from ticklab.model import Side, Source
from ticklab.settings import Feed

BTC = Feed("KRAKEN", "BTC/USD", "BTC/USD", 3)

OHLC = """{"error": [], "result": {"BTC/USD": [
  [1791655440, "83008.2", "83013.8", "83008.2", "83013.8", "83008.7", "0.02700019", 23],
  [1791655500, "83013.8", "83013.8", "83013.8", "83013.8", "0.0", "0.00000000", 0],
  [1791698640, "82987.1", "82987.2", "82970.8", "82970.8", "82985.6", "0.15141771", 31]],
  "last": 1791698580}}"""


def test_ohlc_keeps_finished_minutes_including_empty_ones():
    candles = kraken_rest.parse_ohlc(OHLC, BTC)
    assert sorted(candles) == [
        datetime(2026, 10, 10, 18, 4, tzinfo=UTC),
        datetime(2026, 10, 10, 18, 5, tzinfo=UTC),
    ]
    first = candles[datetime(2026, 10, 10, 18, 4, tzinfo=UTC)]
    assert first.open == Decimal("83008.2") and first.volume == Decimal("0.02700019")
    assert first.trade_count == 23 and first.quote_volume is None
    assert candles[datetime(2026, 10, 10, 18, 5, tzinfo=UTC)].trade_count == 0


def test_trade_rows_keep_every_digit_of_their_times():
    body = json.loads(
        '{"result": {"BTC/USD": [["82970.90000", "0.00078987", 1791698670.6360314, "b", "l", "",'
        " 110958809]]}}",
        parse_float=Decimal,
    )
    trade = kraken.parse_rest_row(body["result"]["BTC/USD"][0], BTC)
    assert trade.trade_time == 1791698670636031  # truncated from .6360314, not rounded
    assert trade.price == Decimal("82970.9") and trade.taker_side is Side.BUY
    assert trade.trade_id == 110958809


# Real rows of BTC/USD `Trades` pages, 2026-10-11, as Kraken sent them.
ROWS = [
    '["83007.80000","0.00039613",1791708508.4162734,"b","l","",110963981]',
    '["83007.80000","0.00002626",1791708513.2881837,"b","l","",110963982]',
    '["83007.80000","0.00010000",1791708513.2881837,"b","l","",110963983]',
    '["83007.80000","0.00108049",1791708513.2881837,"b","l","",110963984]',
    '["83007.80000","0.00005000",1791708523.2762,"b","l","",110963985]',
    '["83007.80000","0.00045177",1791708523.2762,"b","l","",110963986]',
    '["83007.80000","0.00030318",1791708523.2762,"b","l","",110963987]',
    '["83007.80000","0.00006611",1791708533.0989728,"b","l","",110963988]',
]


def fake_kraken(page_size: int = 4) -> tuple[httpx.Client, list[int]]:
    """Serves ROWS like Kraken: rows at or after `since` (inclusive), in pages, with `last` the
    time of a page's last row. Kraken's own nanosecond times are finer than these rows' times;
    the fake uses the rows' times, so trades of one match share a time here."""
    requests = []

    def time_ns(row: str) -> int:
        return int(json.loads(row, parse_float=Decimal)[2] * 10**9)

    def handler(request: httpx.Request) -> httpx.Response:
        since = int(request.url.params["since"])
        requests.append(since)
        page = [row for row in ROWS if time_ns(row) >= since][:page_size]
        last = time_ns(page[-1]) if page else since
        text = '{"error":[],"result":{"BTC/USD":[' + ",".join(page) + f'],"last":"{last}"}}}}'
        return httpx.Response(200, text=text)

    return httpx.Client(transport=httpx.MockTransport(handler)), requests


def test_pages_between_ids_page_forward_and_yield_each_trade_once():
    client, requests = fake_kraken()
    # The trade before the gap, 110963981, was stored at 1791708508.416273 s; gap repair starts
    # 1 µs earlier.
    since = (kraken.seconds_micros("1791708508.4162734") - 1) * 1000
    waits = []
    pages = kraken_rest.pages_between_ids(
        client, "http://kraken", BTC, since, 110963982, 110963986, lambda: waits.append(1)
    )
    assert [t.trade_id for page in pages for t in page] == list(range(110963982, 110963987))
    assert len(requests) == len(waits) == 3  # pages overlap at their boundary trades


def test_pages_between_ids_stop_at_the_newest_trade():
    client, requests = fake_kraken()
    since = 1791708523276200000
    pages = list(
        kraken_rest.pages_between_ids(
            client, "http://kraken", BTC, since, 110963986, 110963995, lambda: None
        )
    )
    assert [[t.trade_id for t in page] for page in pages] == [[110963986, 110963987, 110963988]]
    assert len(requests) == 2  # the second page holds only the boundary trade


def test_history_files_are_found_by_the_range_they_cover(tmp_path):
    start = datetime(2026, 10, 10, 8, 0, tzinfo=UTC)
    end = datetime(2026, 10, 11, 8, 0, tzinfo=UTC)
    path = kraken_history.path_for(tmp_path, BTC, start, end)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"")
    assert path.name == "20261010T0800_20261011T0800.jsonl.gz"
    inside = (start + timedelta(hours=1), end - timedelta(hours=1))
    assert kraken_history.find(tmp_path, BTC, *inside) == path
    assert kraken_history.find(tmp_path, BTC, start - timedelta(minutes=1), end) is None


def test_history_pages_overlap_but_each_trade_is_read_once(tmp_path):
    path = tmp_path / "pages.jsonl.gz"
    with gzip.open(path, "wt") as f:
        for page in (ROWS[0:4], ROWS[3:8]):  # the second page starts at the first's last trade
            text = '{"error":[],"result":{"BTC/USD":[' + ",".join(page) + '],"last":"0"}}'
            f.write(json.dumps({"since": 0, "recv_us": 0, "url": "", "raw": text}) + "\n")
    start = datetime(2026, 10, 11, 0, 0, tzinfo=UTC)
    trades = list(kraken_history.read(path, BTC, start, start + timedelta(days=1)))
    assert [t.trade_id for t in trades] == list(range(110963981, 110963989))
    assert kraken_history.contiguous(trades) == []
    assert {t.source for t in trades} == {Source.HISTORY}
