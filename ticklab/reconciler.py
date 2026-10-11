"""The reconciler: our candles against the exchange's official ones, minute by minute.

- A minute is compared once it has settled: 6 minutes after it ends, which is the watermark
  bound B (2 s) plus the lateness budget L (5 min) plus a margin. Every field the exchange
  publishes is compared exactly.
- A mismatch is checked once more, 10 minutes after the minute ends, and that result replaces
  the first. Only settled checks count.
- Each mismatch gets a likely cause, checked in this order: the producer was down, trades came
  too late for the candle, trades are missing (a gap not repaired), or unknown.
- Raw responses are recorded like WebSocket frames, and the official candles are stored in
  ClickHouse next to ours.
"""

from __future__ import annotations

import itertools
import logging
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
from prometheus_client import Counter as MetricCounter
from prometheus_client import Gauge

from ticklab import binance, binance_rest, kraken, kraken_rest
from ticklab.candles import Candle, compare
from ticklab.clickhouse import ClickHouse
from ticklab.recorder import Recorder, session_name
from ticklab.settings import Feed, Settings
from ticklab.stream import now_us

log = logging.getLogger(__name__)

SETTLE = timedelta(minutes=6)
RECHECK = timedelta(minutes=10)
MINUTE = timedelta(minutes=1)
LATENESS = timedelta(minutes=5)  # L in the Flink job: how long a candle can still change
# Live frames stopping for longer than this, on all of an exchange's pairs at once, means the
# producer was down: far longer than any normal pause (Kraken also sends heartbeats each second,
# but they aren't stored, and its pairs trade less often).
PRODUCER_SILENCE = {binance.EXCHANGE: timedelta(seconds=10), kraken.EXCHANGE: timedelta(seconds=60)}

CHECKS = MetricCounter(
    "ticklab_reconcile_checks", "Settled minutes compared", ["exchange", "symbol", "status"]
)
SETTLED_UP_TO = Gauge(
    "ticklab_reconcile_settled_minute_seconds", "Start of the newest minute compared", ["symbol"]
)


@dataclass(frozen=True)
class Result:
    feed: Feed
    minute: datetime
    differences: list[str]
    cause: str
    attempt: int

    @property
    def status(self) -> str:
        return "MISMATCH" if self.differences else "MATCH"


def our_candles(
    clickhouse: ClickHouse, feed: Feed, first: datetime, last: datetime
) -> dict[datetime, Candle]:
    """Our candles for minutes first..last, the latest version of each."""
    rows = clickhouse.rows(
        """SELECT minute, open, high, low, close, volume, quote_volume, taker_buy_volume,
                  taker_buy_quote_volume, trade_count
        FROM ticklab.candles FINAL
        WHERE exchange = {exchange:String} AND symbol = {symbol:String}
          AND minute BETWEEN {first:DateTime64(3)} AND {last:DateTime64(3)}""",
        exchange=feed.exchange,
        symbol=feed.symbol,
        first=first.strftime("%F %T"),
        last=last.strftime("%F %T"),
    )
    return {
        _parse_time(row["minute"]): Candle(
            **{k: Decimal(v) for k, v in row.items() if k not in ("minute", "trade_count")},
            trade_count=int(row["trade_count"]),
        )
        for row in rows
    }


def minute_floor(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def _ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def _parse_time(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


class Reconciler:
    def __init__(self, settings: Settings, feeds: list[Feed]) -> None:
        self._settings = settings
        self.feeds = feeds
        self._clickhouse = ClickHouse(settings)
        self._client = httpx.Client(timeout=30)
        self._recorder = Recorder(
            settings.recordings_dir, "official-candles", session_name("reconciler")
        )
        self._requests = 0

    def close(self) -> None:
        self._recorder.close()

    # Fetching both sides.

    def official(self, feed: Feed, first: datetime, last: datetime) -> dict[datetime, Candle]:
        """Official candles for minutes first..last, recorded and stored on the way."""
        candles: dict[datetime, Candle] = {}
        if feed.exchange == kraken.EXCHANGE:
            # Kraken serves only its 720 most recent minutes, all in one response.
            url, text = kraken_rest.ohlc(self._client, self._settings.kraken_rest_url, feed)
            self._record(url, text)
            page = kraken_rest.parse_ohlc(text, feed)
            candles = {m: c for m, c in page.items() if first <= m <= last}
            self._store_official(feed, candles)
            return candles
        start = first
        while start <= last:
            end = min(last, start + 999 * MINUTE)
            url, text = binance_rest.klines(self._client, feed, _ms(start), _ms(end))
            self._record(url, text)
            page = binance_rest.parse_klines(text)
            self._store_official(feed, page)
            candles.update(page)
            start = end + MINUTE
        return candles

    def _record(self, url: str, text: str) -> None:
        """Raw responses, like WebSocket frames: one connection per request."""
        self._requests += 1
        received = now_us()
        self._recorder.open_conn(self._requests, received, url)
        self._recorder.frame(self._requests, received, text)
        self._recorder.close_conn(self._requests, received, "done")

    def ours(self, feed: Feed, first: datetime, last: datetime) -> dict[datetime, Candle]:
        return our_candles(self._clickhouse, feed, first, last)

    def _store_official(self, feed: Feed, candles: dict[datetime, Candle]) -> None:
        if not candles:
            return
        fetched = datetime.now(UTC).strftime("%F %T.%f")[:-3]
        lines = []
        for minute, c in sorted(candles.items()):
            values = [
                feed.exchange,
                feed.symbol,
                minute.strftime("%F %T"),
                c.open,
                c.high,
                c.low,
                c.close,
                c.volume,
                c.quote_volume,
                c.taker_buy_volume,
                c.taker_buy_quote_volume,
                None,
                c.trade_count,
                fetched,
            ]
            lines.append("\t".join("\\N" if v is None else str(v) for v in values))
        self._clickhouse.execute(
            "INSERT INTO ticklab.official_candles FORMAT TabSeparated\n" + "\n".join(lines) + "\n"
        )

    # Comparing.

    def check(self, feed: Feed, first: datetime, last: datetime, attempt: int) -> list[Result]:
        theirs = self.official(feed, first, last)
        ours = self.ours(feed, first, last)
        results = []
        minute = first
        while minute <= last:
            if minute in theirs:
                differences = compare(ours.get(minute), theirs[minute])
                cause = self.cause(feed, minute) if differences else ""
                results.append(Result(feed, minute, differences, cause, attempt))
            minute += MINUTE
        self._write(results)
        return results

    def cause(self, feed: Feed, minute: datetime) -> str:
        span = {
            "exchange": feed.exchange,
            "symbol": feed.symbol,
            "first": minute.strftime("%F %T"),
            "end": (minute + MINUTE).strftime("%F %T"),
        }
        if self._producer_was_silent(feed, minute):
            return "producer down"
        late = self._clickhouse.value(
            """SELECT count() FROM ticklab.late_trades FINAL
            WHERE exchange = {exchange:String} AND symbol = {symbol:String}
              AND trade_time >= {first:DateTime64(3)} AND trade_time < {end:DateTime64(3)}""",
            **span,
        )
        if int(late):
            return "too late"
        return "gap not repaired" if self._missing_ids(feed, minute) else "unknown"

    def _producer_was_silent(self, feed: Feed, minute: datetime) -> bool:
        """Whether live frames stopped for more than PRODUCER_SILENCE during the minute, on any of
        the exchange's feeds (they share one connection). Not applicable to replayed history,
        which has no arrival times."""
        any_live = self._clickhouse.value(
            """SELECT count() FROM (
                SELECT 1 FROM ticklab.trades
                WHERE exchange = {exchange:String} AND received_time IS NOT NULL LIMIT 1)""",
            exchange=feed.exchange,
        )
        if not int(any_live):  # replayed history: there is no producer to have been down
            return False
        start = int((minute - PRODUCER_SILENCE[feed.exchange]).timestamp() * 1_000_000)
        end = int((minute + MINUTE + PRODUCER_SILENCE[feed.exchange]).timestamp() * 1_000_000)
        arrivals = (
            [start]
            + [
                int(row["t"])
                for row in self._clickhouse.rows(
                    """SELECT toUnixTimestamp64Micro(received_time) AS t FROM ticklab.trades
                WHERE exchange = {exchange:String} AND received_time IS NOT NULL
                  AND received_time BETWEEN fromUnixTimestamp64Micro({start:Int64})
                                        AND fromUnixTimestamp64Micro({end:Int64})
                ORDER BY t""",
                    exchange=feed.exchange,
                    start=start,
                    end=end,
                )
            ]
            + [end]
        )
        longest = max(later - earlier for earlier, later in itertools.pairwise(arrivals))
        return longest > PRODUCER_SILENCE[feed.exchange].total_seconds() * 1_000_000

    def _missing_ids(self, feed: Feed, minute: datetime) -> bool:
        """Whether IDs are missing in the minute or right around it."""
        row = self._clickhouse.rows(
            """SELECT count() AS n, min(trade_id) AS lo, max(trade_id) AS hi
            FROM ticklab.trades FINAL
            WHERE exchange = {exchange:String} AND symbol = {symbol:String}
              AND trade_time >= {first:DateTime64(3)} - INTERVAL 1 MINUTE
              AND trade_time < {end:DateTime64(3)} + INTERVAL 1 MINUTE""",
            exchange=feed.exchange,
            symbol=feed.symbol,
            first=minute.strftime("%F %T"),
            end=(minute + MINUTE).strftime("%F %T"),
        )[0]
        return int(row["n"]) == 0 or int(row["hi"]) - int(row["lo"]) + 1 > int(row["n"])

    def _write(self, results: list[Result]) -> None:
        if not results:
            return
        checked = datetime.now(UTC).strftime("%F %T.%f")[:-3]
        lines = []
        for r in results:
            text = "; ".join(r.differences).replace("\t", " ").replace("\n", " ")
            lines.append(
                "\t".join(
                    [
                        r.feed.exchange,
                        r.feed.symbol,
                        r.minute.strftime("%F %T"),
                        checked,
                        r.status,
                        r.cause,
                        text,
                        str(r.attempt),
                    ]
                )
            )
            CHECKS.labels(r.feed.exchange, r.feed.symbol, r.status).inc()
        self._clickhouse.execute(
            "INSERT INTO ticklab.reconciliation FORMAT TabSeparated\n" + "\n".join(lines) + "\n"
        )

    # Running.

    def run_live(self, stop: threading.Event, every_s: float = 60) -> None:
        while not stop.is_set():
            for feed in self.feeds:
                try:
                    self._live_step(feed, datetime.now(UTC))
                except Exception:
                    log.exception("%s: reconciliation step failed", feed.symbol)
            stop.wait(every_s)

    def _live_step(self, feed: Feed, now: datetime) -> None:
        # Settled by the clock, and final in the pipeline: the job has emitted a candle at least L
        # newer, so its watermark is past this minute's lateness budget. While the job catches up
        # on a backlog, the second condition holds the comparison back.
        newest = self._clickhouse.value(
            """SELECT max(minute) FROM ticklab.candles
            WHERE exchange = {exchange:String} AND symbol = {symbol:String}""",
            exchange=feed.exchange,
            symbol=feed.symbol,
        )
        if not newest or newest.startswith("1970"):
            return
        settled = min(minute_floor(now - SETTLE) - MINUTE, _parse_time(newest) - LATENESS)
        first = self._next_unchecked(feed)
        if first is not None and first <= settled:
            results = self.check(feed, first, settled, attempt=1)
            bad = [r for r in results if r.differences]
            log.info(
                "%s: %d minutes checked up to %s, %d mismatched",
                feed.symbol,
                len(results),
                settled.strftime("%H:%M"),
                len(bad),
            )
            SETTLED_UP_TO.labels(feed.symbol).set(settled.timestamp())
        for minute in self._due_rechecks(feed, minute_floor(now - RECHECK) - MINUTE):
            self.check(feed, minute, minute, attempt=2)

    def _next_unchecked(self, feed: Feed) -> datetime | None:
        """The minute after the newest one checked, or else the first full minute of live data."""
        last = self._clickhouse.value(
            """SELECT max(minute) FROM ticklab.reconciliation
            WHERE exchange = {exchange:String} AND symbol = {symbol:String}""",
            exchange=feed.exchange,
            symbol=feed.symbol,
        )
        if last and not last.startswith("1970"):
            return _parse_time(last) + MINUTE
        first_live = self._clickhouse.value(
            """SELECT min(received_time) FROM ticklab.trades
            WHERE exchange = {exchange:String} AND symbol = {symbol:String}
              AND received_time IS NOT NULL""",
            exchange=feed.exchange,
            symbol=feed.symbol,
        )
        if not first_live:
            return None
        return minute_floor(_parse_time(first_live)) + MINUTE

    def _due_rechecks(self, feed: Feed, due: datetime) -> list[datetime]:
        rows = self._clickhouse.rows(
            """SELECT minute FROM ticklab.reconciliation FINAL
            WHERE exchange = {exchange:String} AND symbol = {symbol:String}
              AND status = 'MISMATCH' AND attempt = 1 AND minute <= {due:DateTime64(3)}""",
            exchange=feed.exchange,
            symbol=feed.symbol,
            due=due.strftime("%F %T"),
        )
        return [_parse_time(row["minute"]) for row in rows]


def summary(results: list[Result]) -> str:
    """A table of matches and mismatches per feed, with causes."""
    lines = ["| Feed | Minutes | Match | Mismatch | Causes |", "|---|---:|---:|---:|---|"]
    by_feed: dict[str, list[Result]] = {}
    for r in results:
        by_feed.setdefault(r.feed.symbol, []).append(r)
    for symbol, items in sorted(by_feed.items()):
        bad = [r for r in items if r.differences]
        causes = ", ".join(f"{c} {n}" for c, n in Counter(r.cause for r in bad).items()) or "-"
        matched = len(items) - len(bad)
        lines.append(f"| {symbol} | {len(items):,} | {matched:,} | {len(bad)} | {causes} |")
    return "\n".join(lines)


def reconcilable(feeds: list[Feed], exchanges: tuple[str, ...]) -> list[Feed]:
    """Every feed of the given exchanges: both publish official 1-minute candles."""
    return [feed for feed in feeds if feed.exchange in exchanges]
