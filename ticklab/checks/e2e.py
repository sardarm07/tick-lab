"""The end-to-end test: a fixture replayed with chaos, then drained, then checked exactly.

It runs against a stack whose gap repair talks to the fake exchange serving the same fixture:
the `ticklab-ci` stack in CI (`make e2e`) or the replay stack (`make chaos`). The fixture is of
one exchange, Binance or Kraken. It passes only if

1. every candle equals the exchange's official one in every field it publishes;
2. every dropped range was reported as a gap and repaired;
3. no trade ended up in late-trades, since everything was injected within the lateness budget;
4. Flink's deduplication dropped at least as many repeats as were injected.
"""

from __future__ import annotations

import time

from ticklab import chaos, replay
from ticklab.candles import compare
from ticklab.checks.common import Report, say, stamp
from ticklab.clickhouse import ClickHouse
from ticklab.fixture import Fixture, load
from ticklab.flinkctl import JOBS, Flink
from ticklab.kafka import TradePublisher
from ticklab.reconciler import our_candles
from ticklab.settings import Settings, feeds_for


def run(settings: Settings, fixture_name: str, seed: int, speed: float) -> int:
    fixture = load(fixture_name)
    feeds = {feed.native: feed for feed in feeds_for(fixture.exchange)}
    clickhouse = ClickHouse(settings)
    stack = clickhouse.value("SELECT any(name) FROM ticklab.stack")
    if stack not in ("ci", "replay"):
        say(f"refusing: the running stack is {stack!r}, not ci or replay")
        return 2
    report = Report(f"End-to-end: {fixture.name}, chaos seed {seed}, {speed:g}x")
    events, injected = chaos.apply(fixture.trades(feeds), seed)
    dropped = sum(len(r) for r in injected.dropped.values())
    say(
        f"[{stamp()}] {len(events):,} arrivals: {injected.duplicates} repeats, "
        f"{injected.delayed} delayed past B, {dropped} ranges of trades dropped"
    )

    publisher = TradePublisher(settings, client_id="chaos")
    result = replay.run(events, publisher, speed)
    say(f"[{stamp()}] replayed {result.trades:,} in {result.seconds:.0f}s")
    settle(clickhouse)
    flink = Flink(stack=stack)
    # Flink's REST API refreshes metrics every 10 s, so the first reading can lag behind.
    deadline = time.monotonic() + 60
    while (duplicates := flink.operator_counter("duplicates")) < injected.duplicates:
        if time.monotonic() > deadline:
            break
        time.sleep(5)
    for job in JOBS:
        flink.stop(job, drain=True)
    settle(clickhouse, quiet_s=20)

    check_candles(report, clickhouse, fixture, feeds)
    check_gaps(report, clickhouse, injected.dropped)
    late = int(clickhouse.value("SELECT count() FROM ticklab.late_trades FINAL"))
    report.record("3. no trade too late for its candle", late == 0, f"({late} in late_trades)")
    report.record(
        "4. repeats removed and counted",
        duplicates >= injected.duplicates,
        f"(dedup dropped {duplicates:,}; {injected.duplicates:,} repeats injected, plus the "
        "delayed trades gap repair had already fetched)",
    )
    return report.finish()


def settle(clickhouse: ClickHouse, quiet_s: float = 30, timeout_s: float = 900) -> None:
    """Until candles and gaps stop changing and every detected gap has an outcome."""
    deadline = time.monotonic() + timeout_s
    last, since = None, time.monotonic()
    while time.monotonic() < deadline:
        state = clickhouse.rows(
            """SELECT (SELECT count() FROM ticklab.candles) AS candles,
                      (SELECT sum(trade_count) FROM ticklab.candles) AS trades,
                      (SELECT countIf(status = 'DETECTED') FROM ticklab.gaps FINAL) AS detected,
                      (SELECT countIf(status IN ('REPAIRED', 'FAILED')) FROM ticklab.gaps FINAL)
                          AS handled"""
        )[0]
        open_gaps = int(state["detected"]) - int(state["handled"])
        if state != last:
            last, since = state, time.monotonic()
        elif open_gaps <= 0 and time.monotonic() - since >= quiet_s:
            return
        time.sleep(5)
    say("settle: timed out")


def check_candles(report: Report, clickhouse: ClickHouse, fixture: Fixture, feeds: dict) -> None:
    compared = mismatched = 0
    for feed, official in fixture.official(feeds).items():
        ours = our_candles(clickhouse, feed, min(official), max(official))
        for minute, theirs in sorted(official.items()):
            differences = compare(ours.get(minute), theirs)
            compared += 1
            if differences:
                mismatched += 1
                say(f"- {feed.symbol} {minute:%H:%M}: {'; '.join(differences)}")
    report.record(
        f"1. every candle equals {fixture.exchange.title()}'s official one",
        mismatched == 0 and compared > 0,
        f"({compared - mismatched} of {compared} candles)",
    )


def check_gaps(report: Report, clickhouse: ClickHouse, dropped: dict) -> None:
    repaired = clickhouse.rows(
        """SELECT symbol, first_missing_id, last_missing_id FROM ticklab.gaps FINAL
        WHERE status = 'REPAIRED'"""
    )
    missing = []
    for symbol, ranges in dropped.items():
        for first, last in ranges:
            covered = any(
                r["symbol"] == symbol
                and int(r["first_missing_id"]) <= first
                and int(r["last_missing_id"]) >= last
                for r in repaired
            )
            if not covered:
                missing.append(f"{symbol} {first}..{last}")
    total = sum(len(r) for r in dropped.values())
    say(
        f"Gaps repaired: {len(repaired)} (dropped ranges, plus single delayed trades that looked "
        "like gaps until they arrived)"
    )
    report.record(
        "2. every dropped range detected and repaired",
        not missing,
        f"({total - len(missing)} of {total})" + (f" missing: {missing}" if missing else ""),
    )
