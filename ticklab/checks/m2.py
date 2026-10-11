"""`make check-m2`: the five checks of milestone M2, with their evidence.

About 3 hours:
1. 120 consecutive live minutes reconciled against Binance's official candles;
2. the same 15 recorded minutes replayed at 1x and at 10x give identical candles;
3. replaying those minutes again (pure duplicates) changes nothing, and Flink counts them;
4. `make ci` (now with the Java tests) passes, and the live stack fits its memory budget;
5. one full day of history, replayed and drained, matches the official candle of every minute.

`--since HH:MM` (UTC, today) starts the live window at a minute already behind us, to reuse a
live run that is already going. Output also goes to data/checks/m2/.
"""

from __future__ import annotations

import argparse
import subprocess
import time
from datetime import UTC, date, datetime, timedelta

from ticklab import binance
from ticklab.checks.common import (
    OUT_DIR,
    Report,
    containers,
    disk_line,
    make,
    prometheus,
    say,
    stamp,
    write,
)
from ticklab.checks.m1 import latest_published_day
from ticklab.clickhouse import ClickHouse
from ticklab.reconciler import Reconciler, binance_feeds, summary
from ticklab.settings import ROOT, Feed, Settings, feeds_for

LIVE_MINUTES = 120
REPLAY_MINUTES = 15
MEMORY_BUDGET_MB = 4600
CANDLE_COLUMNS = (
    "exchange, symbol, minute, open, high, low, close, volume, quote_volume, taker_buy_volume, "
    "taker_buy_quote_volume, vwap, trade_count, first_trade_id, last_trade_id"
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="HH:MM UTC today: start of the live window")
    parser.add_argument(
        "--after-live",
        action="store_true",
        help="skip checks 1 and 4b (already passed) and run the rest",
    )
    args = parser.parse_args()
    settings = Settings.from_env()
    feeds = binance_feeds(feeds_for(binance.EXCHANGE))
    clickhouse = ClickHouse(settings)
    report = Report("M2")
    say(f"# M2 check, started {stamp()} UTC\n\ndisk: {disk_line()}\n")

    make("replay-down")
    if args.after_live:
        hour, minute = map(int, args.since.split(":"))
        start = datetime.now(UTC).replace(hour=hour, minute=minute, second=0, microsecond=0)
        return run_rest(report, settings, clickhouse, feeds, start)
    if not any(c.name == "ticklab-producer-binance-1" for c in containers("ticklab")):
        make("up")  # otherwise the running stack is used untouched: a rebuild could restart it
    if args.since:
        hour, minute = map(int, args.since.split(":"))
        start = datetime.now(UTC).replace(hour=hour, minute=minute, second=0, microsecond=0)
    else:
        start = (datetime.now(UTC) + timedelta(minutes=1)).replace(second=0, microsecond=0)
    end = start + timedelta(minutes=LIVE_MINUTES)
    say(
        f"\nLive window: {start:%H:%M} to {end:%H:%M} UTC; reconciled and re-checked by "
        f"{end + timedelta(minutes=12):%H:%M}"
    )
    wait_until(end + timedelta(minutes=12), clickhouse, start)

    check_live(report, clickhouse, start, end)
    check_memory(report)
    make("down")
    return run_rest(report, settings, clickhouse, feeds, start)


def run_rest(
    report: Report, settings: Settings, clickhouse: ClickHouse, feeds: list[Feed], start: datetime
) -> int:
    """Checks 2, 3, 4a and 5: replays, CI and a day of history."""
    check_replays(report, clickhouse, start, start + timedelta(minutes=REPLAY_MINUTES))
    check_ci(report)
    check_history_day(report, settings, clickhouse, feeds)
    make("replay-down")
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def wait_until(moment: datetime, clickhouse: ClickHouse, start: datetime) -> None:
    while (left := (moment - datetime.now(UTC)).total_seconds()) > 0:
        time.sleep(min(60.0, left))
        progress = clickhouse.value(
            """SELECT count() FROM ticklab.reconciliation FINAL
            WHERE minute >= {start:DateTime64(3)}""",
            start=start.strftime("%F %T"),
        )
        say(
            f"  [{stamp()}] {max(0.0, left - 60) / 60:.0f} min to go; "
            f"{int(progress)} pair-minutes reconciled so far"
        )


def check_live(report: Report, clickhouse: ClickHouse, start: datetime, end: datetime) -> None:
    rows = clickhouse.rows(
        """SELECT symbol, count() AS minutes, countIf(status = 'MATCH') AS matched,
                  countIf(status = 'MISMATCH' AND cause NOT IN ('', 'unknown')) AS explained,
                  countIf(status = 'MISMATCH' AND cause IN ('', 'unknown')) AS unexplained,
                  arrayStringConcat(groupUniqArrayIf(cause, status = 'MISMATCH'), ', ') AS causes
        FROM ticklab.reconciliation FINAL
        WHERE minute >= {start:DateTime64(3)} AND minute < {end:DateTime64(3)}
        GROUP BY symbol ORDER BY symbol""",
        start=start.strftime("%F %T"),
        end=end.strftime("%F %T"),
    )
    say("| Pair | Minutes | Exact match | Mismatch, explained | Unexplained | Causes |")
    say("|---|---:|---:|---:|---:|---|")
    passed = len(rows) == 3
    for row in rows:
        minutes, unexplained = int(row["minutes"]), int(row["unexplained"])
        passed &= minutes == LIVE_MINUTES and unexplained == 0
        say(
            f"| {row['symbol']} | {minutes} | {row['matched']} | {row['explained']} "
            f"| {unexplained} | {row['causes'] or '-'} |"
        )
    mismatches = clickhouse.rows(
        """SELECT symbol, minute, cause, differences FROM ticklab.reconciliation FINAL
        WHERE minute >= {start:DateTime64(3)} AND minute < {end:DateTime64(3)}
          AND status = 'MISMATCH' ORDER BY minute, symbol LIMIT 20""",
        start=start.strftime("%F %T"),
        end=end.strftime("%F %T"),
    )
    for row in mismatches:
        say(f"- {row['symbol']} {row['minute'][:16]} ({row['cause']}): {row['differences'][:200]}")
    blocks = prometheus(
        f'sum(increase(ticklab_ws_frames_total{{kind="block_trade"}}[{LIVE_MINUTES + 15}m]))'
    )
    block_count = round(float(blocks[0]["value"][1])) if blocks else 0
    say(f"Binance block trades seen on the stream in this window: {block_count}")
    report.record(
        f"1. {LIVE_MINUTES} consecutive live minutes per pair: 100% match or explained",
        passed,
        f"({start:%H:%M}-{end:%H:%M} UTC)",
    )


def check_memory(report: Report) -> None:
    states = containers("ticklab")
    say("| Container | State | Memory (MiB) | Limit (MiB) |")
    say("|---|---|---:|---:|")
    for s in states:
        say(f"| {s.name} | {s.health} | {s.memory_mib:,.0f} | {s.limit_mib:,.0f} |")
    total_mb = sum(s.memory_mib for s in states) * 2**20 / 1e6
    say(f"| total | | {total_mb:,.0f} MB | budget {MEMORY_BUDGET_MB:,} MB |")
    healthy = bool(states) and all(s.health == "healthy" for s in states)
    report.record(
        "4b. live stack healthy and within its memory budget",
        healthy and total_mb <= MEMORY_BUDGET_MB,
        f"({len(states)} containers, {total_mb:,.0f} MB of {MEMORY_BUDGET_MB:,} MB)",
    )


def export_candles(clickhouse: ClickHouse) -> str:
    return clickhouse.execute(
        f"SELECT {CANDLE_COLUMNS} FROM ticklab.candles FINAL "
        "ORDER BY exchange, symbol, minute FORMAT TSV"
    )


def duplicates_counted() -> int:
    result = prometheus("sum(flink_taskmanager_job_task_operator_duplicates)")
    return round(float(result[0]["value"][1])) if result else 0


def replayed_trades(start: datetime, end: datetime, speed: str | None) -> int:
    variables = {
        "FROM": f"{start:%Y-%m-%dT%H:%M:%S}",
        "TO": f"{end:%Y-%m-%dT%H:%M:%S}",
        "SESSION": "live-",
    }
    if speed:
        variables["SPEED"] = speed
    command = ["make", "replay", *(f"{k}={v}" for k, v in variables.items())]
    say(f"$ {' '.join(command)}")
    output = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    lines = (output.stdout + output.stderr).splitlines()
    done = [line for line in lines if "replayed" in line and "trades from" in line]
    say(f"  {done[-1].split('INFO ticklab: ')[-1] if done else '(no summary)'}")
    return int(done[-1].split("replayed ")[1].split(" trades")[0]) if done else 0


def wait_for_candles(clickhouse: ClickHouse, quiet_s: float = 20, timeout_s: float = 600) -> None:
    """Until the candles table stops changing (Kafka engine blocks are a few seconds apart)."""
    deadline = time.monotonic() + timeout_s
    last, since = None, time.monotonic()
    while time.monotonic() < deadline:
        state = clickhouse.value("SELECT (count(), sum(trade_count)) FROM ticklab.candles")
        if state != last:
            last, since = state, time.monotonic()
        elif time.monotonic() - since >= quiet_s:
            return
        time.sleep(5)


def check_replays(report: Report, clickhouse: ClickHouse, start: datetime, end: datetime) -> None:
    out = OUT_DIR / "m2"
    make("replay-up")
    trades = replayed_trades(start, end, "1")
    make("replay-drain")
    wait_for_candles(clickhouse)
    at_1x = export_candles(clickhouse)
    write(out / "candles-1x.tsv", at_1x)

    make("replay-up")
    replayed_trades(start, end, "10")
    before = duplicates_counted()
    repeats = replayed_trades(start, end, None)
    deadline = time.monotonic() + 300
    while duplicates_counted() - before < repeats and time.monotonic() < deadline:
        time.sleep(10)
    counted = duplicates_counted() - before
    make("replay-drain")
    wait_for_candles(clickhouse)
    at_10x = export_candles(clickhouse)
    write(out / "candles-10x-plus-duplicates.tsv", at_10x)

    rows_1x, rows_10x = at_1x.splitlines(), at_10x.splitlines()
    differing = len(set(rows_1x) ^ set(rows_10x))
    say(
        f"{trades:,} trades replayed; candles at 1x: {len(rows_1x)}, at 10x then all again: "
        f"{len(rows_10x)}; differing rows: {differing}"
    )
    report.record(
        "2. the same 15 minutes at 1x and at 10x give identical candles",
        differing == 0 and len(rows_1x) > 0,
        f"({len(rows_1x)} candles, {differing} differing rows)",
    )
    say(f"Duplicates: {repeats:,} trades replayed a second time; Flink's dedup counted {counted:,}")
    report.record(
        "3. replayed duplicates change nothing and are counted",
        differing == 0 and counted == repeats,
        f"({counted:,} of {repeats:,} repeats dropped)",
    )


def check_ci(report: Report) -> None:
    result = subprocess.run(["make", "ci"], cwd=ROOT)
    report.record("4a. make ci passes", result.returncode == 0, "(ruff, pytest, Maven verify)")


def check_history_day(
    report: Report, settings: Settings, clickhouse: ClickHouse, feeds: list[Feed]
) -> None:
    day: date = latest_published_day(settings, feeds[0])
    make("history", FROM=day.isoformat())
    make("replay-up")
    make("replay-history", FROM=day.isoformat())
    wait_for_candles(clickhouse, quiet_s=30, timeout_s=1800)
    make("replay-drain")
    wait_for_candles(clickhouse)
    first = datetime(day.year, day.month, day.day, tzinfo=UTC)
    last = first + timedelta(days=1) - timedelta(minutes=1)
    reconciler = Reconciler(settings, feeds)
    results = []
    try:
        for feed in feeds:
            results += reconciler.check(feed, first, last, attempt=1)
    finally:
        reconciler.close()
    say(f"Day {day}, every minute against Binance's official candles:")
    say(summary(results))
    for r in [r for r in results if r.differences][:10]:
        say(f"- {r.feed.symbol} {r.minute:%H:%M} ({r.cause}): {'; '.join(r.differences)[:200]}")
    matched = sum(1 for r in results if not r.differences)
    report.record(
        f"5. history for {day}, replayed and drained: every minute matches",
        matched == len(results) == 3 * 1440,
        f"({matched:,} of {len(results):,} minutes)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
