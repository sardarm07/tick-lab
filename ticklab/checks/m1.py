"""`make check-m1`: the six checks of milestone M1, with their evidence.

About 30 minutes: 15 live minutes, a producer crash, a replay of the same 15 minutes, the
unit tests, and one full day of Binance history replayed and counted. It leaves both stacks
stopped. Output also goes to data/checks/m1/.
"""

from __future__ import annotations

import subprocess
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx

from ticklab import binance, binance_rest, history
from ticklab.checks.common import (
    OUT_DIR,
    Report,
    containers,
    disk_line,
    iso,
    make,
    micros,
    prometheus,
    say,
    stamp,
    wait,
    write,
)
from ticklab.clickhouse import ClickHouse
from ticklab.settings import ROOT, Feed, Settings, feeds_for

LIVE_MINUTES = 15
MEMORY_BUDGET_MB = 2500
PRODUCER = "ticklab-producer-binance-1"

WINDOW_FILTER = """
    received_time >= fromUnixTimestamp64Micro({start:Int64})
    AND received_time < fromUnixTimestamp64Micro({end:Int64})"""


def main() -> int:
    settings = Settings.from_env()
    feeds = feeds_for(binance.EXCHANGE)
    clickhouse = ClickHouse(settings)
    report = Report("M1")
    out = OUT_DIR / "m1"
    say(f"# M1 check, started {stamp()} UTC\n\ndisk: {disk_line()}\n")

    make("replay-down")
    make("up")
    start = _next_minute(datetime.now(UTC) + timedelta(seconds=30))
    end = start + timedelta(minutes=LIVE_MINUTES)
    say(f"\nLive window: {start:%H:%M} to {end:%H:%M} UTC (by received time)")
    time.sleep(max(0.0, (start - datetime.now(UTC)).total_seconds()))
    wait(LIVE_MINUTES, "live window", report=lambda: _stored_since(clickhouse, start))
    time.sleep(15)  # the Kafka engine writes in blocks every few seconds

    check_health_and_memory(report)
    window = (micros(start), micros(end))
    check_no_gaps(report, clickhouse, feeds, *window)
    check_against_rest(report, clickhouse, settings, feeds, micros(start + timedelta(minutes=7)))
    live_set = export_set(clickhouse, *window)
    write(out / "live-set.tsv", live_set)
    check_crash_recovery(report, clickhouse)
    make("down")
    check_replay(report, clickhouse, live_set, start, end)
    check_ci(report)
    check_history(report, clickhouse, settings, feeds)
    make("replay-down")
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def _next_minute(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0) + timedelta(minutes=1)


def _stored_since(clickhouse: ClickHouse, start: datetime) -> str:
    count = clickhouse.value(
        "SELECT count() FROM ticklab.trades"
        " WHERE received_time >= fromUnixTimestamp64Micro({s:Int64})",
        s=micros(start),
    )
    return f"{int(count):,} trades stored so far"


def check_health_and_memory(report: Report) -> None:
    states = containers("ticklab")
    say("| Container | State | Memory (MiB) | Limit (MiB) |")
    say("|---|---|---:|---:|")
    for s in states:
        say(f"| {s.name} | {s.health} | {s.memory_mib:,.0f} | {s.limit_mib:,.0f} |")
    total_mb = sum(s.memory_mib for s in states) * 2**20 / 1e6
    say(f"| total | | {total_mb:,.0f} MB | budget {MEMORY_BUDGET_MB:,} MB |")
    healthy = bool(states) and all(s.health == "healthy" for s in states)
    report.record(
        "1. containers healthy, memory within budget",
        healthy and total_mb <= MEMORY_BUDGET_MB,
        f"({len(states)} containers, {total_mb:,.0f} MB of {MEMORY_BUDGET_MB:,} MB)",
    )


def check_no_gaps(
    report: Report, clickhouse: ClickHouse, feeds: list[Feed], start: int, end: int
) -> None:
    rows = clickhouse.rows(
        f"""SELECT symbol, count() AS trades, min(trade_id) AS first, max(trade_id) AS last
        FROM ticklab.trades FINAL
        WHERE exchange = 'BINANCE' AND {WINDOW_FILTER}
        GROUP BY symbol ORDER BY symbol""",
        start=start,
        end=end,
    )
    say("| Pair | count() FINAL | min ID | max ID | max - min + 1 |")
    say("|---|---:|---:|---:|---:|")
    complete = len(rows) == len(feeds)
    for row in rows:
        trades, first, last = int(row["trades"]), int(row["first"]), int(row["last"])
        span = last - first + 1
        complete &= trades == span
        say(f"| {row['symbol']} | {trades:,} | {first} | {last} | {span:,} |")
    report.record(
        "2a. 15 live minutes: no gaps, no duplicates",
        complete,
        "(count equals the ID span for every pair)" if complete else "",
    )


def check_against_rest(
    report: Report, clickhouse: ClickHouse, settings: Settings, feeds: list[Feed], minute: int
) -> None:
    """Every stored trade of one minute against Binance's REST API, field by field. The
    trades just before and after the minute must fall outside it, so none is missing."""
    end = minute + 60_000_000
    say(f"Sampled minute: {iso(minute)} UTC")
    say("| Pair | Ours | Binance | Fields compared | Differences |")
    say("|---|---:|---:|---:|---:|")
    all_match = True
    with httpx.Client(timeout=30) as client:
        for feed in feeds:
            ours = clickhouse.rows(
                """SELECT trade_id, toString(price) AS price, toString(quantity) AS quantity,
                          toString(taker_side) AS side, toUnixTimestamp64Micro(trade_time) AS time
                FROM ticklab.trades FINAL
                WHERE exchange = 'BINANCE' AND symbol = {symbol:String}
                  AND trade_time >= fromUnixTimestamp64Micro({start:Int64})
                  AND trade_time < fromUnixTimestamp64Micro({end:Int64})
                ORDER BY trade_id""",
                symbol=feed.symbol,
                start=minute,
                end=end,
            )
            if not ours:
                all_match = False
                say(f"| {feed.symbol} | 0 | ? | 0 | no trades stored |")
                continue
            first, last = int(ours[0]["trade_id"]), int(ours[-1]["trade_id"])
            theirs = binance_rest.trades_between_ids(
                client, settings.binance_rest_url, feed, first - 1, last + 1
            )
            edges_ok = (
                len(theirs) >= 2 and theirs[0].trade_time < minute and theirs[-1].trade_time >= end
            )
            inside = [t for t in theirs if minute <= t.trade_time < end]
            differences = 0 if edges_ok else 1
            if len(inside) != len(ours):
                differences += abs(len(inside) - len(ours))
            for mine, ref in zip(ours, inside, strict=False):
                differences += sum(
                    [
                        int(mine["trade_id"]) != ref.trade_id,
                        Decimal(mine["price"]) != ref.price,
                        Decimal(mine["quantity"]) != ref.quantity,
                        mine["side"] != ref.taker_side.value,
                        int(mine["time"]) != ref.trade_time,
                    ]
                )
            all_match &= differences == 0
            say(
                f"| {feed.symbol} | {len(ours):,} | {len(inside):,} | {5 * len(ours):,} "
                f"| {differences} |"
            )
    report.record(
        "2b. one minute matches Binance historicalTrades field by field",
        all_match,
        "(ID, price, quantity, side, time in µs)",
    )


def export_set(clickhouse: ClickHouse, start: int, end: int) -> str:
    return clickhouse.execute(
        f"""SELECT exchange, symbol, trade_id FROM ticklab.trades FINAL
        WHERE {WINDOW_FILTER}
        ORDER BY exchange, symbol, trade_id
        FORMAT TSV""",
        start=start,
        end=end,
    )


def check_crash_recovery(report: Report, clickhouse: ClickHouse) -> None:
    def started_at() -> str:
        return subprocess.run(
            ["docker", "inspect", "--format", "{{.State.StartedAt}}", PRODUCER],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    before = started_at()
    killed = micros(datetime.now(UTC))
    say(f"$ docker kill {PRODUCER}   (at {iso(killed)} UTC)")
    subprocess.run(["docker", "kill", PRODUCER], check=True, capture_output=True)
    came_back = "not at all"
    for _ in range(30):
        time.sleep(2)
        if started_at() != before:
            came_back = "by Docker's restart policy"
            break
    else:
        make("up")
        came_back = "after `make up`"
    say(f"The producer came back {came_back}.")
    wait(2, "letting data flow and Prometheus scrape", every_s=30)
    resumed = int(
        clickhouse.value(
            "SELECT count() FROM ticklab.trades"
            " WHERE received_time > fromUnixTimestamp64Micro({t:Int64})",
            t=killed,
        )
    )
    restarts = prometheus('changes(process_start_time_seconds{job="producer-binance"}[10m])')
    restart_count = int(float(restarts[0]["value"][1])) if restarts else 0
    connects = prometheus('sum(ticklab_ws_connects_total{job="producer-binance"})')
    connect_count = int(float(connects[0]["value"][1])) if connects else 0
    gaps = clickhouse.rows(
        """SELECT symbol,
                  maxIf(trade_id, received_time < fromUnixTimestamp64Micro({t:Int64})) AS before,
                  minIf(trade_id, received_time > fromUnixTimestamp64Micro({t:Int64})) AS after
        FROM ticklab.trades FINAL
        WHERE received_time > fromUnixTimestamp64Micro({t:Int64}) - INTERVAL 10 MINUTE
        GROUP BY symbol ORDER BY symbol""",
        t=killed,
    )
    say(
        "Restarts seen by Prometheus (changes in process start time, last 10 min): "
        f"{restart_count}; connections opened by the new process: {connect_count}; "
        f"trades stored since the kill: {resumed:,}"
    )
    say("Trades missed while the producer was down (gap repair, in M3, will fetch these):")
    for row in gaps:
        missed = int(row["after"]) - int(row["before"]) - 1
        say(
            f"  {row['symbol']}: last ID before {row['before']}, "
            f"first after {row['after']}, missed {missed}"
        )
    report.record(
        "3. producer killed: comes back, restart counted, data resumes",
        came_back != "not at all" and restart_count >= 1 and connect_count >= 1 and resumed > 0,
        f"(came back {came_back}; restarts {restart_count}; {resumed:,} trades since)",
    )


def check_replay(
    report: Report, clickhouse: ClickHouse, live_set: str, start: datetime, end: datetime
) -> None:
    make("replay-up")
    make(
        "replay",
        FROM=start.strftime("%Y-%m-%dT%H:%M:%S"),
        TO=end.strftime("%Y-%m-%dT%H:%M:%S"),
        SESSION="live-",
    )
    expected = len(live_set.splitlines())
    replay_set = _wait_for_rows(
        lambda: export_set(clickhouse, micros(start), micros(end)), expected
    )
    write(OUT_DIR / "m1" / "replay-set.tsv", replay_set)
    live_rows, replay_rows = set(live_set.splitlines()), set(replay_set.splitlines())
    say(
        f"live: {len(live_rows):,} distinct trades; replay: {len(replay_rows):,}; "
        f"only live: {len(live_rows - replay_rows)}; only replay: {len(replay_rows - live_rows)}"
    )
    report.record(
        "4. replaying the recording reproduces the live set exactly",
        live_rows == replay_rows,
        f"({len(live_rows):,} (exchange, symbol, trade_id))",
    )


def _wait_for_rows(fetch, expected: int, timeout_s: float = 300) -> str:
    """Poll until the export has the expected number of rows, or stops changing."""
    deadline = time.monotonic() + timeout_s
    last, stable_since = None, time.monotonic()
    while True:
        text = fetch()
        count = len(text.splitlines())
        if count == expected or time.monotonic() > deadline:
            return text
        if count != last:
            last, stable_since = count, time.monotonic()
        elif time.monotonic() - stable_since > 30 and count > 0:
            return text
        say(f"  [{stamp()}] {count:,} of {expected:,} rows stored")
        time.sleep(5)


def check_ci(report: Report) -> None:
    result = subprocess.run(["make", "ci"], cwd=ROOT)
    report.record("5. make ci passes", result.returncode == 0, "(ruff, pytest)")


def latest_published_day(settings: Settings, feed: Feed) -> date:
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for back in range(1, 4):
            day = datetime.now(UTC).date() - timedelta(days=back)
            url = history.file_url(settings.binance_history_url, feed, day) + ".CHECKSUM"
            if client.get(url).status_code == 200:
                return day
    raise RuntimeError("no daily file published in the last 3 days")


def check_history(
    report: Report, clickhouse: ClickHouse, settings: Settings, feeds: list[Feed]
) -> None:
    day = latest_published_day(settings, feeds[0])
    make("history", FROM=day.isoformat())
    checks = [history.check_file(settings.history_dir, feed, day) for feed in feeds]
    make("replay-up")
    make("replay-history", FROM=day.isoformat())
    day_start = micros(datetime(day.year, day.month, day.day, tzinfo=UTC))
    expected = sum(c.rows for c in checks)

    def counts() -> str:
        return clickhouse.execute(
            """SELECT symbol, count() FROM ticklab.trades FINAL
            WHERE source = 'HISTORY'
              AND trade_time >= fromUnixTimestamp64Micro({start:Int64})
              AND trade_time < fromUnixTimestamp64Micro({end:Int64})
            GROUP BY symbol ORDER BY symbol FORMAT TSV""",
            start=day_start,
            end=day_start + history.DAY_US,
        )

    deadline = time.monotonic() + 900
    while True:
        stored = {line.split("\t")[0]: int(line.split("\t")[1]) for line in counts().splitlines()}
        total = sum(stored.values())
        if total >= expected or time.monotonic() > deadline:
            break
        say(f"  [{stamp()}] {total:,} of {expected:,} history rows stored")
        time.sleep(10)
    say(f"Day {day}:")
    say("| Pair | File SHA-256 | Rows in file | IDs contiguous | count() FINAL |")
    say("|---|---|---:|---|---:|")
    passed = True
    for check in checks:
        ok = check.ok and stored.get(check.feed.symbol) == check.rows
        passed &= ok
        say(
            f"| {check.feed.symbol} | {'ok' if check.checksum_ok else 'MISMATCH'} | {check.rows:,} "
            f"| {'yes' if not check.problems else '; '.join(check.problems)} "
            f"| {stored.get(check.feed.symbol, 0):,} |"
        )
    report.record(
        f"6. history for {day}: checksums, contiguous IDs, every row stored once",
        passed,
        f"({expected:,} rows)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
