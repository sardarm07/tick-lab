"""`make check-m6`: who moves first (milestone 6), with its evidence.

1. A week of both exchanges' history (2026-10-04 to 10) replayed through the analyses job alone,
   in an empty replay stack: every trade delivered and stored.
2. Stream equals batch: every row of `lead_lag` equals the plain-Python recomputation from the
   same history (`ticklab/lead_lag.py`), and none is missing or extra.
3. The findings, from the job's rows: per base, the pairs, who moved first, and the leads.
4. The "Analyses" dashboard shows the week: every query succeeds and every table has rows.
5. Live, the three jobs run side by side on the lite profile for 10 minutes: none restarts, no
   checkpoint fails once they are running, and the stack stays within its memory budget.

About 1.5 hours, most of it the replay: encoding trades in Python, about 19,000 a second.
`--live-only` runs check 5 alone.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx

from ticklab import binance, history, kraken, lead_lag, replay
from ticklab.checks.common import (
    OUT_DIR,
    Report,
    containers,
    disk_line,
    make,
    say,
    stamp,
    wait,
    write,
)
from ticklab.checks.m4 import MEMORY_BUDGET_MB
from ticklab.checks.m5 import DASHBOARDS, panel_data
from ticklab.clickhouse import ClickHouse
from ticklab.flinkctl import JOBS
from ticklab.model import Trade
from ticklab.settings import Settings, feeds_for

FIRST, LAST = date(2026, 10, 4), date(2026, 10, 10)
START = datetime.combine(FIRST, datetime.min.time(), UTC)
END = datetime.combine(LAST + timedelta(days=1), datetime.min.time(), UTC)
OUT = OUT_DIR / "m6"
FLINK_URL = "http://127.0.0.1:8081"

Row = tuple[str, str, str, int, Decimal, int | None, Decimal | None, int | None, Decimal]
# Per feed: how many trades, the lowest trade ID and the highest.
Feeds = dict[tuple[str, str], tuple[int, int, int]]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-only", action="store_true", help="only check 5")
    args = parser.parse_args()
    settings = Settings.from_env()
    report = Report("M6")
    say(f"# M6 check, started {stamp()} UTC\n\ndisk: {disk_line()}\n")
    make("down")
    if not args.live_only:
        replayed(report, settings)
    check_live(report)
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def replayed(report: Report, settings: Settings) -> None:
    """Checks 1 to 4, on a replay stack."""
    make("replay-up", EXCHANGES="BINANCE,KRAKEN", JOBS="analyses")
    started = time.monotonic()
    make("replay-history", FROM=FIRST.isoformat(), TO=LAST.isoformat(), EXCHANGES="BINANCE,KRAKEN")
    say(f"[{stamp()}] replayed in {(time.monotonic() - started) / 60:.0f} min")
    # Waits until the job has read every trade, then fires every remaining window and timer.
    make("replay-drain")
    clickhouse = ClickHouse(settings)
    settle(clickhouse)

    say(f"[{stamp()}] recomputing the week in plain Python")
    feeds, batch = recompute(settings)
    check_delivery(report, clickhouse, feeds)
    stream = stream_rows(clickhouse)
    check_stream_against_batch(report, stream, batch)
    findings(report, stream)
    check_dashboard(report)
    make("replay-down")


def settle(clickhouse: ClickHouse, quiet_s: float = 30, timeout_s: float = 1800) -> None:
    """Until ClickHouse has stopped storing trades and lead-lag rows."""
    deadline = time.monotonic() + timeout_s
    last, since = None, time.monotonic()
    while time.monotonic() < deadline:
        state = clickhouse.value(
            "SELECT (count(), (SELECT count() FROM ticklab.trades)) FROM ticklab.lead_lag"
        )
        if state != last:
            last, since = state, time.monotonic()
        elif time.monotonic() - since >= quiet_s:
            return
        time.sleep(5)
    raise TimeoutError("ClickHouse kept changing")


def recompute(settings: Settings) -> tuple[Feeds, set[Row]]:
    """The trades of each feed in the week, and the lead-lag rows the definitions give."""
    feeds: dict[tuple[str, str], list[int]] = {}

    def events() -> Iterator[tuple[int, Trade]]:
        for item in replay.merged(
            replay.history_trades(
                settings.history_dir, feeds_for(binance.EXCHANGE), history.days(FIRST, LAST)
            ),
            replay.kraken_history_trades(
                settings.history_dir, feeds_for(kraken.EXCHANGE), START, END
            ),
        ):
            trade = item[1]
            seen = feeds.setdefault((trade.exchange, trade.symbol), [0, trade.trade_id, 0])
            seen[0] += 1
            seen[1] = min(seen[1], trade.trade_id)
            seen[2] = max(seen[2], trade.trade_id)
            yield item

    found = lead_lag.match(lead_lag.moves(events(), lead_lag.THRESHOLDS))
    rows = {
        (
            r.base,
            "UP" if r.up else "DOWN",
            r.leader,
            r.leader_time_us,
            r.leader_bps,
            r.follower_time_us,
            r.follower_bps,
            r.lead_us,
            lead_lag.THRESHOLDS[r.base],
        )
        for r in found
    }
    return {feed: (n, low, high) for feed, (n, low, high) in feeds.items()}, rows


def check_delivery(report: Report, clickhouse: ClickHouse, feeds: Feeds) -> None:
    """Each trade stored once: per feed, the count after merging repeats (FINAL) and the range of
    trade IDs equal the history's."""
    stored: Feeds = {}
    for exchange, symbol in feeds:
        row = clickhouse.rows(
            """SELECT count() AS n, min(trade_id) AS low, max(trade_id) AS high
               FROM ticklab.trades FINAL
               WHERE exchange = {exchange:String} AND symbol = {symbol:String}""",
            exchange=exchange,
            symbol=symbol,
        )[0]
        stored[(exchange, symbol)] = (int(row["n"]), int(row["low"]), int(row["high"]))
    say("| Feed | Trades in the history | Stored | IDs |")
    say("|---|---:|---:|---|")
    for feed in sorted(feeds):
        n, low, high = feeds[feed]
        say(f"| {':'.join(feed)} | {n:,} | {stored[feed][0]:,} | {low} to {high} |")
    trades = sum(n for n, _, _ in feeds.values())
    report.record(
        "1. the week replayed through the analyses job: every trade delivered and stored once",
        bool(feeds) and stored == feeds,
        f"({trades:,} trades)",
    )


def stream_rows(clickhouse: ClickHouse) -> set[Row]:
    def optional(value: str | None, kind: type) -> int | Decimal | None:
        return None if value is None else kind(value)

    rows = clickhouse.rows(
        """SELECT base, toString(direction) AS direction, toString(leader) AS leader,
                  toUnixTimestamp64Micro(leader_time) AS leader_us, leader_bps,
                  toUnixTimestamp64Micro(follower_time) AS follower_us, follower_bps, lead_us,
                  threshold_bps
           FROM ticklab.lead_lag FINAL"""
    )
    return {
        (
            r["base"],
            r["direction"],
            r["leader"],
            int(r["leader_us"]),
            Decimal(r["leader_bps"]),
            optional(r["follower_us"], int),
            optional(r["follower_bps"], Decimal),
            optional(r["lead_us"], int),
            Decimal(r["threshold_bps"]),
        )
        for r in rows
    }


def check_stream_against_batch(report: Report, stream: set[Row], batch: set[Row]) -> None:
    missing, extra = sorted(batch - stream, key=str), sorted(stream - batch, key=str)
    say(f"lead_lag: {len(stream):,} rows from the job, {len(batch):,} from the recomputation")
    for row in missing[:10]:
        say(f"- only in the recomputation: {row}")
    for row in extra[:10]:
        say(f"- only in the job's output: {row}")
    write(OUT / "batch.json", json.dumps(sorted(map(str, batch)), indent=0))
    report.record(
        "2. stream equals batch: the job's lead-lag rows equal the plain-Python recomputation",
        bool(stream) and not missing and not extra,
        f"({len(stream):,} rows, {len(missing)} missing, {len(extra)} extra)",
    )


def wilson(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """A 95% confidence interval for a share."""
    if n == 0:
        return math.nan, math.nan
    p = successes / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return centre - half, centre + half


def findings(report: Report, stream: set[Row]) -> None:
    lines = [
        "| Base | Threshold | Pairs | Binance first | 95% interval | Kraken first | Ties "
        "| Median lead, Binance first | Kraken first | Unfollowed, Binance | Kraken |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for base in sorted({row[0] for row in stream}):
        rows = [row for row in stream if row[0] == base]
        pairs = [row for row in rows if row[7] is not None]
        led = {
            e: [row[7] / 1000 for row in pairs if row[7] and row[2] == e]
            for e in ("BINANCE", "KRAKEN")
        }
        decided = len(led["BINANCE"]) + len(led["KRAKEN"])
        low, high = wilson(len(led["BINANCE"]), decided)
        unfollowed = {e: sum(1 for row in rows if row[7] is None and row[2] == e) for e in led}
        medians = [f"{statistics.median(v):.0f} ms" if v else "-" for v in led.values()]
        lines.append(
            f"| {base} | {rows[0][8].normalize()} bps | {len(pairs):,} "
            f"| {len(led['BINANCE']) / max(1, decided):.0%} | {low:.0%} to {high:.0%} "
            f"| {len(led['KRAKEN']) / max(1, decided):.0%} | {len(pairs) - decided} "
            f"| {medians[0]} | {medians[1]} "
            f"| {unfollowed['BINANCE']:,} | {unfollowed['KRAKEN']:,} |"
        )
    say(f"\nWho moved first, {FIRST} to {LAST}:\n")
    for line in lines:
        say(line)
    write(OUT / "findings.md", "\n".join(lines) + "\n")
    report.record(
        "3. the findings: per base, the pairs, who moved first and the leads",
        bool(stream),
        f"({sum(1 for row in stream if row[7] is not None):,} pairs over 7 days)",
    )


def check_dashboard(report: Report) -> None:
    dashboard = json.loads((DASHBOARDS / "analyses.json").read_text())
    start, end = str(int(START.timestamp() * 1000)), str(int(END.timestamp() * 1000))
    problems = []
    for panel in dashboard["panels"]:
        points, errors = panel_data(dashboard, panel, start, end)
        problems += [f"{panel['title']} {error}" for error in errors]
        if points == 0:
            problems.append(f"{panel['title']}: no rows")
    for line in problems:
        say(f"- {line}")
    report.record(
        "4. the Analyses dashboard shows the week: every query succeeds, every table has rows",
        not problems,
        f"({len(dashboard['panels'])} panels)",
    )


def check_live(report: Report) -> None:
    """A checkpoint that Flink tries while a job's tasks are still starting fails to begin ("Not
    all required tasks are currently running"); that is harmless, so the count of failed
    checkpoints is taken a minute after the start, and must not grow from then on."""
    make("up")
    client = httpx.Client(base_url=FLINK_URL, timeout=30)
    wait(1, "the jobs start")
    started = checkpoint_counts(client)
    wait(9, "live, all three jobs")
    running = checkpoint_counts(client)
    problems = []
    for job in JOBS:
        if job.name not in running or job.name not in started:
            problems.append(f"{job.name} is not running")
            continue
        _, restarts, completed, failed = running[job.name]
        _, _, completed_before, failed_before = started[job.name]
        say(
            f"- {job.name}: {restarts} restarts; checkpoints completed {completed_before} after a "
            f"minute, {completed} after ten; failed {failed_before}, then {failed}"
        )
        if restarts != 0 or failed != failed_before or completed <= completed_before:
            problems.append(job.name)
    states = containers("ticklab")
    for s in states:
        say(f"| {s.name} | {s.health} | {s.memory_mib:,.0f} MiB | limit {s.limit_mib:,.0f} MiB |")
    total_mb = sum(s.memory_mib for s in states) * 2**20 / 1e6
    healthy = bool(states) and all(s.health == "healthy" for s in states)
    make("down")
    report.record(
        "5. live, three jobs on the lite profile: no restarts, checkpoints, within the budget",
        not problems and healthy and total_mb <= MEMORY_BUDGET_MB,
        f"({len(running)} jobs, {total_mb:,.0f} MB of {MEMORY_BUDGET_MB:,} MB)",
    )


def checkpoint_counts(client: httpx.Client) -> dict[str, tuple[str, int, int, int]]:
    """For each running job: its ID, restarts, completed and failed checkpoints."""
    found = {}
    for job in client.get("/jobs/overview").json()["jobs"]:
        if job["state"] != "RUNNING":
            continue
        jid = job["jid"]
        metrics = client.get(f"/jobs/{jid}/metrics", params={"get": "numRestarts"}).json()
        restarts = int(float(metrics[0]["value"])) if metrics else -1
        counts = client.get(f"/jobs/{jid}/checkpoints").json()["counts"]
        found[job["name"]] = (jid, restarts, counts["completed"], counts["failed"])
    return found


if __name__ == "__main__":
    raise SystemExit(main())
