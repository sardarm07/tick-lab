"""`make check-m3`: the checks of milestone M3, with their evidence.

About 90 minutes, on the live stack:

1. Three failures, 10 minutes apart: the Flink TaskManager dies, the producer restarts, the
   producer loses the internet for 60 s. Every minute from the first failure until 30 minutes
   after the last must match Binance's official candle exactly, and every gap they caused must
   be reported and repaired.
2. Pausing the producer for 2 minutes (`docker pause`) makes every feed SILENT, then RECOVERED.
3. `make ci` (lint, unit tests, Java tests and the end-to-end test with chaos) passes, and so
   does a chaos run with another seed.
"""

from __future__ import annotations

import subprocess
import time
from datetime import UTC, datetime, timedelta

from ticklab.checks.common import Report, containers, disk_line, make, say, stamp
from ticklab.clickhouse import ClickHouse
from ticklab.settings import ROOT, Settings

SPACING = timedelta(minutes=10)
AFTER = timedelta(minutes=30)
PRODUCER = "ticklab-producer-binance-1"


def main() -> int:
    settings = Settings.from_env()
    clickhouse = ClickHouse(settings)
    report = Report("M3")
    say(f"# M3 check, started {stamp()} UTC\n\ndisk: {disk_line()}\n")
    make("replay-down")
    make("up")
    time.sleep(120)  # let the stack warm up and the reconciler catch up

    failures = []
    for target in ("fail-taskmanager", "fail-producer", "fail-network"):
        failures.append((target, datetime.now(UTC)))
        make(target)
        sleep_until(datetime.now(UTC) + SPACING, f"after {target}")
    paused_at = datetime.now(UTC)
    say(f"$ docker pause {PRODUCER}   ({paused_at:%H:%M:%S} UTC, for 2 minutes)")
    subprocess.run(["docker", "pause", PRODUCER], check=True)
    time.sleep(120)
    subprocess.run(["docker", "unpause", PRODUCER], check=True)

    first = failures[0][1].replace(second=0, microsecond=0)
    last = (paused_at + timedelta(minutes=2)).replace(second=0, microsecond=0) + AFTER
    sleep_until(last + timedelta(minutes=12), "reconciliation and re-checks")
    check_reconciliation(report, clickhouse, first, last, failures)
    check_silence(report, clickhouse, paused_at)
    check_memory()
    make("down")
    for name, target in (
        ("3a. make ci, with the end-to-end test", ["make", "ci"]),
        ("3b. chaos with another seed", ["make", "chaos", "SEED=11"]),
    ):
        result = subprocess.run(target, cwd=ROOT)
        report.record(name, result.returncode == 0, "")
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def sleep_until(moment: datetime, label: str) -> None:
    while (left := (moment - datetime.now(UTC)).total_seconds()) > 0:
        time.sleep(min(60.0, left))
        say(f"  [{stamp()}] {label}: {max(0.0, left - 60) / 60:.0f} min to go")


def check_reconciliation(
    report: Report, clickhouse: ClickHouse, first: datetime, last: datetime, failures: list
) -> None:
    span = {"first": first.strftime("%F %T"), "last": last.strftime("%F %T")}
    rows = clickhouse.rows(
        """SELECT symbol, count() AS minutes, countIf(status = 'MATCH') AS matched,
                  arrayStringConcat(groupUniqArrayIf(cause, status = 'MISMATCH'), ', ') AS causes
        FROM ticklab.reconciliation FINAL
        WHERE minute >= {first:DateTime64(3)} AND minute < {last:DateTime64(3)}
        GROUP BY symbol ORDER BY symbol""",
        **span,
    )
    expected = int((last - first).total_seconds() // 60)
    say("Failures: " + ", ".join(f"{t} at {at:%H:%M:%S}" for t, at in failures))
    say(f"Window {first:%H:%M}-{last:%H:%M} UTC, {expected} minutes per pair")
    say("| Pair | Minutes | Exact match | Mismatch causes |")
    say("|---|---:|---:|---|")
    exact = len(rows) == 3
    for row in rows:
        exact &= int(row["minutes"]) == expected and row["matched"] == row["minutes"]
        say(f"| {row['symbol']} | {row['minutes']} | {row['matched']} | {row['causes'] or '-'} |")
    gaps = clickhouse.rows(
        """SELECT symbol, first_missing_id, last_missing_id,
                  groupArray(toString(status)) AS statuses,
                  dateDiff('millisecond', min(updated_at), max(updated_at)) / 1000 AS seconds
        FROM ticklab.gaps FINAL
        WHERE after_trade_time >= {first:DateTime64(3)} AND after_trade_time < {last:DateTime64(3)}
        GROUP BY symbol, first_missing_id, last_missing_id
        ORDER BY first_missing_id""",
        **span,
    )
    say("| Pair | Missing IDs | Trades | Statuses | Detected to last status (s) |")
    say("|---|---|---:|---|---:|")
    repaired = True
    for gap in gaps:
        ids = int(gap["last_missing_id"]) - int(gap["first_missing_id"]) + 1
        statuses = sorted(gap["statuses"])
        repaired &= "REPAIRED" in statuses and "CLOSED" in statuses
        say(
            f"| {gap['symbol']} | {gap['first_missing_id']}..{gap['last_missing_id']} | {ids} "
            f"| {', '.join(statuses)} | {float(gap['seconds']):.1f} |"
        )
    report.record(
        "1a. every minute around the failures matches exactly",
        exact,
        f"({first:%H:%M}-{last:%H:%M} UTC)",
    )
    report.record(
        "1b. every gap the failures caused was detected, repaired and closed",
        bool(gaps) and repaired,
        f"({len(gaps)} gaps)",
    )


def check_silence(report: Report, clickhouse: ClickHouse, paused_at: datetime) -> None:
    rows = clickhouse.rows(
        """SELECT symbol, toString(event_type) AS event, at, threshold_seconds
        FROM ticklab.feed_health FINAL
        WHERE at >= {start:DateTime64(3)} ORDER BY at""",
        start=paused_at.strftime("%F %T"),
    )
    say("| Pair | Event | At (UTC) | Threshold (s) |")
    say("|---|---|---|---:|")
    for row in rows:
        at, threshold = row["at"][11:19], row["threshold_seconds"]
        say(f"| {row['symbol']} | {row['event']} | {at} | {threshold} |")
    by_pair: dict[str, list[str]] = {}
    for row in rows:
        by_pair.setdefault(row["symbol"], []).append(row["event"])
    ok = len(by_pair) == 3 and all(
        events[:2] == ["SILENT", "RECOVERED"] for events in by_pair.values()
    )
    report.record(
        "2. a 2-minute pause: every feed SILENT, then RECOVERED",
        ok,
        f"({sum(len(v) for v in by_pair.values())} events)",
    )


def check_memory() -> None:
    states = containers("ticklab")
    total_mb = sum(s.memory_mib for s in states) * 2**20 / 1e6
    say(f"Live stack: {len(states)} containers, {total_mb:,.0f} MB")


if __name__ == "__main__":
    raise SystemExit(main())
