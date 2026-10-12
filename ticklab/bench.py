"""Benchmarks: the busiest hour of the history, replayed at a fixed speed, and measured.

`make bench SPEED=1|10|100 PROFILE=lite|full` starts an empty replay stack with the profile's
memory limits and both Flink jobs, then this module:

1. replays both exchanges' trades of the busiest hour in the data study (2026-10-08 15:00 to
   16:00 UTC) at the given speed, sampling memory, consumer lag and checkpoints as it goes;
2. drains the jobs, so every window fires, and waits for storage to settle;
3. measures latency in ClickHouse and writes `bench/results/<profile>-<speed>x.json` and `.md`.

What is measured, and why:

- **Throughput:** trades published per second. At speed S the hour is due in 3600 / S
  seconds. The trades are encoded before the clock starts, so the replay itself isn't what
  limits it; one that took longer was held back by Kafka.
- **Consumer lag:** the Flink sources' `pendingRecords`, the trades published but not yet read.
  A pipeline that keeps up keeps it near zero; one that doesn't lets it grow.
- **Latency,** two of the three definitions (feed latency needs live data):
  - pipeline latency of trades: stored in ClickHouse minus published to Kafka;
  - output latency of candles: emitted by Flink minus the publishing of the newest trade in it.
- **Checkpoints:** the longest checkpoint and the largest one, per job.
- **Memory:** the peak of every container.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ticklab import binance, history, kraken, replay
from ticklab.checks.common import containers, prometheus
from ticklab.clickhouse import ClickHouse
from ticklab.flinkctl import JOBS, Flink
from ticklab.kafka import TradePublisher
from ticklab.model import Trade
from ticklab.settings import ROOT, Settings, feeds_for

log = logging.getLogger(__name__)

BUSIEST_HOUR = datetime(2026, 10, 8, 15, 0, tzinfo=UTC)  # docs/data-study.md
RESULTS = ROOT / "bench" / "results"


def hour_of_trades(settings: Settings, start: datetime) -> Iterator[tuple[int, Trade]]:
    """Both exchanges' trades of the hour from `start`, merged by trade time."""
    end = start + timedelta(hours=1)
    start_us, end_us = int(start.timestamp() * 1e6), int(end.timestamp() * 1e6)

    def binance_hour() -> Iterator[tuple[int, Trade]]:
        days = history.days(start.date(), start.date())
        feeds = feeds_for(binance.EXCHANGE)
        for t, trade in replay.history_trades(settings.history_dir, feeds, days):
            if t >= end_us:
                return
            if t >= start_us:
                yield t, trade

    kraken_hour = replay.kraken_history_trades(
        settings.history_dir, feeds_for(kraken.EXCHANGE), start, end
    )
    return replay.merged(binance_hour(), kraken_hour)


class Sampler(threading.Thread):
    """Peak memory per container, and the largest consumer lag, every few seconds."""

    def __init__(self, every_s: float = 5) -> None:
        super().__init__(daemon=True)
        self.every_s = every_s
        self.memory_mib: dict[str, float] = {}
        self.lag: dict[str, float] = {}
        self._done = threading.Event()

    def run(self) -> None:
        while not self._done.is_set():
            for state in containers("ticklab-replay"):
                name = state.name.removeprefix("ticklab-replay-").removesuffix("-1")
                self.memory_mib[name] = max(self.memory_mib.get(name, 0.0), state.memory_mib)
            for row in prometheus(
                "sum by (job_name) (flink_taskmanager_job_task_operator_pendingRecords)"
            ):
                job = row["metric"].get("job_name", "?")
                self.lag[job] = max(self.lag.get(job, 0.0), float(row["value"][1]))
            self._done.wait(self.every_s)

    def stop(self) -> None:
        self._done.set()
        self.join()


def publish(
    messages: list[tuple[int, int, bytes, bytes]], publisher: TradePublisher, speed: float
) -> replay.ReplayResult:
    """Sends (trade time, partition, key, value) messages, `speed` times faster than the trades
    happened, and waits for every delivery."""
    started = time.monotonic()
    first = messages[0][0]
    for event_us, partition, key, value in messages:
        due = started + (event_us - first) / 1e6 / speed
        while (wait := due - time.monotonic()) > 0:
            publisher.poll(min(wait, 0.1))
        publisher.publish_encoded(partition, key, value)
    undelivered = publisher.flush(300)
    return replay.ReplayResult(
        len(messages), time.monotonic() - started, undelivered, publisher.failed
    )


def quantiles(clickhouse: ClickHouse, sql: str, **params: str) -> dict[str, float]:
    """p50, p95, p99 and max of a column called `ms`, in milliseconds."""
    row = clickhouse.rows(
        f"""SELECT count() AS n, quantilesExact(0.5, 0.95, 0.99)(ms) AS q, max(ms) AS top
        FROM ({sql})""",
        **params,
    )[0]
    p50, p95, p99 = (float(v) for v in row["q"])
    return {"n": int(row["n"]), "p50": p50, "p95": p95, "p99": p99, "max": float(row["top"])}


def checkpoints(seconds: float) -> dict[str, dict[str, float]]:
    window = f"[{max(60, int(seconds))}s:5s]"
    result: dict[str, dict[str, float]] = {}
    for metric, key, scale in (
        ("flink_jobmanager_job_lastCheckpointDuration", "longest_ms", 1),
        ("flink_jobmanager_job_lastCheckpointFullSize", "largest_kb", 1 / 1024),
    ):
        for row in prometheus(f"max by (job_name) (max_over_time({metric}{window}))"):
            job = row["metric"].get("job_name", "?")
            result.setdefault(job, {})[key] = round(float(row["value"][1]) * scale, 1)
    return result


def run(settings: Settings, speed: float, profile: str) -> Path:
    clickhouse = ClickHouse(settings)
    if clickhouse.value("SELECT any(name) FROM ticklab.stack") != "replay":
        raise SystemExit("the bench runs on the replay stack: make bench starts it")
    publisher = TradePublisher(settings, client_id="bench")
    # Encoded before the clock starts: the replay must not be what limits the pipeline.
    log.info("encoding the trades of %s", BUSIEST_HOUR)
    trades = hour_of_trades(settings, BUSIEST_HOUR)
    messages = [(t, *publisher.encode(trade)) for t, trade in trades]
    sampler = Sampler()
    sampler.start()
    started = datetime.now(UTC)
    log.info("replaying %d trades at %gx", len(messages), speed)
    result = publish(messages, publisher, speed)
    replayed_at = time.monotonic()
    flink = Flink(stack="replay")
    for job in JOBS:
        flink.stop(job, drain=True)
    settle(clickhouse)
    sampler.stop()
    finished = datetime.now(UTC)

    span = {"start": started.strftime("%F %T.%f")[:-3], "end": finished.strftime("%F %T.%f")[:-3]}
    pipeline = quantiles(
        clickhouse,
        """SELECT toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(published_at) AS ms
        FROM ticklab.trades
        WHERE published_at BETWEEN {start:DateTime64(3)} AND {end:DateTime64(3)}""",
        **span,
    )
    # The candles of the replayed hour. Flink stamps them in Kafka with their event time, so
    # they are found by minute rather than by publish time.
    output = quantiles(
        clickhouse,
        """SELECT toUnixTimestamp64Milli(emitted_at) - toUnixTimestamp64Milli(input_published_at)
                  AS ms
        FROM ticklab.candles
        WHERE minute >= {hour:DateTime64(3)} AND minute < {hour:DateTime64(3)} + INTERVAL 1 HOUR""",
        hour=BUSIEST_HOUR.strftime("%F %T"),
    )
    due_s = 3600 / speed
    results = {
        "profile": profile,
        "speed": speed,
        "hour": BUSIEST_HOUR.isoformat(),
        "trades": result.trades,
        "replay_seconds": round(result.seconds, 1),
        "due_seconds": round(due_s, 1),
        "trades_per_second": round(result.trades / result.seconds),
        "kept_schedule": result.seconds <= due_s * 1.05,
        "drain_and_settle_seconds": round(time.monotonic() - replayed_at, 1),
        "undelivered": result.undelivered,
        "failed": result.failed,
        "max_consumer_lag": {job: int(v) for job, v in sorted(sampler.lag.items())},
        "pipeline_latency_ms": pipeline,
        "output_latency_ms": output,
        "checkpoints": checkpoints((finished - started).total_seconds()),
        "peak_memory_mib": {k: round(v) for k, v in sorted(sampler.memory_mib.items())},
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{profile}-{speed:g}x.json"
    path.write_text(json.dumps(results, indent=1) + "\n")
    path.with_suffix(".md").write_text(markdown(results))
    log.info("results in %s", path)
    return path


def settle(clickhouse: ClickHouse, quiet_s: float = 20, timeout_s: float = 900) -> None:
    """Until the candles and trades tables stop changing."""
    deadline = time.monotonic() + timeout_s
    last, since = None, time.monotonic()
    while time.monotonic() < deadline:
        state = clickhouse.value(
            "SELECT (count(), (SELECT count() FROM ticklab.trades)) FROM ticklab.candles"
        )
        if state != last:
            last, since = state, time.monotonic()
        elif time.monotonic() - since >= quiet_s:
            return
        time.sleep(5)


def markdown(r: dict) -> str:
    def q(name: str) -> str:
        v = r[name]
        return f"{v['p50']:,.0f} / {v['p95']:,.0f} / {v['p99']:,.0f} / {v['max']:,.0f}"

    lines = [
        f"# {r['profile']}, {r['speed']:g}x",
        "",
        f"The busiest hour of the history ({r['hour']}), both exchanges: {r['trades']:,} trades.",
        "",
        "| Measure | Value |",
        "|---|---|",
        f"| Replay time (due) | {r['replay_seconds']:,} s ({r['due_seconds']:,} s) |",
        f"| Trades published per second | {r['trades_per_second']:,} |",
        f"| Kept to schedule | {'yes' if r['kept_schedule'] else 'no'} |",
        "| Largest consumer lag | "
        + ", ".join(f"{j} {v:,}" for j, v in r["max_consumer_lag"].items())
        + " |",
        f"| Pipeline latency of trades, p50 / p95 / p99 / max (ms) | {q('pipeline_latency_ms')} |",
        f"| Output latency of candles, p50 / p95 / p99 / max (ms) | {q('output_latency_ms')} |",
        "| Longest checkpoint (ms), largest (KB) | "
        + ", ".join(
            f"{j} {c.get('longest_ms', 0):,.0f} / {c.get('largest_kb', 0):,.0f}"
            for j, c in sorted(r["checkpoints"].items())
        )
        + " |",
        "| Peak memory (MiB) | "
        + ", ".join(f"{k} {v:,}" for k, v in r["peak_memory_mib"].items())
        + " |",
        "",
    ]
    return "\n".join(lines)
