"""Command line: `python -m ticklab <command>`. The Makefile wraps the common ones."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, date, datetime, timedelta

import httpx

from ticklab import binance, bootstrap, history, recorder, replay, study
from ticklab.clickhouse import ClickHouse
from ticklab.kafka import TradePublisher
from ticklab.settings import Settings, feeds_for

log = logging.getLogger("ticklab")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args = _parser().parse_args()
    settings = Settings.from_env()
    return args.run(settings, args) or 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m ticklab")
    commands = parser.add_subparsers(required=True)

    command = commands.add_parser("init", help="create the topic, schema and tables")
    command.set_defaults(run=lambda settings, args: bootstrap.wait_and_run(settings))

    command = commands.add_parser("produce", help="live trades to Kafka, plus recording")
    command.add_argument("exchange", choices=["binance"])
    command.set_defaults(run=_produce)

    command = commands.add_parser("record", help="record live frames only (no Kafka)")
    command.add_argument("exchange", choices=["binance"])
    command.set_defaults(run=_record)

    command = commands.add_parser("history", help="Binance daily trade files")
    command.add_argument("action", choices=["download", "check"])
    _day_range(command)
    command.set_defaults(run=_history)

    command = commands.add_parser("replay", help="replay into the replay stack")
    replays = command.add_subparsers(required=True)
    sub = replays.add_parser("recordings")
    _time_range(sub)
    sub.add_argument("--session", help="only files whose session starts with this")
    sub.add_argument("--speed", type=float, help="1 = as recorded; omit for as fast as possible")
    sub.set_defaults(run=_replay_recordings)
    sub = replays.add_parser("history")
    _day_range(sub)
    sub.add_argument("--speed", type=float, help="1 = real time; omit for as fast as possible")
    sub.set_defaults(run=_replay_history)

    command = commands.add_parser("flink", help="start, stop or drain the core job")
    command.add_argument("action", choices=["start", "stop", "drain", "status"])
    command.set_defaults(run=_flink)

    command = commands.add_parser("reconcile", help="compare our candles with official ones")
    reconciles = command.add_subparsers(required=True)
    sub = reconciles.add_parser("live", help="every minute, as minutes settle")
    sub.add_argument("--metrics-port", type=int, default=8000)
    sub.set_defaults(run=_reconcile_live)
    sub = reconciles.add_parser("range", help="every minute in a range, once")
    _time_range(sub)
    sub.set_defaults(run=_reconcile_range)

    command = commands.add_parser("study", help="the M1 data study")
    studies = command.add_subparsers(required=True)
    sub = studies.add_parser("disorder", help="arrival disorder in recordings")
    _time_range(sub)
    sub.add_argument("--session")
    sub.set_defaults(run=_study_disorder)
    sub = studies.add_parser("gaps", help="gaps between trades in history")
    _day_range(sub)
    sub.set_defaults(run=_study_gaps)
    return parser


def _day_range(command: argparse.ArgumentParser) -> None:
    command.add_argument("--from", dest="first", type=date.fromisoformat, required=True)
    command.add_argument("--to", dest="last", type=date.fromisoformat)


def _time_range(command: argparse.ArgumentParser) -> None:
    command.add_argument("--from", dest="start", type=_utc_time, required=True)
    command.add_argument("--to", dest="end", type=_utc_time, required=True)


def _utc_time(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def _us(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def _produce(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab.producer import run_binance

    asyncio.run(run_binance(settings, publish=True))


def _record(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab.producer import run_binance

    asyncio.run(run_binance(settings, publish=False))


def _history(settings: Settings, args: argparse.Namespace) -> int:
    feeds = feeds_for(binance.EXCHANGE)
    days = history.days(args.first, args.last or args.first)
    if args.action == "download":
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            for day in days:
                for feed in feeds:
                    url = settings.binance_history_url
                    outcome = history.download_day(client, url, settings.history_dir, feed, day)
                    log.info("%s %s: %s", feed.native, day, outcome)
        return 0
    failed = False
    print("| File | SHA-256 | Rows | First ID | Last ID | Contiguous |")
    print("|---|---|---:|---:|---:|---|")
    for feed in feeds:
        previous: history.FileCheck | None = None
        for day in days:
            check = history.check_file(settings.history_dir, feed, day)
            joins = previous is None or (
                previous.last_id is not None and check.first_id == previous.last_id + 1
            )
            if not joins:
                check.problems.append(f"does not start right after {previous.path.name}")
            print(
                f"| {check.path.name} | {'ok' if check.checksum_ok else 'MISMATCH'} "
                f"| {check.rows:,} | {check.first_id} | {check.last_id} "
                f"| {'yes' if not check.problems else 'NO: ' + '; '.join(check.problems)} |"
            )
            failed |= not check.ok
            previous = check
    return 1 if failed else 0


def _require_replay_stack(settings: Settings) -> None:
    stack = ClickHouse(settings).value("SELECT any(name) FROM ticklab.stack")
    if stack != "replay":
        sys.exit(f"refusing to replay: the running stack is {stack!r}, not 'replay'")


def _replay_recordings(settings: Settings, args: argparse.Namespace) -> int:
    _require_replay_stack(settings)
    start, end = _us(args.start), _us(args.end)
    files = recorder.find_files(settings.recordings_dir, binance.EXCHANGE, start, end, args.session)
    if not files:
        sys.exit("no recordings in that range")
    unparsed = replay.Unparsed()
    events = replay.recorded_trades(files, feeds_for(binance.EXCHANGE), start, end, unparsed)
    publisher = TradePublisher(settings, client_id="replay")
    result = replay.run(events, publisher, args.speed)
    log.info(
        "replayed %d trades from %d files in %.1fs (%d frames unparsed)",
        result.trades,
        len(files),
        result.seconds,
        unparsed.count,
    )
    return 1 if result.undelivered or result.failed else 0


def _replay_history(settings: Settings, args: argparse.Namespace) -> int:
    _require_replay_stack(settings)
    days = history.days(args.first, args.last or args.first)
    events = replay.history_trades(settings.history_dir, feeds_for(binance.EXCHANGE), days)
    publisher = TradePublisher(settings, client_id="replay")
    result = replay.run(events, publisher, args.speed)
    log.info("replayed %d trades in %.1fs", result.trades, result.seconds)
    return 1 if result.undelivered or result.failed else 0


def _flink(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab.flinkctl import Flink

    flink = Flink(stack=settings.stack)
    if args.action == "start":
        # Idleness is for live data only: in a replay it would make backlogged trades late.
        idleness = "10" if settings.stack == "live" else "0"
        flink.start(
            [
                "--bootstrap",
                "redpanda:9092",
                "--registry",
                "http://redpanda:8081",
                "--idleness-seconds",
                idleness,
            ]
        )
    elif args.action == "stop":
        flink.stop(drain=False)
    elif args.action == "drain":
        flink.stop(drain=True)
    else:
        for job in flink.jobs():
            print(job["jid"], job["name"], job["state"])


def _reconcile_live(settings: Settings, args: argparse.Namespace) -> None:
    import signal
    import threading

    from prometheus_client import start_http_server

    from ticklab.reconciler import Reconciler, binance_feeds

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    start_http_server(args.metrics_port)
    reconciler = Reconciler(settings, binance_feeds(feeds_for(binance.EXCHANGE)))
    try:
        reconciler.run_live(stop)
    finally:
        reconciler.close()


def _reconcile_range(settings: Settings, args: argparse.Namespace) -> int:
    from ticklab.reconciler import Reconciler, binance_feeds, minute_floor, summary

    first = minute_floor(args.start)
    last = minute_floor(args.end) - timedelta(minutes=1)
    reconciler = Reconciler(settings, binance_feeds(feeds_for(binance.EXCHANGE)))
    results = []
    try:
        for feed in reconciler.feeds:
            results += reconciler.check(feed, first, last, attempt=1)
    finally:
        reconciler.close()
    print(summary(results))
    for r in [r for r in results if r.differences][:10]:
        detail = "; ".join(r.differences)
        print(f"- {r.feed.symbol} {r.minute:%Y-%m-%d %H:%M} ({r.cause}): {detail}")
    return 1 if any(r.differences for r in results) else 0


def _study_disorder(settings: Settings, args: argparse.Namespace) -> None:
    start, end = _us(args.start), _us(args.end)
    files = recorder.find_files(settings.recordings_dir, binance.EXCHANGE, start, end, args.session)
    unparsed = replay.Unparsed()
    events = replay.recorded_trades(files, feeds_for(binance.EXCHANGE), start, end, unparsed)
    label = f"{args.start:%Y-%m-%d %H:%M} to {args.end:%Y-%m-%d %H:%M} UTC, {len(files)} files"
    print(study.disorder_report(study.measure_disorder(events), label))


def _study_gaps(settings: Settings, args: argparse.Namespace) -> None:
    last = args.last or args.first
    days = history.days(args.first, last)
    trades = (
        trade
        for _, trade in replay.history_trades(
            settings.history_dir, feeds_for(binance.EXCHANGE), days
        )
    )
    print(study.gaps_report(study.measure_gaps(trades), args.first, last))


if __name__ == "__main__":
    sys.exit(main())
