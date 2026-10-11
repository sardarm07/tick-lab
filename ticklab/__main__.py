"""Command line: `python -m ticklab <command>`. The Makefile wraps the common ones."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, date, datetime

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
