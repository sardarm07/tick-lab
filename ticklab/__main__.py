"""Command line: `python -m ticklab <command>`. The Makefile wraps the common ones."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

import httpx

from ticklab import binance, bootstrap, history, kraken, kraken_history, recorder, replay, study
from ticklab.clickhouse import ClickHouse
from ticklab.kafka import TradePublisher
from ticklab.model import Trade
from ticklab.settings import Settings, feeds_for, load_feeds

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
    command.add_argument("exchange", choices=["binance", "kraken"])
    command.set_defaults(run=_produce)

    command = commands.add_parser("record", help="record live frames only (no Kafka)")
    command.add_argument("exchange", choices=["binance", "kraken"])
    command.set_defaults(run=_record)

    command = commands.add_parser("history", help="Binance daily trade files")
    command.add_argument("action", choices=["download", "check"])
    _day_range(command)
    command.set_defaults(run=_history)

    command = commands.add_parser("kraken-history", help="Kraken trades from REST, by time")
    command.add_argument("action", choices=["download", "check"])
    _time_range(command)
    command.set_defaults(run=_kraken_history)

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
    sub = replays.add_parser("kraken-history")
    _time_range(sub)
    sub.add_argument("--speed", type=float, help="1 = real time; omit for as fast as possible")
    sub.set_defaults(run=_replay_kraken_history)

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

    command = commands.add_parser("repair", help="gap repair: fetch missing trades")
    command.add_argument("--metrics-port", type=int, default=8000)
    command.set_defaults(run=_repair)

    command = commands.add_parser("fake-exchange", help="serve a fixture as its exchange's API")
    command.add_argument("--fixture", required=True)
    command.add_argument("--port", type=int, default=8090)
    command.set_defaults(run=_fake_exchange)

    command = commands.add_parser("fixture", help="cut a fixture of up to 15 minutes")
    command.add_argument("--source", choices=["history", "recordings", "kraken"], default="history")
    command.add_argument("--from", dest="start", type=_utc_time, required=True)
    command.add_argument("--minutes", type=int, default=15)
    command.add_argument("--name", required=True)
    command.set_defaults(run=_fixture)

    command = commands.add_parser("e2e", help="replay a fixture with chaos and check the result")
    command.add_argument("--fixture", required=True)
    command.add_argument("--seed", type=int, default=7)
    command.add_argument("--speed", type=float, default=10)
    command.set_defaults(run=_e2e)

    command = commands.add_parser("study", help="the M1 data study")
    studies = command.add_subparsers(required=True)
    sub = studies.add_parser("disorder", help="arrival disorder in recordings")
    _time_range(sub)
    sub.add_argument("--session")
    sub.set_defaults(run=_study_disorder)
    sub = studies.add_parser("gaps", help="gaps between trades in history")
    _day_range(sub)
    sub.add_argument("--exchange", choices=["binance", "kraken"], default="binance")
    sub.set_defaults(run=_study_gaps)
    sub = studies.add_parser("signals", help="spreads, moves and volumes behind the alerts")
    _day_range(sub)
    sub.set_defaults(run=_study_signals)
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
    from ticklab.producer import run_feed

    asyncio.run(run_feed(settings, args.exchange.upper(), publish=True))


def _record(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab.producer import run_feed

    asyncio.run(run_feed(settings, args.exchange.upper(), publish=False))


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


def _kraken_history(settings: Settings, args: argparse.Namespace) -> int:
    feeds = feeds_for(kraken.EXCHANGE)
    if args.action == "download":
        with httpx.Client(timeout=60) as client:
            for feed in feeds:
                path, requests = kraken_history.download(
                    client,
                    settings.kraken_rest_url,
                    settings.history_dir,
                    feed,
                    args.start,
                    args.end,
                )
                log.info("%s: %s (%d requests)", feed.symbol, path, requests)
        return 0
    failed = False
    print("| Market | Trades | First ID | Last ID | First trade | Last trade | Contiguous |")
    print("|---|---:|---:|---:|---|---|---|")
    for feed in feeds:
        path = kraken_history.find(settings.history_dir, feed, args.start, args.end)
        if path is None:
            print(f"| {feed.symbol} | no file covers the range | | | | | NO |")
            failed = True
            continue
        trades = list(kraken_history.read(path, feed, args.start, args.end))
        problems = kraken_history.contiguous(trades)
        if not trades:
            problems.append("no trades")
            print(f"| {feed.symbol} | 0 | | | | | NO: no trades |")
        else:
            first, last = trades[0], trades[-1]
            print(
                f"| {feed.symbol} | {len(trades):,} | {first.trade_id} | {last.trade_id} "
                f"| {_time_text(first.trade_time)} | {_time_text(last.trade_time)} "
                f"| {'yes' if not problems else 'NO: ' + '; '.join(problems[:3])} |"
            )
        failed |= bool(problems)
    return 1 if failed else 0


def _time_text(micros: int) -> str:
    return datetime.fromtimestamp(micros / 1e6, UTC).strftime("%Y-%m-%d %H:%M:%S")


def _require_replay_stack(settings: Settings) -> None:
    stack = ClickHouse(settings).value("SELECT any(name) FROM ticklab.stack")
    if stack != "replay":
        sys.exit(f"refusing to replay: the running stack is {stack!r}, not 'replay'")


def _recorded(
    settings: Settings, start: int, end: int, session: str | None, unparsed: replay.Unparsed
) -> tuple[Iterator[tuple[int, Trade]], int]:
    """Recorded trades of the stack's exchanges, merged by arrival time, and the file count."""
    streams = []
    count = 0
    for exchange in settings.exchanges:
        files = recorder.find_files(settings.recordings_dir, exchange, start, end, session)
        count += len(files)
        feeds = feeds_for(exchange)
        streams.append(replay.recorded_trades(files, feeds, start, end, unparsed, exchange))
    return replay.merged(*streams), count


def _replay_recordings(settings: Settings, args: argparse.Namespace) -> int:
    _require_replay_stack(settings)
    start, end = _us(args.start), _us(args.end)
    unparsed = replay.Unparsed()
    events, files = _recorded(settings, start, end, args.session, unparsed)
    if not files:
        sys.exit("no recordings in that range")
    publisher = TradePublisher(settings, client_id="replay")
    result = replay.run(events, publisher, args.speed)
    log.info(
        "replayed %d trades from %d files in %.1fs (%d frames unparsed)",
        result.trades,
        files,
        result.seconds,
        unparsed.count,
    )
    return 1 if result.undelivered or result.failed else 0


def _replay_history(settings: Settings, args: argparse.Namespace) -> int:
    """Whole days of the stack's exchanges, merged by trade time: Binance's daily files, and
    Kraken history files that cover those days."""
    _require_replay_stack(settings)
    last = args.last or args.first
    streams = []
    if binance.EXCHANGE in settings.exchanges:
        days = history.days(args.first, last)
        streams.append(
            replay.history_trades(settings.history_dir, feeds_for(binance.EXCHANGE), days)
        )
    if kraken.EXCHANGE in settings.exchanges:
        start = datetime.combine(args.first, datetime.min.time(), UTC)
        end = datetime.combine(last + timedelta(days=1), datetime.min.time(), UTC)
        feeds = feeds_for(kraken.EXCHANGE)
        streams.append(replay.kraken_history_trades(settings.history_dir, feeds, start, end))
    events = replay.merged(*streams)
    publisher = TradePublisher(settings, client_id="replay")
    result = replay.run(events, publisher, args.speed)
    log.info("replayed %d trades in %.1fs", result.trades, result.seconds)
    return 1 if result.undelivered or result.failed else 0


def _replay_kraken_history(settings: Settings, args: argparse.Namespace) -> int:
    _require_replay_stack(settings)
    feeds = feeds_for(kraken.EXCHANGE)
    events = replay.kraken_history_trades(settings.history_dir, feeds, args.start, args.end)
    publisher = TradePublisher(settings, client_id="replay")
    result = replay.run(events, publisher, args.speed)
    log.info("replayed %d trades in %.1fs", result.trades, result.seconds)
    return 1 if result.undelivered or result.failed else 0


def _flink(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab.flinkctl import CORE, JOBS, SIGNALS, Flink

    flink = Flink(stack=settings.stack)
    if args.action == "start":
        # The feeds of the exchanges this stack carries (TICKLAB_EXCHANGES, default all).
        feeds = [f for f in load_feeds() if f.exchange in settings.exchanges]
        common = [
            "--partitions",
            ",".join(str(f.partition) for f in feeds),
            # Idleness is for live data only: in a replay it would make backlogged trades late.
            "--idleness-seconds",
            "10" if settings.stack == "live" else "0",
            "--bootstrap",
            "redpanda:9092",
            "--registry",
            "http://redpanda:8081",
        ]
        silence = ",".join(f"{f.exchange}:{f.symbol}={f.silence_s}" for f in feeds)
        flink.start(CORE, [*common, "--silence", silence])
        # Live signals start at the newest trade the first time: a day of backlog would only
        # be worked through for nothing. Replay and test stacks start empty, so from the start.
        start_at = "latest" if settings.stack == "live" else "earliest"
        flink.start(SIGNALS, [*common, "--start-at", start_at])
    elif args.action in ("stop", "drain"):
        for job in reversed(JOBS):
            flink.stop(job, drain=args.action == "drain")
    else:
        for job in flink.jobs():
            print(job["jid"], job["name"], job["state"])


def _reconcile_live(settings: Settings, args: argparse.Namespace) -> None:
    import signal
    import threading

    from prometheus_client import start_http_server

    from ticklab.reconciler import Reconciler, reconcilable

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    start_http_server(args.metrics_port)
    reconciler = Reconciler(settings, reconcilable(load_feeds(), settings.exchanges))
    try:
        reconciler.run_live(stop)
    finally:
        reconciler.close()


def _reconcile_range(settings: Settings, args: argparse.Namespace) -> int:
    from ticklab.reconciler import Reconciler, minute_floor, reconcilable, summary

    first = minute_floor(args.start)
    last = minute_floor(args.end) - timedelta(minutes=1)
    reconciler = Reconciler(settings, reconcilable(load_feeds(), settings.exchanges))
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


def _repair(settings: Settings, args: argparse.Namespace) -> None:
    import signal
    import threading

    from prometheus_client import start_http_server

    from ticklab.gap_repair import GapRepair

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    start_http_server(args.metrics_port)
    GapRepair(settings, load_feeds()).run(stop)


def _fake_exchange(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab import fake_exchange, fixture

    fake_exchange.serve(fixture.load(args.fixture), args.port)


def _fixture(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab import fixture

    if args.source == "kraken":
        feeds = feeds_for(kraken.EXCHANGE)
        made = fixture.cut_kraken(settings, feeds, args.start, args.minutes, args.name)
    else:
        feeds = feeds_for(binance.EXCHANGE)
        made = fixture.cut(settings, feeds, args.source, args.start, args.minutes, args.name)
    print(json.dumps(made.meta(), indent=1))


def _e2e(settings: Settings, args: argparse.Namespace) -> int:
    from ticklab.checks import e2e

    return e2e.run(settings, args.fixture, args.seed, args.speed)


def _study_disorder(settings: Settings, args: argparse.Namespace) -> None:
    start, end = _us(args.start), _us(args.end)
    unparsed = replay.Unparsed()
    events, files = _recorded(settings, start, end, args.session, unparsed)
    label = f"{args.start:%Y-%m-%d %H:%M} to {args.end:%Y-%m-%d %H:%M} UTC, {files} files"
    print(study.disorder_report(study.measure_disorder(events), label))


def _study_signals(settings: Settings, args: argparse.Namespace) -> None:
    from ticklab import signal_study

    last = args.last or args.first
    start = datetime.combine(args.first, datetime.min.time(), UTC)
    end = datetime.combine(last + timedelta(days=1), datetime.min.time(), UTC)
    events = replay.merged(
        replay.history_trades(
            settings.history_dir, feeds_for(binance.EXCHANGE), history.days(args.first, last)
        ),
        replay.kraken_history_trades(settings.history_dir, feeds_for(kraken.EXCHANGE), start, end),
    )
    collected = signal_study.collect(events)
    print(f"From {args.first} to {last}, UTC.\n")
    print(signal_study.report(collected, days=(end - start).days))


def _study_gaps(settings: Settings, args: argparse.Namespace) -> None:
    last = args.last or args.first
    if args.exchange == "kraken":
        start = datetime.combine(args.first, datetime.min.time(), UTC)
        end = datetime.combine(last + timedelta(days=1), datetime.min.time(), UTC)
        feeds = feeds_for(kraken.EXCHANGE)
        events = replay.kraken_history_trades(settings.history_dir, feeds, start, end)
    else:
        days = history.days(args.first, last)
        events = replay.history_trades(settings.history_dir, feeds_for(binance.EXCHANGE), days)
    trades = (trade for _, trade in events)
    print(study.gaps_report(study.measure_gaps(trades), args.first, last))


if __name__ == "__main__":
    sys.exit(main())
