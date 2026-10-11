"""`make check-m4`: the checks of milestone M4, with their evidence.

About 5 hours:

1. 120 live minutes on both exchanges, every minute of all seven feeds reconciled against the
   exchange's own candles: 100% match, or every mismatch explained.
2. Spread statistics over those minutes (median and p99 of |spread|), one second worked through
   from the stored trades, and every second recomputed from them in plain Python.
3. Two replays of the same day of history, both exchanges, give identical spreads and alerts,
   and every spread equals a plain-Python recomputation from the same history. The alert
   scenarios themselves are Java harness tests, run by `make ci` (check 6).
4. The three failure scripts during the live run, on each exchange: 100% match afterwards.
5. Kraken history for the past 24 hours: trade IDs contiguous for each pair, and the hours
   that Kraken's official candles still cover (12) match after a replay.
6. `make ci` passes, and the live stack stays within its memory budget.

`--skip-live` runs checks 3, 5 and 6a only. `--since HH:MM` re-checks a live window that already
ran (failures included) instead of running a new one. Output also goes to data/checks/m4/.
"""

from __future__ import annotations

import argparse
import subprocess
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, localcontext

from ticklab import binance, kraken, replay, signal_study
from ticklab.checks.common import (
    OUT_DIR,
    Report,
    containers,
    disk_line,
    make,
    say,
    stamp,
    write,
)
from ticklab.checks.m1 import latest_published_day
from ticklab.checks.m2 import wait_for_candles
from ticklab.checks.m3 import sleep_until
from ticklab.clickhouse import ClickHouse
from ticklab.model import Side, Source, Trade
from ticklab.reconciler import Reconciler, summary
from ticklab.settings import ROOT, Settings, feeds_for, load_feeds

LIVE_MINUTES = 120
WARM_UP = timedelta(minutes=10)
SPACING = timedelta(minutes=10)
MEMORY_BUDGET_MB = 4600
FAILURES = (
    ("fail-taskmanager", {}),
    ("fail-producer", {"PRODUCER": "binance"}),
    ("fail-producer", {"PRODUCER": "kraken"}),
    ("fail-network", {"PRODUCER": "binance"}),
    ("fail-network", {"PRODUCER": "kraken"}),
)
OUT = OUT_DIR / "m4"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-live", action="store_true", help="checks 3, 5 and 6a only")
    parser.add_argument("--since", help="HH:MM UTC today: re-check the live window from then")
    args = parser.parse_args()
    settings = Settings.from_env()
    clickhouse = ClickHouse(settings)
    report = Report("M4")
    say(f"# M4 check, started {stamp()} UTC\n\ndisk: {disk_line()}\n")
    make("replay-down")
    if args.since:
        hour, minute = map(int, args.since.split(":"))
        start = datetime.now(UTC).replace(hour=hour, minute=minute, second=0, microsecond=0)
        end = start + timedelta(minutes=LIVE_MINUTES)
        say(f"\nRe-checking the live window {start:%H:%M} to {end:%H:%M} UTC, which already ran")
        check_live(report, clickhouse, start, end, failures=[])
        check_spreads(report, clickhouse, start, end)
        check_memory(report)
        make("down")
    elif not args.skip_live:
        live(report, settings, clickhouse)
    check_kraken_history(report, settings, clickhouse)
    check_replays(report, settings, clickhouse)
    make("replay-down")  # the end-to-end test needs the ports
    result = subprocess.run(["make", "ci"], cwd=ROOT)
    report.record("6a. make ci passes", result.returncode == 0, "(ruff, pytest, Maven, e2e)")
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def live(report: Report, settings: Settings, clickhouse: ClickHouse) -> None:
    make("up")
    start = (datetime.now(UTC) + WARM_UP).replace(second=0, microsecond=0)
    end = start + timedelta(minutes=LIVE_MINUTES)
    say(f"\nLive window: {start:%H:%M} to {end:%H:%M} UTC, all seven feeds")
    sleep_until(start, "warming up")
    failures = []
    for target, variables in FAILURES:
        failures.append((target, variables.get("PRODUCER", ""), datetime.now(UTC)))
        make(target, **variables)
        sleep_until(datetime.now(UTC) + SPACING, f"after {target} {variables}")
    sleep_until(end + timedelta(minutes=12), "live minutes, then reconciliation and re-checks")
    check_live(report, clickhouse, start, end, failures)
    check_spreads(report, clickhouse, start, end)
    check_memory(report)
    make("down")


def check_live(
    report: Report, clickhouse: ClickHouse, start: datetime, end: datetime, failures: list
) -> None:
    span = {"start": start.strftime("%F %T"), "end": end.strftime("%F %T")}
    rows = clickhouse.rows(
        """SELECT toString(exchange) AS exchange, symbol, count() AS minutes,
                  countIf(status = 'MATCH') AS matched,
                  countIf(status = 'MISMATCH' AND cause IN ('', 'unknown')) AS unexplained,
                  arrayStringConcat(groupUniqArrayIf(cause, status = 'MISMATCH'), ', ') AS causes
        FROM ticklab.reconciliation FINAL
        WHERE minute >= {start:DateTime64(3)} AND minute < {end:DateTime64(3)}
        GROUP BY exchange, symbol ORDER BY exchange, symbol""",
        **span,
    )
    if failures:
        say("Failures: " + ", ".join(f"{t} {p} at {at:%H:%M:%S}" for t, p, at in failures))
    say("| Exchange | Pair | Minutes | Exact match | Unexplained | Mismatch causes |")
    say("|---|---|---:|---:|---:|---|")
    complete = len(rows) == len(load_feeds())
    exact = complete
    explained = complete
    for row in rows:
        complete &= int(row["minutes"]) == LIVE_MINUTES
        exact &= row["matched"] == row["minutes"]
        explained &= int(row["unexplained"]) == 0
        say(
            f"| {row['exchange']} | {row['symbol']} | {row['minutes']} | {row['matched']} "
            f"| {row['unexplained']} | {row['causes'] or '-'} |"
        )
    report.record(
        f"1. {LIVE_MINUTES} live minutes on both exchanges: 100% match or explained",
        complete and explained,
        f"({start:%H:%M}-{end:%H:%M} UTC, {len(rows)} feeds)",
    )
    gaps = clickhouse.rows(
        """SELECT toString(exchange) AS exchange, symbol, first_missing_id, last_missing_id,
                  groupArray(toString(status)) AS statuses
        FROM ticklab.gaps FINAL
        WHERE after_trade_time >= {start:DateTime64(3)} AND after_trade_time < {end:DateTime64(3)}
        GROUP BY exchange, symbol, first_missing_id, last_missing_id
        ORDER BY exchange, symbol, first_missing_id""",
        **span,
    )
    say("| Exchange | Pair | Missing IDs | Trades | Statuses |")
    say("|---|---|---|---:|---|")
    repaired = True
    for gap in gaps:
        ids = int(gap["last_missing_id"]) - int(gap["first_missing_id"]) + 1
        statuses = sorted(gap["statuses"])
        repaired &= "REPAIRED" in statuses and "CLOSED" in statuses
        say(
            f"| {gap['exchange']} | {gap['symbol']} | {gap['first_missing_id']}.."
            f"{gap['last_missing_id']} | {ids} | {', '.join(statuses)} |"
        )
    report.record(
        "4. failure scripts on each exchange: 100% match, every gap repaired",
        exact and repaired,
        f"({len(failures)} failures, {len(gaps)} gaps)",
    )


def check_spreads(report: Report, clickhouse: ClickHouse, start: datetime, end: datetime) -> None:
    span = {"start": start.strftime("%F %T"), "end": end.strftime("%F %T")}
    stats = clickhouse.rows(
        """SELECT base, count() AS seconds,
                  quantileExact(0.5)(abs(spread_bps)) AS p50,
                  quantileExact(0.99)(abs(spread_bps)) AS p99,
                  max(abs(spread_bps)) AS widest
        FROM ticklab.spread_1s FINAL
        WHERE second >= {start:DateTime64(3)} AND second < {end:DateTime64(3)}
        GROUP BY base ORDER BY base""",
        **span,
    )
    say("| Asset | Seconds with a spread | Median abs(spread), bps | p99 | Widest |")
    say("|---|---:|---:|---:|---:|")
    for row in stats:
        say(
            f"| {row['base']} | {int(row['seconds']):,} | {Decimal(row['p50']):.2f} "
            f"| {Decimal(row['p99']):.2f} | {Decimal(row['widest']):.2f} |"
        )
    worked = worked_example(clickhouse, start, end)
    agree, total, differing = stream_against_batch(clickhouse, start, end)
    say(f"Every second recomputed from the stored trades: {agree:,} of {total:,} identical")
    # Seconds whose trades came late (repaired, or redelivered after a reconnect) miss the
    # 1-second windows in the job but not the recomputation: they cluster at the failures.
    by_minute: Counter[tuple[str, str, str]] = Counter()
    for line in differing:
        base, _, clock = line.split()[:3]
        kind = "no spread in the job" if "job None" in line else "differs"
        if "recomputed None" in line:
            kind = "no spread recomputed"
        by_minute[(base, clock[:5], kind)] += 1
    if by_minute:
        say("| Asset | Minute (UTC) | Seconds | Kind |")
        say("|---|---|---:|---|")
        for (base, minute, kind), n in sorted(by_minute.items()):
            say(f"| {base} | {minute} | {n} | {kind} |")
    report.record(
        "2. spread statistics, and one second worked through from the trades",
        len(stats) == 3 and worked,
        f"({sum(int(r['seconds']) for r in stats):,} seconds; {agree:,} of {total:,} "
        "identical to a plain-Python recomputation)",
    )


def worked_example(clickhouse: ClickHouse, start: datetime, end: datetime) -> bool:
    """One BTC second from the middle of the window, recomputed from the trades table."""
    middle = start + (end - start) / 2
    row = clickhouse.rows(
        """SELECT second, binance_price, kraken_price, usdt_usd, kraken_price_usdt, spread_bps,
                  kraken_age_ms
        FROM ticklab.spread_1s FINAL
        WHERE base = 'BTC' AND second >= {middle:DateTime64(3)} AND kraken_age_ms > 0
        ORDER BY second LIMIT 1""",
        middle=middle.strftime("%F %T"),
    )[0]
    second = datetime.fromisoformat(row["second"]).replace(tzinfo=UTC)
    kraken_second = second - timedelta(milliseconds=int(row["kraken_age_ms"]))

    def last_price(exchange: str, symbol: str, before: datetime, after: datetime | None) -> str:
        return clickhouse.value(
            """SELECT toString(argMax(price, trade_id)) FROM ticklab.trades FINAL
            WHERE exchange = {exchange:String} AND symbol = {symbol:String}
              AND trade_time < {before:DateTime64(6)}
              AND trade_time >= {after:DateTime64(6)}""",
            exchange=exchange,
            symbol=symbol,
            before=before.strftime("%F %T.%f"),
            after=(after or before - timedelta(hours=1)).strftime("%F %T.%f"),
        )

    one = timedelta(seconds=1)
    b = Decimal(last_price("BINANCE", "BTC/USDT", second + one, second))
    k = Decimal(last_price("KRAKEN", "BTC/USD", kraken_second + one, kraken_second))
    rate = Decimal(last_price("KRAKEN", "USDT/USD", kraken_second + one, None))
    with localcontext() as context:
        context.prec = 60
        converted = (k / rate).quantize(signal_study.TWELVE_PLACES, ROUND_HALF_EVEN)
        bps = ((converted - b) * 10_000 / b).quantize(signal_study.TWELVE_PLACES, ROUND_HALF_EVEN)
    say(f"\nWorked example, BTC, the second from {second:%H:%M:%S} UTC:")
    say(f"- Binance BTC/USDT, last trade of the second: {b.normalize():f}")
    earlier = int(row["kraken_age_ms"]) // 1000
    say(
        f"- Kraken BTC/USD, last trade up to then (in the second from "
        f"{kraken_second:%H:%M:%S}, {earlier} s earlier): {k.normalize():f}"
    )
    say(f"- Kraken USDT/USD, last trade up to the end of that Kraken second: {rate.normalize():f}")
    say(f"- Kraken in USDT: {k.normalize():f} / {rate.normalize():f} = {converted.normalize():f}")
    say(
        f"- Spread: ({converted.normalize():f} - {b.normalize():f}) / {b.normalize():f} x 10,000 "
        f"= {bps.normalize():f} bps"
    )
    stored = (
        Decimal(row["binance_price"]),
        Decimal(row["kraken_price"]),
        Decimal(row["usdt_usd"]),
        Decimal(row["kraken_price_usdt"]),
        Decimal(row["spread_bps"]),
    )
    same = stored == (b, k, rate, converted, bps)
    say(f"- The signals job stored: {', '.join(f'{v.normalize():f}' for v in stored)}")
    say(f"- {'Identical' if same else 'DIFFERENT'}")
    return same


def stream_against_batch(
    clickhouse: ClickHouse, start: datetime, end: datetime
) -> tuple[int, int, list[str]]:
    """The window's spreads, recomputed in plain Python from the stored trades (with half an
    hour before it, for the first Kraken prices and rate), against the signals job's."""
    feeds = {(f.exchange, f.symbol): f for f in load_feeds()}
    rows = clickhouse.rows(
        """SELECT toString(exchange) AS exchange, symbol, trade_id, toString(price) AS price,
                  toString(quantity) AS quantity, toString(taker_side) AS side,
                  toUnixTimestamp64Micro(trade_time) AS t
        FROM ticklab.trades FINAL
        WHERE trade_time >= {start:DateTime64(3)} AND trade_time < {end:DateTime64(3)}
        ORDER BY t, exchange, symbol, trade_id""",
        start=(start - timedelta(minutes=30)).strftime("%F %T"),
        end=end.strftime("%F %T"),
    )

    def events():
        for r in rows:
            feed = feeds[(r["exchange"], r["symbol"])]
            trade = Trade(
                exchange=r["exchange"],
                symbol=r["symbol"],
                base=feed.base,
                quote=feed.quote,
                trade_id=int(r["trade_id"]),
                price=Decimal(r["price"]),
                quantity=Decimal(r["quantity"]),
                taker_side=Side[r["side"]],
                trade_time=int(r["t"]),
                received_time=None,
                source=Source.WS,
            )
            yield trade.trade_time, trade

    ours = {
        (r["base"], r["at"][:19]): Decimal(r["spread_bps"])
        for r in clickhouse.rows(
            """SELECT base, toString(second) AS at, spread_bps FROM ticklab.spread_1s FINAL
            WHERE second >= {start:DateTime64(3)} AND second < {end:DateTime64(3)}""",
            start=start.strftime("%F %T"),
            end=end.strftime("%F %T"),
        )
    }
    begin = int(start.timestamp())
    batch = {
        (row.base, datetime.fromtimestamp(row.second, UTC).strftime("%F %T")): row.spread_bps
        for row in signal_study.spreads(signal_study.second_bars(events()))
        if row.second >= begin
    }
    keys = sorted(set(ours) | set(batch))
    differing = [
        f"{base} {second}: job {ours.get((base, second))}, recomputed {batch.get((base, second))}"
        for base, second in keys
        if ours.get((base, second)) != batch.get((base, second))
    ]
    return len(keys) - len(differing), len(keys), differing


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
        "6b. live stack, both exchanges and both jobs: healthy, within its memory budget",
        healthy and total_mb <= MEMORY_BUDGET_MB,
        f"({len(states)} containers, {total_mb:,.0f} MB of {MEMORY_BUDGET_MB:,} MB)",
    )


def check_kraken_history(report: Report, settings: Settings, clickhouse: ClickHouse) -> None:
    # Ends two minutes ago, so that REST certainly has every trade of the last minute.
    end = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=2)
    start = end - timedelta(hours=24)
    times = {"FROM": f"{start:%Y-%m-%dT%H:%M}", "TO": f"{end:%Y-%m-%dT%H:%M}"}
    command = ["make", "kraken-history", *(f"{k}={v}" for k, v in times.items())]
    say(f"$ {' '.join(command)}")
    result = subprocess.run(command, cwd=ROOT)
    contiguous = result.returncode == 0
    make("replay-up", EXCHANGES="KRAKEN")
    make("replay-kraken-history", **times)
    wait_for_candles(clickhouse, quiet_s=30, timeout_s=1800)
    make("replay-drain")
    wait_for_candles(clickhouse)
    feeds = feeds_for(kraken.EXCHANGE)
    reconciler = Reconciler(settings, feeds)
    results = []
    try:
        for feed in feeds:
            results += reconciler.check(feed, start, end - timedelta(minutes=1), attempt=1)
    finally:
        reconciler.close()
    say(f"Kraken, {start:%m-%d %H:%M} to {end:%m-%d %H:%M} UTC, against Kraken's official candles:")
    say(summary(results))
    for r in [r for r in results if r.differences][:10]:
        say(f"- {r.feed.symbol} {r.minute:%H:%M} ({r.cause}): {'; '.join(r.differences)[:200]}")
    matched = sum(1 for r in results if not r.differences)
    covered = len(results) // max(1, len(feeds))
    report.record(
        "5. Kraken's past 24 hours: IDs contiguous; the 12 hours still official all match",
        contiguous and matched == len(results) and covered >= 700,
        f"({matched:,} of {len(results):,} minutes, {covered} a pair)",
    )


def check_replays(report: Report, settings: Settings, clickhouse: ClickHouse) -> None:
    day: date = latest_published_day(settings, feeds_for(binance.EXCHANGE)[0])
    make("history", FROM=day.isoformat())
    following = day + timedelta(days=1)
    make("kraken-history", FROM=f"{day}T00:00", TO=f"{following}T00:00")
    outputs = []
    for run in (1, 2):
        make("replay-up", EXCHANGES="BINANCE,KRAKEN")
        make("replay-history", FROM=day.isoformat(), EXCHANGES="BINANCE,KRAKEN")
        wait_for_candles(clickhouse, quiet_s=30, timeout_s=3600)
        make("replay-drain")
        wait_for_candles(clickhouse)
        spreads = clickhouse.execute(
            "SELECT * EXCEPT (published_at, inserted_at) FROM ticklab.spread_1s FINAL "
            "ORDER BY base, second FORMAT TSV"
        )
        alerts = clickhouse.execute(
            "SELECT * EXCEPT (inserted_at) FROM ticklab.alerts FINAL "
            "ORDER BY type, key, opened_at FORMAT TSV"
        )
        write(OUT / f"replay-{run}-spreads.tsv", spreads)
        write(OUT / f"replay-{run}-alerts.tsv", alerts)
        outputs.append((spreads, alerts))
    (spreads_1, alerts_1), (spreads_2, alerts_2) = outputs
    say(f"Day {day}, both exchanges, replayed twice:")
    say(f"- spreads: {len(spreads_1.splitlines()):,} and {len(spreads_2.splitlines()):,} rows")
    say(f"- alerts: {len(alerts_1.splitlines())} and {len(alerts_2.splitlines())}")
    for line in alerts_1.splitlines()[:30]:
        say(f"  {line}")
    ours = {}
    for line in spreads_1.splitlines():
        base, second, *_, bps, _, _ = line.split("\t")
        ours[(base, second[:19])] = Decimal(bps)
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    events = replay.merged(
        replay.history_trades(settings.history_dir, feeds_for(binance.EXCHANGE), [day]),
        replay.kraken_history_trades(
            settings.history_dir, feeds_for(kraken.EXCHANGE), start, start + timedelta(days=1)
        ),
    )
    batch = {
        (row.base, datetime.fromtimestamp(row.second, UTC).strftime("%F %T")): row.spread_bps
        for row in signal_study.spreads(signal_study.second_bars(events))
    }
    differing = [k for k in set(ours) | set(batch) if ours.get(k) != batch.get(k)]
    say(f"- plain-Python recomputation: {len(batch):,} spreads, {len(differing)} differ")
    report.record(
        "3. two replays of the same day give identical spreads and alerts, equal to a batch run",
        spreads_1 == spreads_2 and alerts_1 == alerts_2 and bool(spreads_1) and not differing,
        f"({len(spreads_1.splitlines()):,} spreads, {len(alerts_1.splitlines())} alerts)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
