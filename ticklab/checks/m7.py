"""`make check-m7`: round-number magnets (milestone 7), with its evidence.

1. Two days of Binance history (2026-10-07 and 08, the busiest of the study week) replayed
   through the analyses job alone, in an empty replay stack: every trade delivered and stored.
2. Stream equals batch: every level test the job decided equals the plain-Python recomputation
   from the same history (`ticklab/round_levels.py`), and none is missing or extra.
3. The findings, from four weeks of history (2026-09-12 to 10-10) by the same definitions: per
   pair, tier and kind of level, the outcomes, and the bounce share with its 95% interval.
4. The "Analyses" dashboard shows the replayed days: every query succeeds, and the round-level
   tables have rows (Binance alone gives the lead-lag tables no pairs).

About 1.5 hours: the replay, then four weeks of trades in plain Python.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

from ticklab import binance, history, replay, round_levels
from ticklab.checks.common import OUT_DIR, Report, disk_line, make, say, stamp, write
from ticklab.checks.m6 import Feeds, check_dashboard, check_delivery, settle
from ticklab.clickhouse import ClickHouse
from ticklab.model import Trade
from ticklab.round_levels import LevelTest, LevelTester, wilson
from ticklab.settings import Settings, feeds_for

REPLAYED = (date(2026, 10, 7), date(2026, 10, 8))
FINDINGS = (date(2026, 9, 12), date(2026, 10, 10))
OUT = OUT_DIR / "m7"

Row = tuple[str, str, str, int, str, int, int, int, int, str, int]
LEVEL_PANELS = {"Round levels against random ones", "Latest level tests"}


def main() -> int:
    settings = Settings.from_env()
    report = Report("M7")
    say(f"# M7 check, started {stamp()} UTC\n\ndisk: {disk_line()}\n")
    first, last = REPLAYED
    make("down")
    make("replay-up", EXCHANGES="BINANCE", JOBS="analyses")
    started = time.monotonic()
    make("replay-history", FROM=first.isoformat(), TO=last.isoformat(), EXCHANGES="BINANCE")
    say(f"[{stamp()}] replayed in {(time.monotonic() - started) / 60:.0f} min")
    make("replay-drain")
    clickhouse = ClickHouse(settings)
    settle(clickhouse, "round_levels")

    say(f"[{stamp()}] recomputing the replayed days in plain Python")
    feeds, tester = recompute(settings, first, last)
    check_delivery(report, clickhouse, feeds, "1. two days replayed through the analyses job")
    check_stream_against_batch(
        report, stream_rows(clickhouse), Counter(row(t) for t in tester.decided)
    )
    start = datetime.combine(first, datetime.min.time(), UTC)
    end = datetime.combine(last + timedelta(days=1), datetime.min.time(), UTC)
    check_dashboard(
        report, start, end, "4. the Analyses dashboard shows the replayed days", LEVEL_PANELS
    )
    make("replay-down")

    say(f"[{stamp()}] four weeks of history in plain Python, for the findings")
    _, tester = recompute(settings, *FINDINGS)
    findings(report, tester)
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def recompute(settings: Settings, first: date, last: date) -> tuple[Feeds, LevelTester]:
    """Binance's trades from `first` to `last`, through the definitions; and per feed, how many
    trades there were, and the lowest and highest trade ID."""
    feeds: dict[tuple[str, str], list[int]] = {}

    def events() -> Iterator[Trade]:
        for _, trade in replay.history_trades(
            settings.history_dir, feeds_for(binance.EXCHANGE), history.days(first, last)
        ):
            seen = feeds.setdefault((trade.exchange, trade.symbol), [0, trade.trade_id, 0])
            seen[0] += 1
            seen[1] = min(seen[1], trade.trade_id)
            seen[2] = max(seen[2], trade.trade_id)
            yield trade

    tester = LevelTester(round_levels.BANDS)
    for trade in events():
        tester.add(trade)
    tester.finish()
    return {feed: (n, low, high) for feed, (n, low, high) in feeds.items()}, tester


def row(test: LevelTest) -> Row:
    assert test.close is not None and test.outcome is not None
    return (
        test.base,
        test.tier,
        test.kind,
        test.level,
        "FROM_BELOW" if test.up else "FROM_ABOVE",
        test.time_us,
        test.trade_id,
        test.decide_second,
        test.close,
        test.outcome,
        test.band,
    )


def stream_rows(clickhouse: ClickHouse) -> Counter[Row]:
    """Every row, repeats included: the table's key would hide two tests that collide."""
    rows = clickhouse.rows(
        """SELECT base, toString(tier) AS tier, toString(kind) AS kind,
                  toInt64(level * 100) AS level, toString(approach) AS approach,
                  toUnixTimestamp64Micro(touch_time) AS touch_us, touch_trade_id,
                  intDiv(toUnixTimestamp64Milli(decide_second), 1000) AS decide,
                  toInt64(close * 100) AS close, toString(outcome) AS outcome,
                  toInt64(band * 100) AS band
           FROM ticklab.round_levels FINAL"""
    )
    return Counter(
        (
            r["base"],
            r["tier"],
            r["kind"],
            int(r["level"]),
            r["approach"],
            int(r["touch_us"]),
            int(r["touch_trade_id"]),
            int(r["decide"]),
            int(r["close"]),
            r["outcome"],
            int(r["band"]),
        )
        for r in rows
    )


def check_stream_against_batch(report: Report, stream: Counter[Row], batch: Counter[Row]) -> None:
    """Counts as well as rows: a test twice in either is a difference."""
    missing, extra = sorted((batch - stream).elements()), sorted((stream - batch).elements())
    say(
        f"round_levels: {stream.total():,} tests from the job, {batch.total():,} from the "
        f"recomputation, {len(batch):,} distinct"
    )
    for r in missing[:10]:
        say(f"- only in the recomputation: {r}")
    for r in extra[:10]:
        say(f"- only in the job's output: {r}")
    report.record(
        "2. stream equals batch: the job's level tests equal the plain-Python recomputation",
        bool(stream) and not missing and not extra,
        f"({stream.total():,} tests, {len(missing)} missing, {len(extra)} extra)",
    )


def findings(report: Report, tester: LevelTester) -> None:
    lines = [
        "| Pair | Tier | Levels | Tests | Break | Bounce | Neutral | Bounce share of decided "
        "| 95% interval |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for base in sorted(round_levels.BANDS):
        for tier in round_levels.TIERS:
            for kind in round_levels.KINDS:
                tests = [
                    t
                    for t in tester.decided
                    if t.base == base and t.tier == tier and t.kind == kind
                ]
                counts = {
                    o: sum(1 for t in tests if t.outcome == o)
                    for o in ("BREAK", "BOUNCE", "NEUTRAL")
                }
                decided = counts["BREAK"] + counts["BOUNCE"]
                low, high = wilson(counts["BOUNCE"], decided)
                share = f"{counts['BOUNCE'] / decided:.1%}" if decided else "-"
                lines.append(
                    f"| {base}/USDT | {tier} | {kind} | {len(tests):,} | {counts['BREAK']:,} "
                    f"| {counts['BOUNCE']:,} | {counts['NEUTRAL']:,} | {share} "
                    f"| {low:.1%} to {high:.1%} |"
                )
    first, last = FINDINGS
    say(f"\nRound levels against random ones, Binance, {first} to {last}:\n")
    for line in lines:
        say(line)
    say(f"\nDiscarded for a gap in the data: {tester.discarded}")
    write(OUT / "findings.md", "\n".join(lines) + "\n")
    report.record(
        "3. the findings from four weeks: outcomes and bounce shares, with 95% intervals",
        bool(tester.decided),
        f"({len(tester.decided):,} tests over {(last - first).days + 1} days)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
