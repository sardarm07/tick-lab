from decimal import Decimal

from ticklab import round_levels
from ticklab.model import Side, Source, Trade
from ticklab.round_levels import STEPS_CENTS, LevelTester

S = 1_791_700_000  # a whole second, in seconds since the epoch


def trade(trade_id: int, at: float, price: str, base: str = "BTC") -> Trade:
    """A Binance trade `at` seconds after S."""
    return Trade(
        "BINANCE",
        f"{base}/USDT",
        base,
        "USDT",
        trade_id,
        Decimal(price),
        Decimal(1),
        Side.BUY,
        S * 1_000_000 + round(at * 1_000_000),
        None,
        Source.HISTORY,
    )


def run(*trades: Trade, tiers: tuple[str, ...] = ("MAJOR",)) -> list[round_levels.LevelTest]:
    """The tests of round levels of `tiers`, with a band of 20 USDT."""
    tester = LevelTester({"BTC": 2_000})
    for t in trades:
        tester.add(t)
    return [t for t in tester.finish() if t.kind == "ROUND" and t.tier in tiers]


def test_random_levels_are_fixed_and_away_from_round_levels():
    for tier, step in STEPS_CENTS["BTC"].items():
        for k in range(820, 830):
            level = round_levels.random_level("BTC", tier, k)
            assert level == round_levels.random_level("BTC", tier, k)
            assert k * step < level < (k + 1) * step
            minor = STEPS_CENTS["BTC"]["MINOR"]
            assert minor // 10 <= level % minor <= minor - minor // 10


def test_a_test_starts_when_the_price_reaches_the_level_from_beyond_its_band():
    tests = run(
        trade(1, 0.0, "82950"),  # 50 below 83,000, beyond the 20 band
        trade(2, 0.5, "82990"),  # inside the band: armed, but not there yet
        trade(3, 1.2, "83000.01"),  # reaches the level: a test from below
        trade(4, 1.5, "82995"),  # back inside the band: disarmed, no second test
        trade(5, 2.0, "83000"),
        trade(6, 61.5, "83030"),  # the close of second 61, 60 s after the test began
    )
    assert len(tests) == 1
    test = tests[0]
    assert (test.tier, test.level, test.up) == ("MAJOR", 8_300_000, True)
    assert test.time_us == (S + 1) * 1_000_000 + 200_000
    assert test.decide_second == S + 61
    assert (test.close, test.outcome) == (8_303_000, "BREAK")


def test_the_close_at_the_decision_says_break_bounce_or_neutral():
    def decided(after: str) -> str:
        (test,) = run(trade(1, 0.0, "82950"), trade(2, 1.0, "83000"), trade(3, 61.0, after))
        return test.outcome

    assert decided("83020") == "BREAK"  # 20 past the level: the band
    assert decided("82980") == "BOUNCE"
    assert decided("83019.99") == "NEUTRAL"


def test_a_quiet_decision_second_takes_the_last_close_before_it():
    tests = run(
        trade(1, 0.0, "82950"),
        trade(2, 1.0, "83000"),
        trade(3, 30.0, "82970"),  # the last trade before the decision at the end of second 61
        trade(4, 70.0, "83100"),  # reaches 83,000 again: a second test
    )
    first = min(tests, key=lambda t: t.time_us)
    assert (first.close, first.outcome) == (8_297_000, "BOUNCE")
    assert len(tests) == 2


def test_the_level_rearms_once_the_price_leaves_its_band():
    tests = run(
        trade(1, 0.0, "82950"),
        trade(2, 1.0, "83000"),  # a test from below, decided at the end of second 61
        trade(3, 2.0, "83050"),  # above the band: armed from above
        trade(4, 70.0, "83000"),  # a test from above
    )
    started = [(t.up, t.time_us - S * 1_000_000, t.trade_id) for t in tests]
    assert started == [(True, 1_000_000, 2), (False, 70_000_000, 4)]


def test_a_level_is_tested_once_at_a_time():
    tests = run(
        trade(1, 0.0, "82950"),
        trade(2, 1.0, "83000"),  # a test from below, open until second 61
        trade(3, 2.0, "83050"),  # armed from above
        trade(4, 3.0, "83000"),  # reached again while the test is open: no second test
    )
    assert [(t.up, t.trade_id) for t in tests] == [(True, 2)]


def test_a_gap_in_the_trade_ids_discards_the_tests_it_overlaps():
    tester = LevelTester({"BTC": 2_000})
    for t in (trade(1, 0.0, "82950"), trade(2, 1.0, "83000"), trade(4, 5.0, "83001")):
        tester.add(t)
    assert tester.finish() == []  # every test open at the gap, round or random, is dropped
    assert tester.discarded >= 1


def test_a_major_level_is_also_a_minor_one():
    tests = run(trade(1, 0.0, "82950"), trade(2, 1.0, "83150"), tiers=("MAJOR", "MINOR"))
    levels = sorted((t.tier, t.level) for t in tests)
    assert levels == [("MAJOR", 8_300_000), ("MINOR", 8_300_000), ("MINOR", 8_310_000)]
