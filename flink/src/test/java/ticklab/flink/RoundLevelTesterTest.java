package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import ticklab.v1.RoundLevelTest;

/** Round-level tests on made-up BTC trades: the same cases as tests/test_round_levels.py. */
class RoundLevelTesterTest {
    static final long S = 1_791_700_000L;
    static final RoundLevelRules RULES = new RoundLevelRules(
            RoundLevelRules.ROUND_LEVELS.steps(),
            Map.of("BTC", 2_000L),
            RoundLevelRules.ROUND_LEVELS.seed(),
            Duration.ofSeconds(60),
            2);

    record Trade(long id, double at, String price) {}

    static Trade t(long id, double at, String price) {
        return new Trade(id, at, price);
    }

    /** The trades as seconds, each with its second's end as its time, then the end of input. */
    static List<RoundLevelTest> run(Trade... trades) throws Exception {
        TreeMap<Long, List<Trade>> seconds = new TreeMap<>();
        for (Trade trade : trades) {
            long micros = S * 1_000_000 + Math.round(trade.at() * 1_000_000);
            seconds.computeIfAbsent(Math.floorDiv(micros, 1_000_000L), k -> new ArrayList<>())
                    .add(trade);
        }
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new RoundLevelTester(RULES)),
                (SecondOfTrades second) -> second.base,
                Types.STRING)) {
            h.open();
            for (var entry : seconds.entrySet()) {
                List<Trade> in = entry.getValue();
                SecondOfTrades second = new SecondOfTrades();
                second.base = "BTC";
                second.secondMillis = entry.getKey() * 1000;
                second.ids = in.stream().mapToLong(Trade::id).toArray();
                second.timesMicros = in.stream()
                        .mapToLong(x -> S * 1_000_000 + Math.round(x.at() * 1_000_000))
                        .toArray();
                second.priceCents = in.stream()
                        .mapToLong(x -> new BigDecimal(x.price()).movePointRight(2).longValue())
                        .toArray();
                h.processElement(second, second.secondMillis + 999);
            }
            h.processWatermark(new Watermark(Long.MAX_VALUE));
            return h.extractOutputValues();
        }
    }

    /** The tests of round major levels. */
    static List<RoundLevelTest> round(List<RoundLevelTest> tests) {
        return tests.stream()
                .filter(x -> x.getKind().toString().equals("ROUND"))
                .filter(x -> x.getTier().toString().equals("MAJOR"))
                .toList();
    }

    @Test
    void randomLevelsAreTheSameAsInPython() {
        RoundLevelTester tester = new RoundLevelTester(RoundLevelRules.ROUND_LEVELS);
        // From ticklab.round_levels.random_level.
        assertThat(tester.randomLevel("BTC", "MAJOR", 82)).isEqualTo(8_274_468L);
        assertThat(tester.randomLevel("BTC", "MINOR", 829)).isEqualTo(8_293_770L);
        assertThat(tester.randomLevel("SOL", "MAJOR", 11)).isEqualTo(11_588L);
        assertThat(tester.randomLevel("SOL", "MINOR", 110)).isEqualTo(11_080L);
    }

    @Test
    void aTestStartsWhenThePriceReachesTheLevelFromBeyondItsBand() throws Exception {
        List<RoundLevelTest> tests = round(run(
                t(1, 0.0, "82950"),
                t(2, 0.5, "82990"),
                t(3, 1.2, "83000.01"),
                t(4, 1.5, "82995"),
                t(5, 2.0, "83000"),
                t(6, 61.5, "83030")));
        assertThat(tests).hasSize(1);
        RoundLevelTest test = tests.get(0);
        assertThat(test.getTier().toString()).isEqualTo("MAJOR");
        assertThat(test.getLevel()).isEqualByComparingTo("83000");
        assertThat(test.getApproach().toString()).isEqualTo("FROM_BELOW");
        assertThat(TradeEvent.micros(test.getTouchTime())).isEqualTo((S + 1) * 1_000_000 + 200_000);
        assertThat(test.getDecideSecond().getEpochSecond()).isEqualTo(S + 61);
        assertThat(test.getClose()).isEqualByComparingTo("83030");
        assertThat(test.getOutcome().toString()).isEqualTo("BREAK");
    }

    @Test
    void theCloseAtTheDecisionSaysBreakBounceOrNeutral() throws Exception {
        for (var expected : Map.of("83020", "BREAK", "82980", "BOUNCE", "83019.99", "NEUTRAL")
                .entrySet()) {
            List<RoundLevelTest> tests = round(run(
                    t(1, 0.0, "82950"), t(2, 1.0, "83000"), t(3, 61.0, expected.getKey())));
            assertThat(tests).hasSize(1);
            assertThat(tests.get(0).getOutcome().toString()).isEqualTo(expected.getValue());
        }
    }

    @Test
    void aQuietDecisionSecondTakesTheLastCloseBeforeIt() throws Exception {
        List<RoundLevelTest> tests = round(run(
                t(1, 0.0, "82950"),
                t(2, 1.0, "83000"),
                t(3, 30.0, "82970"),
                t(4, 70.0, "83100")));
        assertThat(tests).hasSize(2);
        RoundLevelTest first = tests.get(0);
        assertThat(TradeEvent.micros(first.getTouchTime())).isEqualTo((S + 1) * 1_000_000);
        assertThat(first.getClose()).isEqualByComparingTo("82970");
        assertThat(first.getOutcome().toString()).isEqualTo("BOUNCE");
    }

    @Test
    void theLevelRearmsOnceThePriceLeavesItsBand() throws Exception {
        List<RoundLevelTest> tests = round(run(
                t(1, 0.0, "82950"), t(2, 1.0, "83000"), t(3, 2.0, "83050"), t(4, 70.0, "83000")));
        assertThat(tests).extracting(x -> x.getApproach() + " " + x.getTouchTradeId())
                .containsExactly("FROM_BELOW 2", "FROM_ABOVE 4");
    }

    @Test
    void aLevelIsTestedOnceAtATime() throws Exception {
        List<RoundLevelTest> tests = round(run(
                t(1, 0.0, "82950"), t(2, 1.0, "83000"), t(3, 2.0, "83050"), t(4, 3.0, "83000")));
        assertThat(tests).extracting(x -> x.getApproach() + " " + x.getTouchTradeId())
                .containsExactly("FROM_BELOW 2");
    }

    @Test
    void aGapInTheTradeIdsDiscardsTheTestsItOverlaps() throws Exception {
        assertThat(run(t(1, 0.0, "82950"), t(2, 1.0, "83000"), t(4, 5.0, "83001"))).isEmpty();
    }

    @Test
    void aMajorLevelIsAlsoAMinorOne() throws Exception {
        List<RoundLevelTest> tests = run(t(1, 0.0, "82950"), t(2, 1.0, "83150")).stream()
                .filter(x -> x.getKind().toString().equals("ROUND"))
                .toList();
        assertThat(tests)
                .extracting(x -> x.getTier() + " " + SignalsPipelineTest.plain(x.getLevel()))
                .containsExactlyInAnyOrder("MAJOR 83000", "MINOR 83000", "MINOR 83100");
    }
}
