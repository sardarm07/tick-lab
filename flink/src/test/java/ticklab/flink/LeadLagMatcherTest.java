package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import ticklab.v1.LeadLag;

/** Pairing moves across the exchanges: the same cases as tests/test_lead_lag.py. */
class LeadLagMatcherTest {
    static final LeadLagRules RULES = new LeadLagRules(
            Map.of("BTC", new BigDecimal("10")), Duration.ofSeconds(10), Duration.ofSeconds(2));
    static final long S = 1_791_700_000L;

    /** A move {@code atMicros} after second S. */
    static Move move(String exchange, long atMicros, boolean up) {
        long micros = S * 1_000_000 + atMicros;
        return new Move(exchange, "BTC", Math.floorDiv(micros, 1_000_000) * 1000, micros, up,
                new BigDecimal(up ? "20" : "-20").setScale(Decimals.SCALE));
    }

    /** The moves in the order given, each with its second's end as its time, then the end. */
    static List<LeadLag> run(Move... moves) throws Exception {
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new LeadLagMatcher(RULES)),
                (Move move) -> move.base,
                Types.STRING)) {
            h.open();
            for (Move move : moves) {
                h.processElement(move, LeadLagMatcher.secondEnd(move.timeMicros));
            }
            h.processWatermark(new Watermark(Long.MAX_VALUE));
            return h.extractOutputValues();
        }
    }

    static List<String> pairs(List<LeadLag> rows) {
        return rows.stream()
                .filter(row -> row.getLeadUs() != null)
                .map(row -> row.getLeader() + " " + row.getLeadUs())
                .toList();
    }

    static long unpaired(List<LeadLag> rows) {
        return rows.stream().filter(row -> row.getLeadUs() == null).count();
    }

    @Test
    void movesPairWithTheEarliestUnpairedMoveOfTheOtherExchange() throws Exception {
        List<LeadLag> rows = run(
                move("BINANCE", 200_000, true),
                move("BINANCE", 700_000, true),
                move("KRAKEN", 1_500_000, true));
        assertThat(pairs(rows)).containsExactly("BINANCE 1300000");
        LeadLag alone = rows.stream().filter(row -> row.getLeadUs() == null).findFirst().get();
        assertThat(alone.getLeaderTime().toEpochMilli()).isEqualTo(S * 1000 + 700);
        assertThat(alone.getFollowerTime()).isNull();
        assertThat(alone.getThresholdBps()).isEqualByComparingTo("10");
    }

    @Test
    void onlyMovesInTheSameDirectionWithinTwoSecondsPair() throws Exception {
        List<LeadLag> rows = run(
                move("KRAKEN", 0, true),
                move("BINANCE", 500_000, false),
                move("BINANCE", 2_000_000, true),
                move("KRAKEN", 5_000_000, true),
                move("BINANCE", 7_000_001, true));
        assertThat(pairs(rows)).containsExactly("KRAKEN 2000000");
        assertThat(unpaired(rows)).isEqualTo(3);
    }

    @Test
    void movesOnTheSameExchangeNeverPair() throws Exception {
        List<LeadLag> rows = run(move("BINANCE", 100_000, true), move("BINANCE", 200_000, true));
        assertThat(unpaired(rows)).isEqualTo(2);
    }

    @Test
    void movesOfOneSecondAreTakenInTimeOrderWhateverTheirArrival() throws Exception {
        // Kraken's move arrives first but happened second; at the same moment, a tie.
        assertThat(pairs(run(move("KRAKEN", 300_001, true), move("BINANCE", 300_000, true))))
                .containsExactly("BINANCE 1");
        assertThat(pairs(run(move("KRAKEN", 300_000, true), move("BINANCE", 300_000, true))))
                .containsExactly("BINANCE 0");
    }
}
