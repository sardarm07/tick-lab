package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import ticklab.v1.Alert;
import ticklab.v1.Spread;

/**
 * The divergence state machine on made-up spreads, one a second at the end of each second (as
 * the signals job emits them). Rules for these tests: opens above 10 bps held for 30 s, closes
 * below 5 bps held for 30 s, a pause over 5 s breaks a run, 60 s without spreads closes.
 */
class DivergenceAlertsTest {
    private static final AlertRules.Divergence RULES = new AlertRules.Divergence(
            new BigDecimal("10"), new BigDecimal("5"), Duration.ofSeconds(30),
            Duration.ofSeconds(5), Duration.ofSeconds(60));

    /** The end of second {@code s}: the time the job gives that second's spread. */
    static long end(long s) {
        return s * 1000 + 999;
    }

    static Spread spread(long second, String bps) {
        BigDecimal price = new BigDecimal("83000").setScale(12);
        return Spread.newBuilder()
                .setBase("BTC")
                .setSecond(Instant.ofEpochMilli(second * 1000))
                .setBinancePrice(price)
                .setKrakenPrice(price)
                .setUsdtUsd(BigDecimal.ONE.setScale(12))
                .setKrakenPriceUsdt(price)
                .setSpreadBps(new BigDecimal(bps).setScale(12))
                .setKrakenAgeMs(0)
                .setRateAgeMs(0)
                .build();
    }

    /** Spreads of {@code bps} for seconds first..last. */
    static List<Spread> run(long first, long last, String bps) {
        List<Spread> spreads = new ArrayList<>();
        for (long s = first; s <= last; s++) {
            spreads.add(spread(s, bps));
        }
        return spreads;
    }

    /**
     * Feeds the spreads in order. With {@code watermarkEach}, a watermark follows every spread,
     * as live; otherwise one watermark comes at the end, as when a replay races ahead.
     */
    static List<String> alerts(List<Spread> spreads, long until, boolean watermarkEach)
            throws Exception {
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DivergenceAlerts(RULES)),
                Spread::getBase,
                Types.STRING)) {
            h.open();
            for (Spread s : spreads) {
                long time = s.getSecond().toEpochMilli() + 999;
                h.processElement(s, time);
                if (watermarkEach) {
                    h.processWatermark(new Watermark(time));
                }
            }
            h.processWatermark(new Watermark(until));
            return h.extractOutputValues().stream().map(DivergenceAlertsTest::describe).toList();
        }
    }

    static String describe(Alert a) {
        return a.getStatus() + " opened " + a.getOpenedAt().toEpochMilli() + " at "
                + a.getAt().toEpochMilli() + " value " + a.getValue() + " peak " + a.getPeak();
    }

    @Test
    void opensAfterHoldingAboveTheThresholdAndClosesAfterHoldingBelowTheOther() throws Exception {
        List<Spread> spreads = new ArrayList<>(run(0, 59, "12"));
        spreads.set(45, spread(45, "-16")); // Kraken cheaper for a second: still a divergence
        spreads.addAll(run(60, 99, "3"));
        assertThat(alerts(spreads, end(200), true)).containsExactly(
                "OPEN opened 30999 at 30999 value 12.0 peak 12.0",
                "CLOSED opened 30999 at 90999 value 12.0 peak -16.0");
    }

    @Test
    void aSpreadBackAtTheThresholdRestartsTheRun() throws Exception {
        List<Spread> spreads = new ArrayList<>(run(0, 19, "12"));
        spreads.add(spread(20, "10")); // not above 10
        spreads.addAll(run(21, 60, "12"));
        assertThat(alerts(spreads, end(60), true))
                .containsExactly("OPEN opened 51999 at 51999 value 12.0 peak 12.0");
    }

    @Test
    void betweenTheThresholdsNothingChanges() throws Exception {
        List<Spread> spreads = new ArrayList<>(run(0, 40, "12"));
        spreads.addAll(run(41, 120, "7")); // below 10 but not below 5: stays open
        assertThat(alerts(spreads, end(120), true))
                .containsExactly("OPEN opened 30999 at 30999 value 12.0 peak 12.0");
    }

    @Test
    void aPauseInTheSpreadsBreaksTheRun() throws Exception {
        List<Spread> spreads = new ArrayList<>(run(0, 10, "12"));
        spreads.addAll(run(17, 50, "12")); // 7 s between two spreads, more than the 5 s allowed
        assertThat(alerts(spreads, end(50), true))
                .containsExactly("OPEN opened 47999 at 47999 value 12.0 peak 12.0");
    }

    @Test
    void anOpenAlertClosesWhenTheSpreadsStop() throws Exception {
        List<Spread> spreads = run(0, 40, "12");
        assertThat(alerts(spreads, end(200), true)).containsExactly(
                "OPEN opened 30999 at 30999 value 12.0 peak 12.0",
                "CLOSED opened 30999 at 100999 value 12.0 peak 12.0");
    }

    @Test
    void theAlertsDoNotDependOnHowFarTheWatermarkJumps() throws Exception {
        List<Spread> spreads = new ArrayList<>(run(0, 35, "12"));
        spreads.add(spread(36, "1"));
        spreads.addAll(run(37, 80, "-14"));
        spreads.addAll(run(81, 115, "2"));
        spreads.addAll(run(130, 170, "11"));
        List<String> live = alerts(spreads, end(400), true);
        List<String> replay = alerts(spreads, end(400), false);
        assertThat(live).containsExactly(
                "OPEN opened 30999 at 30999 value 12.0 peak 12.0",
                "CLOSED opened 30999 at 111999 value 12.0 peak -14.0",
                "OPEN opened 160999 at 160999 value 11.0 peak 11.0",
                "CLOSED opened 160999 at 230999 value 11.0 peak 11.0");
        assertThat(replay).isEqualTo(live);
    }
}
