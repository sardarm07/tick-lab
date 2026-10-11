package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import ticklab.v1.Alert;

/**
 * Volume-spike alerts on made-up minute bars. Rules: opens at 8 times the past hour's average
 * minute, closes below 3 times.
 */
class VolumeSpikeAlertsTest {
    private static final Map<String, AlertRules.VolumeSpike> RULES =
            Map.of("KRAKEN", new AlertRules.VolumeSpike(8.0, 3.0, 60));

    static long end(long m) {
        return m * 60_000 + 59_999;
    }

    static MinuteBar bar(long minute, String volume) {
        return new MinuteBar("KRAKEN:ETH/USD", minute * 60_000, new BigDecimal("2400"),
                new BigDecimal(volume), 10);
    }

    static List<String> alerts(List<MinuteBar> bars, long until) throws Exception {
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VolumeSpikeAlerts(RULES)),
                (MinuteBar bar) -> bar.feed,
                Types.STRING)) {
            h.open();
            for (MinuteBar bar : bars) {
                h.processElement(bar, bar.minuteMillis + 59_999);
                h.processWatermark(new Watermark(bar.minuteMillis + 59_999));
            }
            h.processWatermark(new Watermark(until));
            return h.extractOutputValues().stream()
                    .map(a -> a.getStatus() + "@" + a.getAt().toEpochMilli() + " " + a.getValue())
                    .toList();
        }
    }

    @Test
    void aMinuteFarAboveTheHoursAverageOpensAndAnOrdinaryOneCloses() throws Exception {
        List<MinuteBar> bars = new ArrayList<>();
        for (int m = 0; m < 60; m++) {
            bars.add(bar(m, "1"));
        }
        bars.add(bar(60, "10"));
        bars.add(bar(61, "2"));
        assertThat(alerts(bars, end(100)))
                .containsExactly("OPEN@" + end(60) + " 10.0", "CLOSED@" + end(61) + " 10.0");
    }

    @Test
    void minutesWithoutTradesCountAsZero() throws Exception {
        List<MinuteBar> bars = new ArrayList<>();
        for (int m = 0; m < 60; m += 2) {
            bars.add(bar(m, "2")); // every other minute: an average of 1 a minute
        }
        bars.add(bar(60, "9"));
        List<String> alerts = alerts(bars, end(100));
        assertThat(alerts).containsExactly(
                "OPEN@" + end(60) + " 9.0", "CLOSED@" + end(61) + " 9.0");
    }

    @Test
    void thereIsNoAlertBeforeAnHourOfHistory() throws Exception {
        List<MinuteBar> bars = new ArrayList<>();
        for (int m = 0; m < 30; m++) {
            bars.add(bar(m, "1"));
        }
        bars.add(bar(30, "50"));
        assertThat(alerts(bars, end(100))).isEmpty();
    }
}
