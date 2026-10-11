package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import ticklab.v1.Alert;

/**
 * Price-move alerts on made-up minute bars: a calm hour (every minute up or down 0.1%), then a
 * 1% minute. Rules: opens at 6 times the typical move, closes below 3 times, needs 30 returns.
 */
class PriceMoveAlertsTest {
    private static final AlertRules.PriceMove RULES = new AlertRules.PriceMove(6.0, 3.0, 60, 30);
    private static final MathContext PRECISION = MathContext.DECIMAL128;
    static final String FEED = "BINANCE:BTC/USDT";

    /** The end of minute {@code m}: the time the job gives that minute's bar. */
    static long end(long m) {
        return m * 60_000 + 59_999;
    }

    static MinuteBar bar(long minute, BigDecimal close, String volume) {
        return new MinuteBar(FEED, minute * 60_000, close, new BigDecimal(volume), 10);
    }

    /** Minutes 0..calm, closing alternately 0.1% up and 0.1% down. */
    static List<MinuteBar> calm(int minutes) {
        List<MinuteBar> bars = new ArrayList<>();
        BigDecimal close = new BigDecimal("100");
        BigDecimal step = new BigDecimal("1.001");
        for (int m = 0; m <= minutes; m++) {
            bars.add(bar(m, close, "1"));
            close = m % 2 == 0 ? close.multiply(step, PRECISION) : close.divide(step, PRECISION);
        }
        return bars;
    }

    static BigDecimal last(List<MinuteBar> bars) {
        return bars.get(bars.size() - 1).close;
    }

    static List<Alert> alerts(List<MinuteBar> bars, long until) throws Exception {
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new PriceMoveAlerts(RULES)),
                (MinuteBar bar) -> bar.feed,
                Types.STRING)) {
            h.open();
            for (MinuteBar bar : bars) {
                h.processElement(bar, bar.minuteMillis + 59_999);
                h.processWatermark(new Watermark(bar.minuteMillis + 59_999));
            }
            h.processWatermark(new Watermark(until));
            return h.extractOutputValues();
        }
    }

    @Test
    void aMoveFarOutsideTheHoursTypicalOneOpensAndTheNextCalmMinuteCloses() throws Exception {
        List<MinuteBar> bars = calm(40);
        bars.add(bar(41, last(bars).multiply(new BigDecimal("1.01"), PRECISION), "1"));
        bars.add(bar(42, last(bars).multiply(new BigDecimal("1.0005"), PRECISION), "1"));
        List<Alert> alerts = alerts(bars, end(100));
        assertThat(alerts).hasSize(2);
        Alert open = alerts.get(0);
        assertThat(open.getStatus().toString()).isEqualTo("OPEN");
        assertThat(open.getAt().toEpochMilli()).isEqualTo(end(41));
        // ln(1.01) / ln(1.001): the move in units of the typical move.
        assertThat(open.getValue()).isCloseTo(Math.log(1.01) / Math.log(1.001), within(1e-9));
        assertThat(open.getExplanation()).startsWith(
                "BINANCE:BTC/USDT: the minute to 00:42 moved +1.000%, 10.0 times the typical");
        Alert closed = alerts.get(1);
        assertThat(closed.getStatus().toString()).isEqualTo("CLOSED");
        assertThat(closed.getOpenedAt().toEpochMilli()).isEqualTo(end(41));
        assertThat(closed.getAt().toEpochMilli()).isEqualTo(end(42));
    }

    @Test
    void aMinuteWithoutTradesClosesTheAlert() throws Exception {
        List<MinuteBar> bars = calm(40);
        bars.add(bar(41, last(bars).multiply(new BigDecimal("0.99"), PRECISION), "1"));
        List<Alert> alerts = alerts(bars, end(100));
        assertThat(alerts.stream().map(a -> a.getStatus() + "@" + a.getAt().toEpochMilli()))
                .containsExactly("OPEN@" + end(41), "CLOSED@" + end(42));
        assertThat(alerts.get(1).getExplanation()).contains("Then a minute without trades.");
    }

    @Test
    void thereIsNoAlertBeforeThirtyReturns() throws Exception {
        List<MinuteBar> bars = calm(20);
        bars.add(bar(21, last(bars).multiply(new BigDecimal("1.05"), PRECISION), "1"));
        assertThat(alerts(bars, end(100))).isEmpty();
    }
}
