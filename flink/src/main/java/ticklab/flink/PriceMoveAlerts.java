package ticklab.flink;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import ticklab.v1.Alert;
import ticklab.v1.AlertStatus;
import ticklab.v1.AlertType;

/**
 * Price-move alerts, one state machine per feed, on 1-minute bars.
 *
 * <p>Each minute's log return is compared with the feed's recent volatility: the root mean
 * square of its previous one-minute returns. The ratio is a z-score; a large one is a move far
 * outside what the past hour made normal. The alert closes at the first minute whose z-score is
 * back below the closing threshold, or when the next minute has no trades.
 *
 * <p>Returns are only taken between consecutive minutes; a minute without trades breaks the
 * chain. Floating point is fine here: these are statistics, not prices.
 */
public class PriceMoveAlerts extends InEventTimeOrder<String, MinuteBar, Alert> {
    private static final long MINUTE = 60_000;

    private final AlertRules.PriceMove rules;

    private transient ValueState<double[]> returns;
    private transient ValueState<Long> lastMinute;
    private transient ValueState<BigDecimal> lastClose;
    private transient ValueState<Long> openedAt;
    private transient ValueState<Double> openZ;
    private transient ValueState<Double> peakZ;
    private transient ValueState<String> openText;

    public PriceMoveAlerts(AlertRules.PriceMove rules) {
        super(TypeInformation.of(MinuteBar.class));
        this.rules = rules;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        returns = state("returns", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO);
        lastMinute = state("last-minute", Types.LONG);
        lastClose = state("last-close", Types.BIG_DEC);
        openedAt = state("opened-at", Types.LONG);
        openZ = state("open-z", Types.DOUBLE);
        peakZ = state("peak-z", Types.DOUBLE);
        openText = state("open-text", Types.STRING);
    }

    private <T> ValueState<T> state(String name, TypeInformation<T> type) {
        return getRuntimeContext().getState(new ValueStateDescriptor<>(name, type));
    }

    @Override
    void onEvent(MinuteBar bar, long time, OnTimerContext context, Collector<Alert> out)
            throws Exception {
        Long previous = lastMinute.value();
        if (previous != null && bar.minuteMillis == previous + MINUTE) {
            double move = Math.log(bar.close.doubleValue() / lastClose.value().doubleValue());
            double[] history = returns.value() == null ? new double[0] : returns.value();
            if (history.length >= rules.minReturns()) {
                double typical = rootMeanSquare(history);
                if (typical > 0) {
                    judge(bar, time, move, move / typical, typical, out);
                }
            }
            returns.update(Alerts.append(history, move, rules.window()));
        } else if (openedAt.value() != null) {
            close(bar.feed, time, "Then a minute without trades.", out);
        }
        lastMinute.update(bar.minuteMillis);
        lastClose.update(bar.close);
        // At the end of the next minute: if it had no trades, an open alert closes.
        context.timerService().registerEventTimeTimer(time + MINUTE);
    }

    private void judge(MinuteBar bar, long time, double move, double z, double typical,
            Collector<Alert> out) throws Exception {
        if (openedAt.value() == null) {
            if (Math.abs(z) >= rules.openZ()) {
                String text = String.format(Locale.ROOT,
                        "%s: the minute to %s moved %+.3f%%, %.1f times the typical one-minute "
                                + "move of the past hour (%.3f%%; opens at %.0f times).",
                        bar.feed, Alerts.MINUTE.format(Instant.ofEpochMilli(time + 1)),
                        100 * Math.expm1(move), Math.abs(z), 100 * Math.expm1(typical),
                        rules.openZ());
                openedAt.update(time);
                openZ.update(z);
                peakZ.update(z);
                openText.update(text);
                out.collect(Alerts.of(AlertType.PRICE_MOVE, bar.feed, time, time,
                        AlertStatus.OPEN, z, z, text));
            }
            return;
        }
        if (Math.abs(z) > Math.abs(peakZ.value())) {
            peakZ.update(z);
        }
        if (Math.abs(z) < rules.closeZ()) {
            close(bar.feed, time, String.format(Locale.ROOT,
                    "Back below %.0f times in the minute to %s.", rules.closeZ(),
                    Alerts.MINUTE.format(Instant.ofEpochMilli(time + 1))), out);
        }
    }

    @Override
    void onTime(long time, OnTimerContext context, Collector<Alert> out) throws Exception {
        Long last = lastMinute.value();
        // The timer set at the end of minute m fires at the end of minute m + 1; bars of that
        // minute were handled first, so an unchanged last minute means it had no trades.
        if (openedAt.value() != null && last != null && time == last + 2 * MINUTE - 1) {
            close(context.getCurrentKey(), time, "Then a minute without trades.", out);
        }
    }

    private void close(String feed, long time, String reason, Collector<Alert> out)
            throws Exception {
        double first = openZ.value();
        double peak = peakZ.value();
        String text = openText.value() + " " + reason
                + String.format(Locale.ROOT, " Largest: %.1f times.", Math.abs(peak));
        out.collect(Alerts.of(AlertType.PRICE_MOVE, feed, openedAt.value(), time,
                AlertStatus.CLOSED, first, peak, text));
        openedAt.clear();
        openZ.clear();
        peakZ.clear();
        openText.clear();
    }

    static double rootMeanSquare(double[] values) {
        double sum = 0;
        for (double value : values) {
            sum += value * value;
        }
        return Math.sqrt(sum / values.length);
    }
}
