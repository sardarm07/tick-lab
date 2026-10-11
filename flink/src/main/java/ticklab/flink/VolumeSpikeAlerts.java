package ticklab.flink;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
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
 * Volume-spike alerts, one state machine per feed, on 1-minute bars.
 *
 * <p>Each minute's volume is compared with the average minute of the previous hour, minutes
 * without trades counting as zero. The alert closes at the first minute back below the closing
 * ratio, or when the next minute has no trades. Each exchange has its own thresholds: Kraken's
 * thinner markets spike far more often than Binance's.
 */
public class VolumeSpikeAlerts extends InEventTimeOrder<String, MinuteBar, Alert> {
    private static final long MINUTE = 60_000;

    private final Map<String, AlertRules.VolumeSpike> rulesByExchange;

    private transient ValueState<double[]> volumes;
    private transient ValueState<Long> lastMinute;
    private transient ValueState<Long> openedAt;
    private transient ValueState<Double> openRatio;
    private transient ValueState<Double> peakRatio;
    private transient ValueState<String> openText;

    /** Thresholds by exchange: each feed (exchange:symbol) uses its exchange's. */
    public VolumeSpikeAlerts(Map<String, AlertRules.VolumeSpike> rulesByExchange) {
        super(TypeInformation.of(MinuteBar.class));
        this.rulesByExchange = rulesByExchange;
    }

    private AlertRules.VolumeSpike rules(String feed) {
        return rulesByExchange.get(feed.substring(0, feed.indexOf(':')));
    }

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        volumes = state("volumes", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO);
        lastMinute = state("last-minute", Types.LONG);
        openedAt = state("opened-at", Types.LONG);
        openRatio = state("open-ratio", Types.DOUBLE);
        peakRatio = state("peak-ratio", Types.DOUBLE);
        openText = state("open-text", Types.STRING);
    }

    private <T> ValueState<T> state(String name, TypeInformation<T> type) {
        return getRuntimeContext().getState(new ValueStateDescriptor<>(name, type));
    }

    @Override
    void onEvent(MinuteBar bar, long time, OnTimerContext context, Collector<Alert> out)
            throws Exception {
        AlertRules.VolumeSpike rules = rules(bar.feed);
        double[] history = volumes.value() == null ? new double[0] : volumes.value();
        Long previous = lastMinute.value();
        if (previous != null) {
            long empty = Math.min((bar.minuteMillis - previous) / MINUTE - 1, rules.window());
            for (long i = 0; i < empty; i++) {
                history = Alerts.append(history, 0, rules.window());
            }
        }
        double volume = bar.volume.doubleValue();
        if (history.length >= rules.window()) {
            double average = mean(history);
            if (average > 0) {
                judge(bar, time, volume, volume / average, average, rules, out);
            }
        }
        volumes.update(Alerts.append(history, volume, rules.window()));
        lastMinute.update(bar.minuteMillis);
        // At the end of the next minute: if it had no trades, an open alert closes.
        context.timerService().registerEventTimeTimer(time + MINUTE);
    }

    private void judge(MinuteBar bar, long time, double volume, double ratio, double average,
            AlertRules.VolumeSpike rules, Collector<Alert> out) throws Exception {
        String asset = bar.feed.substring(bar.feed.indexOf(':') + 1, bar.feed.indexOf('/'));
        if (openedAt.value() == null) {
            if (ratio >= rules.openRatio()) {
                String text = String.format(Locale.ROOT,
                        "%s: %s %s traded in the minute to %s, %.1f times the past hour's "
                                + "average minute (%s %s; opens at %.0f times).",
                        bar.feed, number(volume), asset,
                        Alerts.MINUTE.format(Instant.ofEpochMilli(time + 1)), ratio,
                        number(average), asset, rules.openRatio());
                openedAt.update(time);
                openRatio.update(ratio);
                peakRatio.update(ratio);
                openText.update(text);
                out.collect(Alerts.of(AlertType.VOLUME_SPIKE, bar.feed, time, time,
                        AlertStatus.OPEN, ratio, ratio, text));
            }
            return;
        }
        peakRatio.update(Math.max(peakRatio.value(), ratio));
        if (ratio < rules.closeRatio()) {
            close(bar.feed, time, String.format(Locale.ROOT,
                    "Back below %.0f times in the minute to %s.", rules.closeRatio(),
                    Alerts.MINUTE.format(Instant.ofEpochMilli(time + 1))), out);
        }
    }

    @Override
    void onTime(long time, OnTimerContext context, Collector<Alert> out) throws Exception {
        Long last = lastMinute.value();
        if (openedAt.value() != null && last != null && time == last + 2 * MINUTE - 1) {
            close(context.getCurrentKey(), time, "Then a minute without trades.", out);
        }
    }

    private void close(String feed, long time, String reason, Collector<Alert> out)
            throws Exception {
        String text = openText.value() + " " + reason
                + String.format(Locale.ROOT, " Largest: %.1f times.", peakRatio.value());
        out.collect(Alerts.of(AlertType.VOLUME_SPIKE, feed, openedAt.value(), time,
                AlertStatus.CLOSED, openRatio.value(), peakRatio.value(), text));
        openedAt.clear();
        openRatio.clear();
        peakRatio.clear();
        openText.clear();
    }

    static double mean(double[] values) {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum / values.length;
    }

    /** Volumes in plain words: 3 significant digits, at most 4 decimal places. */
    static String number(double value) {
        if (value >= 100) {
            return String.format(Locale.ROOT, "%,.0f", value);
        }
        if (value >= 1) {
            return String.format(Locale.ROOT, "%.2f", value);
        }
        return String.format(Locale.ROOT, "%.4f", value);
    }
}
