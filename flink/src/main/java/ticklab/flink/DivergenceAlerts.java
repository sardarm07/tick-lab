package ticklab.flink;

import java.math.BigDecimal;
import java.util.Locale;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import ticklab.v1.Alert;
import ticklab.v1.AlertStatus;
import ticklab.v1.AlertType;
import ticklab.v1.Spread;

/**
 * Divergence alerts, one state machine per base asset, on the per-second spread.
 *
 * <ul>
 *   <li>Opens when |spread| has stayed above {@code openBps} for {@code hold}: an event-time
 *       timer set when the spread first crosses checks that it never came back in between.
 *   <li>Closes when |spread| has stayed below {@code closeBps} for {@code hold}. The gap between
 *       the two thresholds is hysteresis: a spread hovering near one threshold doesn't open and
 *       close the alert over and over.
 *   <li>Also closes when no spread has been seen for {@code silence}: without fresh prices from
 *       both exchanges, there is nothing left to compare.
 * </ul>
 *
 * <p>Spreads arrive once a second while both prices are fresh. A pause of more than {@code gap}
 * between them breaks a run, so a stale stretch never counts towards {@code hold}.
 */
public class DivergenceAlerts extends InEventTimeOrder<String, Spread, Alert> {
    private final BigDecimal openBps;
    private final BigDecimal closeBps;
    private final long holdMillis;
    private final long gapMillis;
    private final long silenceMillis;

    private transient ValueState<Long> aboveSince;
    private transient ValueState<Long> belowSince;
    private transient ValueState<Long> lastSeen;
    private transient ValueState<Double> lastBps;
    private transient ValueState<Long> openedAt;
    private transient ValueState<Double> openBpsSeen;
    private transient ValueState<Double> peakBps;

    public DivergenceAlerts(AlertRules.Divergence rules) {
        super(TypeInformation.of(Spread.class));
        this.openBps = rules.openBps();
        this.closeBps = rules.closeBps();
        this.holdMillis = rules.hold().toMillis();
        this.gapMillis = rules.gap().toMillis();
        this.silenceMillis = rules.silence().toMillis();
    }

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        aboveSince = state("above-since", Types.LONG);
        belowSince = state("below-since", Types.LONG);
        lastSeen = state("last-seen", Types.LONG);
        lastBps = state("last-bps", Types.DOUBLE);
        openedAt = state("opened-at", Types.LONG);
        openBpsSeen = state("open-bps", Types.DOUBLE);
        peakBps = state("peak-bps", Types.DOUBLE);
    }

    private <T> ValueState<T> state(String name, TypeInformation<T> type) {
        return getRuntimeContext().getState(new ValueStateDescriptor<>(name, type));
    }

    @Override
    void onEvent(Spread spread, long time, OnTimerContext context, Collector<Alert> out)
            throws Exception {
        BigDecimal size = spread.getSpreadBps().abs();
        Long previous = lastSeen.value();
        if (previous != null && time - previous > gapMillis) {
            // A break in the data: neither run can continue across it.
            aboveSince.clear();
            belowSince.clear();
        }
        lastSeen.update(time);
        lastBps.update(spread.getSpreadBps().doubleValue());
        if (openedAt.value() == null) {
            if (size.compareTo(openBps) > 0) {
                if (aboveSince.value() == null) {
                    aboveSince.update(time);
                    context.timerService().registerEventTimeTimer(time + holdMillis);
                }
            } else {
                aboveSince.clear();
            }
            return;
        }
        double bps = spread.getSpreadBps().doubleValue();
        if (Math.abs(bps) > Math.abs(peakBps.value())) {
            peakBps.update(bps);
        }
        if (size.compareTo(closeBps) < 0) {
            if (belowSince.value() == null) {
                belowSince.update(time);
                context.timerService().registerEventTimeTimer(time + holdMillis);
            }
        } else {
            belowSince.clear();
        }
        context.timerService().registerEventTimeTimer(time + silenceMillis);
    }

    @Override
    void onTime(long time, OnTimerContext context, Collector<Alert> out) throws Exception {
        String base = context.getCurrentKey();
        Long seen = lastSeen.value();
        boolean fresh = seen != null && time - seen <= gapMillis;
        Long opened = openedAt.value();
        Long above = aboveSince.value();
        if (opened == null && above != null && time == above + holdMillis && fresh) {
            double bps = lastBps.value();
            openedAt.update(time);
            openBpsSeen.update(bps);
            peakBps.update(bps);
            belowSince.clear();
            context.timerService().registerEventTimeTimer(seen + silenceMillis);
            out.collect(Alerts.of(AlertType.DIVERGENCE, base, time, time, AlertStatus.OPEN,
                    bps, bps, opening(base, bps)));
            return;
        }
        if (opened == null) {
            return;
        }
        Long below = belowSince.value();
        String reason = null;
        if (below != null && time == below + holdMillis && fresh) {
            reason = String.format(Locale.ROOT, "Back within %s bps for %d s.",
                    closeBps.toPlainString(), holdMillis / 1000);
        } else if (seen != null && time >= seen + silenceMillis) {
            reason = String.format(Locale.ROOT,
                    "No fresh prices from both exchanges for %d s.", silenceMillis / 1000);
        }
        if (reason != null) {
            double first = openBpsSeen.value();
            double peak = peakBps.value();
            String text = opening(base, first) + " " + reason + String.format(Locale.ROOT,
                    " The widest gap was %.2f bps (Kraken %s).", Math.abs(peak), side(peak));
            out.collect(Alerts.of(AlertType.DIVERGENCE, base, opened, time, AlertStatus.CLOSED,
                    first, peak, text));
            openedAt.clear();
            openBpsSeen.clear();
            peakBps.clear();
            aboveSince.clear();
            belowSince.clear();
        }
    }

    private String opening(String base, double bps) {
        return String.format(Locale.ROOT,
                "%s: Kraken, converted to USDT, was %.2f bps %s Binance for %d s "
                        + "(opens above %s bps held for %d s).",
                base, Math.abs(bps), side(bps) + " than", holdMillis / 1000,
                openBps.toPlainString(), holdMillis / 1000);
    }

    private static String side(double bps) {
        return bps >= 0 ? "dearer" : "cheaper";
    }
}
