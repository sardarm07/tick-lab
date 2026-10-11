package ticklab.flink;

import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Lets each trade through once, per feed, and reports gaps in the trade IDs.
 *
 * <p>Trade IDs increase within a feed, so most of what has been seen fits in one number,
 * {@code upTo}: every ID up to it has arrived. IDs above it that arrive early, ahead of a missing
 * one, wait in a small map until the gap below them fills. The state stays tiny, and the same
 * state shows where the gaps are.
 *
 * <p>Gaps (to the {@link #GAPS} side output):
 *
 * <ul>
 *   <li>DETECTED once the watermark passes the first trade after the gap. Before that, the
 *       missing trades may just be reordered, so no gap is reported for plain reordering.
 *   <li>CLOSED once every missing trade has arrived (for example after gap repair).
 *   <li>EXPIRED once the watermark is the lateness budget past the first trade after it: its IDs
 *       are recorded as skipped and {@code upTo} moves on. If one turns up later anyway, it is let
 *       through once, as a late trade, not mistaken for a repeat.
 * </ul>
 *
 * <p>At the start of a feed nothing is known about earlier IDs, so for one watermark bound every
 * ID is kept in the map, and trades reordered within the bound are still let through.
 */
public class Dedup extends KeyedProcessFunction<String, TradeEvent, TradeEvent> {
    public static final OutputTag<GapEvent> GAPS =
            new OutputTag<>("gaps", TypeInformation.of(GapEvent.class));

    private static final int MAX_SKIPPED_RANGES = 1000;

    private final long warmUpMillis;
    private final long latenessMillis;

    private transient ValueState<Long> upTo;
    private transient MapState<Long, Long> pending; // trade ID -> trade time in microseconds
    private transient MapState<Long, Long> skipped; // first skipped ID -> last skipped ID
    private transient ValueState<GapEvent> openGap; // the lowest gap, once a trade came after it
    private transient Counter duplicates;
    private transient Counter skippedIds;
    private transient Counter recovered;
    private transient Counter gapsDetected;

    public Dedup(Duration watermarkBound, Duration lateness) {
        this.warmUpMillis = watermarkBound.toMillis();
        this.latenessMillis = lateness.toMillis();
    }

    @Override
    public void open(OpenContext context) {
        upTo = getRuntimeContext().getState(new ValueStateDescriptor<>("up-to", Types.LONG));
        pending = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("pending", Types.LONG, Types.LONG));
        skipped = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("skipped", Types.LONG, Types.LONG));
        openGap = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("open-gap", TypeInformation.of(GapEvent.class)));
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        duplicates = metrics.counter("duplicates");
        skippedIds = metrics.counter("skippedIds");
        recovered = metrics.counter("recoveredAfterSkip");
        gapsDetected = metrics.counter("gapsDetected");
    }

    @Override
    public void processElement(TradeEvent trade, Context context, Collector<TradeEvent> out)
            throws Exception {
        long id = trade.tradeId;
        Long known = upTo.value();
        if (known != null && id <= known) {
            if (takeFromSkipped(id)) {
                recovered.inc();
                out.collect(trade);
            } else {
                duplicates.inc();
            }
            return;
        }
        if (pending.contains(id)) {
            duplicates.inc();
            return;
        }
        out.collect(trade);
        if (known == null) { // warming up
            if (pending.isEmpty()) {
                context.timerService().registerEventTimeTimer(trade.eventTimeMillis() + warmUpMillis);
            }
            pending.put(id, trade.tradeTimeMicros);
        } else if (id == known + 1) {
            long next = compact(id);
            upTo.update(next);
            afterAdvance(next, context);
        } else {
            boolean first = pending.isEmpty();
            pending.put(id, trade.tradeTimeMicros);
            if (first) { // the trade right after a new lowest gap
                context.timerService()
                        .registerEventTimeTimer(trade.eventTimeMillis() + latenessMillis);
                track(context.getCurrentKey(), known + 1, id - 1, trade.tradeTimeMicros, context);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<TradeEvent> out)
            throws Exception {
        long watermark = context.timerService().currentWatermark();
        String feed = context.getCurrentKey();
        Long upToValue = upTo.value();
        long known;
        if (upToValue == null) { // end of the warm-up: start counting from the lowest ID seen
            known = compact(lowestPending()[0] - 1);
            if (!pending.isEmpty()) {
                long[] low = lowestPending();
                context.timerService()
                        .registerEventTimeTimer(Math.floorDiv(low[1], 1000) + latenessMillis);
                track(feed, known + 1, low[0] - 1, low[1], context);
            }
        } else {
            known = upToValue;
        }

        GapEvent gap = openGap.value();
        if (gap != null && !gap.detected && watermark >= Math.floorDiv(gap.afterTradeTimeMicros, 1000)) {
            gap.detected = true;
            gap.firstMissingId = known + 1;
            openGap.update(gap);
            gapsDetected.inc();
            context.output(GAPS, gap.withStatus("DETECTED", now(context)));
        }

        while (!pending.isEmpty()) {
            long[] low = lowestPending(); // the first trade after the lowest open gap
            long due = Math.floorDiv(low[1], 1000) + latenessMillis;
            if (due > watermark) {
                context.timerService().registerEventTimeTimer(due);
                break;
            }
            rememberSkipped(known + 1, low[0] - 1);
            skippedIds.inc(low[0] - 1 - known);
            gap = openGap.value();
            if (gap != null) {
                if (gap.detected) {
                    context.output(GAPS, gap.withStatus("EXPIRED", now(context)));
                }
                openGap.clear();
            }
            known = compact(low[0] - 1);
            if (!pending.isEmpty()) {
                long[] next = lowestPending();
                track(feed, known + 1, next[0] - 1, next[1], context);
            }
        }
        upTo.update(known);
    }

    /** After {@code upTo} moved: closes the tracked gap once all of it has arrived. */
    private void afterAdvance(long next, Context context) throws Exception {
        GapEvent gap = openGap.value();
        if (gap == null || next < gap.lastMissingId) {
            return;
        }
        if (gap.detected) {
            context.output(GAPS, gap.withStatus("CLOSED", now(context)));
        }
        openGap.clear();
        if (!pending.isEmpty()) {
            long[] low = lowestPending();
            track(context.getCurrentKey(), next + 1, low[0] - 1, low[1], context);
        }
    }

    /** Starts tracking a gap; it is reported once the watermark passes the trade after it. */
    private void track(String feed, long first, long last, long afterMicros, Context context)
            throws Exception {
        openGap.update(new GapEvent(feed, first, last, afterMicros));
        context.timerService().registerEventTimeTimer(Math.floorDiv(afterMicros, 1000));
    }

    private static long now(Context context) {
        return context.timerService().currentProcessingTime();
    }

    /** Moves past every pending ID that now follows on without a gap. */
    private long compact(long from) throws Exception {
        long next = from;
        while (pending.contains(next + 1)) {
            pending.remove(next + 1);
            next++;
        }
        return next;
    }

    /** {lowest pending ID, its trade time in microseconds}. */
    private long[] lowestPending() throws Exception {
        long id = Long.MAX_VALUE;
        long time = 0;
        for (Map.Entry<Long, Long> entry : pending.entries()) {
            if (entry.getKey() < id) {
                id = entry.getKey();
                time = entry.getValue();
            }
        }
        return new long[] {id, time};
    }

    private void rememberSkipped(long first, long last) throws Exception {
        skipped.put(first, last);
        int count = 0;
        long oldest = Long.MAX_VALUE;
        for (Long start : skipped.keys()) {
            count++;
            oldest = Math.min(oldest, start);
        }
        if (count > MAX_SKIPPED_RANGES) {
            skipped.remove(oldest);
        }
    }

    /** If the ID lies in a skipped range, takes it out of the range and returns true. */
    private boolean takeFromSkipped(long id) throws Exception {
        Long first = null;
        long last = 0;
        for (Map.Entry<Long, Long> range : skipped.entries()) {
            if (range.getKey() <= id && id <= range.getValue()) {
                first = range.getKey();
                last = range.getValue();
                break;
            }
        }
        if (first == null) {
            return false;
        }
        skipped.remove(first);
        if (first < id) {
            skipped.put(first, id - 1);
        }
        if (id < last) {
            skipped.put(id + 1, last);
        }
        return true;
    }
}
