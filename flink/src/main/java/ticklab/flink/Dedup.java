package ticklab.flink;

import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Lets each trade through once, per feed.
 *
 * <p>Trade IDs increase within a feed, so most of what has been seen fits in one number,
 * {@code upTo}: every ID up to it has arrived. IDs above it that arrive early, ahead of a missing
 * one, wait in a small map until the gap below them fills. The state stays tiny, and the same
 * state shows where the gaps are.
 *
 * <p>A gap still open once the watermark is the lateness budget past the first trade after it
 * is given up on: its IDs are recorded as skipped and {@code upTo} moves past them. If they
 * turn up later anyway, each is let through once, as a late trade, instead of being mistaken for
 * a repeat.
 *
 * <p>At the start of a feed, nothing is known about earlier IDs. For one watermark bound, every
 * ID is kept in the map, so trades reordered within the bound are still let through.
 */
public class Dedup extends KeyedProcessFunction<String, TradeEvent, TradeEvent> {
    private static final int MAX_SKIPPED_RANGES = 1000;

    private final long warmUpMillis;
    private final long latenessMillis;

    private transient ValueState<Long> upTo;
    private transient MapState<Long, Long> pending; // trade ID -> event time in milliseconds
    private transient MapState<Long, Long> skipped; // first skipped ID -> last skipped ID
    private transient Counter duplicates;
    private transient Counter skippedIds;
    private transient Counter recovered;

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
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        duplicates = metrics.counter("duplicates");
        skippedIds = metrics.counter("skippedIds");
        recovered = metrics.counter("recoveredAfterSkip");
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
            pending.put(id, trade.eventTimeMillis());
        } else if (id == known + 1) {
            upTo.update(compact(id));
        } else {
            if (pending.isEmpty()) {
                context.timerService()
                        .registerEventTimeTimer(trade.eventTimeMillis() + latenessMillis);
            }
            pending.put(id, trade.eventTimeMillis());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<TradeEvent> out)
            throws Exception {
        long watermark = context.timerService().currentWatermark();
        Long known = upTo.value();
        if (known == null) { // end of the warm-up: start counting from the lowest ID seen
            known = compact(lowestPending()[0] - 1);
        }
        while (!pending.isEmpty()) {
            long[] first = lowestPending(); // the first trade after the lowest open gap
            if (first[1] + latenessMillis > watermark) {
                context.timerService().registerEventTimeTimer(first[1] + latenessMillis);
                break;
            }
            rememberSkipped(known + 1, first[0] - 1);
            skippedIds.inc(first[0] - 1 - known);
            known = compact(first[0] - 1);
        }
        upTo.update(known);
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

    /** {lowest pending ID, its event time}. */
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
