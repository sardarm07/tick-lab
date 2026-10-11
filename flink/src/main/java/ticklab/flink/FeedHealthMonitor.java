package ticklab.flink;

import java.time.Instant;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import ticklab.v1.Exchange;
import ticklab.v1.FeedHealth;
import ticklab.v1.HealthEvent;

/**
 * Notices when a feed goes silent, with processing-time timers.
 *
 * <p>Event-time timers can't do this: event time only advances when trades arrive, so when the
 * trades stop, event time stops too and the timer never fires. Processing time is the machine's
 * clock, which keeps moving. The price is determinism: replaying the same data can give
 * different SILENT events, so they are left out of replay comparisons.
 *
 * <p>Each trade pushes the feed's deadline to now + its threshold (from the M1 data study). If
 * the deadline passes first, the feed is SILENT; its next trade makes it RECOVERED.
 */
public class FeedHealthMonitor extends KeyedProcessFunction<String, TradeEvent, FeedHealth> {
    private static final int DEFAULT_THRESHOLD_S = 60;

    private final Map<String, Integer> thresholdSeconds;

    private transient ValueState<Long> lastSeen; // processing time of the newest trade
    private transient ValueState<Long> deadline; // the pending timer
    private transient ValueState<Boolean> silent;

    public FeedHealthMonitor(Map<String, Integer> thresholdSeconds) {
        this.thresholdSeconds = Map.copyOf(thresholdSeconds);
    }

    @Override
    public void open(OpenContext context) {
        lastSeen = getRuntimeContext().getState(new ValueStateDescriptor<>("last-seen", Types.LONG));
        deadline = getRuntimeContext().getState(new ValueStateDescriptor<>("deadline", Types.LONG));
        silent = getRuntimeContext().getState(new ValueStateDescriptor<>("silent", Types.BOOLEAN));
    }

    @Override
    public void processElement(TradeEvent trade, Context context, Collector<FeedHealth> out)
            throws Exception {
        long now = context.timerService().currentProcessingTime();
        if (Boolean.TRUE.equals(silent.value())) {
            out.collect(event(context.getCurrentKey(), HealthEvent.RECOVERED, now, lastSeen.value()));
            silent.update(false);
        }
        lastSeen.update(now);
        Long previous = deadline.value();
        if (previous != null) {
            context.timerService().deleteProcessingTimeTimer(previous);
        }
        long next = now + threshold(context.getCurrentKey()) * 1000L;
        context.timerService().registerProcessingTimeTimer(next);
        deadline.update(next);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<FeedHealth> out)
            throws Exception {
        Long expected = deadline.value();
        if (expected == null || expected != timestamp) {
            return; // superseded by a newer trade
        }
        deadline.clear();
        silent.update(true);
        out.collect(event(context.getCurrentKey(), HealthEvent.SILENT, timestamp, lastSeen.value()));
    }

    private int threshold(String feed) {
        return thresholdSeconds.getOrDefault(feed, DEFAULT_THRESHOLD_S);
    }

    private FeedHealth event(String feed, HealthEvent type, long at, Long lastSeenAt) {
        String[] parts = feed.split(":", 2);
        return FeedHealth.newBuilder()
                .setExchange(Exchange.valueOf(parts[0]))
                .setSymbol(parts[1])
                .setEventType(type)
                .setEventId(at)
                .setLastTradeSeenAt(Instant.ofEpochMilli(lastSeenAt == null ? at : lastSeenAt))
                .setThresholdSeconds(threshold(feed))
                .setAt(Instant.ofEpochMilli(at))
                .build();
    }
}
