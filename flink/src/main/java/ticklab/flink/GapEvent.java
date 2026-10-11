package ticklab.flink;

import java.time.Instant;
import ticklab.v1.Exchange;
import ticklab.v1.Gap;
import ticklab.v1.GapStatus;

/** A gap in one feed's trade IDs, tracked by {@link Dedup} (a Flink POJO). */
public class GapEvent {
    public String feed;
    /** The first missing ID when the gap was detected: the gap's identity. */
    public long firstMissingId;

    public long lastMissingId;
    /** Trade time of the first trade after the gap, in microseconds. */
    public long afterTradeTimeMicros;

    public boolean detected;
    public String status;
    public long atMillis;

    public GapEvent() {}

    GapEvent(String feed, long firstMissingId, long lastMissingId, long afterTradeTimeMicros) {
        this.feed = feed;
        this.firstMissingId = firstMissingId;
        this.lastMissingId = lastMissingId;
        this.afterTradeTimeMicros = afterTradeTimeMicros;
    }

    GapEvent withStatus(String status, long atMillis) {
        GapEvent event = new GapEvent(feed, firstMissingId, lastMissingId, afterTradeTimeMicros);
        event.detected = detected;
        event.status = status;
        event.atMillis = atMillis;
        return event;
    }

    public Gap toAvro() {
        String[] parts = feed.split(":", 2);
        return Gap.newBuilder()
                .setExchange(Exchange.valueOf(parts[0]))
                .setSymbol(parts[1])
                .setFirstMissingId(firstMissingId)
                .setLastMissingId(lastMissingId)
                .setStatus(GapStatus.valueOf(status))
                .setAfterTradeTime(TradeEvent.instant(afterTradeTimeMicros))
                .setTradesFound(0)
                .setDetail("")
                .setUpdatedAt(Instant.ofEpochMilli(atMillis))
                .build();
    }

    @Override
    public String toString() {
        return feed + " " + status + " " + firstMissingId + ".." + lastMissingId;
    }
}
