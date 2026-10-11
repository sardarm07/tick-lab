package ticklab.flink;

import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import ticklab.v1.Candle;
import ticklab.v1.FeedHealth;

/**
 * The core pipeline, separate from sources and sinks so tests can run it on any input:
 * trades, then deduplication and gap tracking per feed, then 1-minute event-time candles and
 * silence detection.
 */
public final class CorePipeline {
    /** B: how far each feed's watermark trails its newest trade time (M1 data study). */
    public static final Duration WATERMARK_BOUND = Duration.ofSeconds(2);

    /** Live data: a partition with no trades for this long stops holding back the watermark. */
    public static final Duration IDLENESS = Duration.ofSeconds(10);

    /** L: how long after the watermark passes a minute its candle still accepts late trades. */
    public static final Duration LATENESS = Duration.ofMinutes(5);

    /** Trades too late even for L: kept on a side output, never dropped silently. */
    public static final OutputTag<TradeEvent> LATE_TRADES =
            new OutputTag<>("late-trades", TypeInformation.of(TradeEvent.class));

    private CorePipeline() {}

    /**
     * Event time is the trade time; the watermark trails the newest trade by the bound.
     *
     * <p>With an idle timeout, a partition that gets no trades for that long, by the machine's
     * clock, stops holding back the watermark, so a quiet feed doesn't freeze every window. Live
     * data needs it. A replay must not use it: while the job works through a backlog, a
     * partition can wait its turn in the Kafka consumer for longer than the timeout, be taken for
     * idle, and then see its trades counted as late. Replays pass {@link Duration#ZERO}.
     */
    public static WatermarkStrategy<TradeEvent> watermarks(Duration bound, Duration idleness) {
        WatermarkStrategy<TradeEvent> strategy = WatermarkStrategy
                .<TradeEvent>forBoundedOutOfOrderness(bound)
                .withTimestampAssigner((trade, kafkaTimestamp) -> trade.eventTimeMillis());
        return idleness.isZero() ? strategy : strategy.withIdleness(idleness);
    }

    public record Streams(
            DataStream<Candle> candles,
            DataStream<TradeEvent> lateTrades,
            DataStream<GapEvent> gaps,
            DataStream<FeedHealth> health) {}

    public static Streams build(
            DataStream<TradeEvent> trades,
            Duration bound,
            Duration lateness,
            Map<String, Integer> silenceSeconds) {
        SingleOutputStreamOperator<TradeEvent> unique = trades
                .keyBy(trade -> trade.feed, Types.STRING)
                .process(new Dedup(bound, lateness))
                .name("dedup")
                .uid("dedup");
        SingleOutputStreamOperator<Candle> candles = unique
                .keyBy(trade -> trade.feed, Types.STRING)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(lateness)
                .sideOutputLateData(LATE_TRADES)
                .aggregate(new CandleAggregate(), new CandleWindowFunction())
                .name("candles-1m")
                .uid("candles-1m");
        SingleOutputStreamOperator<FeedHealth> health = unique
                .keyBy(trade -> trade.feed, Types.STRING)
                .process(new FeedHealthMonitor(silenceSeconds))
                .name("feed-health")
                .uid("feed-health");
        return new Streams(
                candles, candles.getSideOutput(LATE_TRADES), unique.getSideOutput(Dedup.GAPS), health);
    }
}
