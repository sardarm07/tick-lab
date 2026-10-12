package ticklab.flink;

import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import ticklab.v1.LeadLag;

/**
 * The analyses, separate from sources and sinks so tests can run them on any input.
 *
 * <p>Who moves first (milestone 6, decision note 0020): each feed's 1-second windows become
 * moves ({@link MoveDetector}), and each base's moves on the two exchanges are paired
 * ({@link LeadLagMatcher}). Trades later than the watermark miss their window and are left out:
 * like the spreads, this is a signal about now, not a record.
 */
public final class AnalysesPipeline {
    private AnalysesPipeline() {}

    public static DataStream<LeadLag> leadLag(DataStream<TradeEvent> unique, LeadLagRules rules) {
        DataStream<Move> moves = unique
                .filter(trade -> rules.thresholds().containsKey(trade.base))
                .name("compared-markets")
                .uid("compared-markets")
                .keyBy(trade -> trade.feed, Types.STRING)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
                .process(new MoveDetector(rules))
                .name("moves")
                .uid("moves");
        return moves
                .keyBy(move -> move.base, Types.STRING)
                .process(new LeadLagMatcher(rules))
                .name("lead-lag")
                .uid("lead-lag");
    }
}
