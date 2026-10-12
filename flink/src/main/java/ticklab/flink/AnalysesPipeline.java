package ticklab.flink;

import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import ticklab.v1.LeadLag;
import ticklab.v1.RoundLevelTest;

/**
 * The analyses, separate from sources and sinks so tests can run them on any input. Both work on
 * 1-second windows of each feed's trades: trades later than the watermark miss their window and
 * are left out, so like the spreads, these are signals about now, not records.
 *
 * <ul>
 *   <li>Who moves first (milestone 6, decision note 0020): each feed's seconds become moves
 *       ({@link MoveDetector}), and each base's moves on the two exchanges are paired
 *       ({@link LeadLagMatcher}).
 *   <li>Round-number magnets (milestone 7, decision note 0021): each Binance pair's seconds are
 *       tested against round and random levels ({@link RoundLevelTester}).
 * </ul>
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

    public static DataStream<RoundLevelTest> roundLevels(DataStream<TradeEvent> unique,
            RoundLevelRules rules) {
        return unique
                .filter(trade -> trade.exchange.equals("BINANCE")
                        && rules.bands().containsKey(trade.base))
                .name("binance-pairs")
                .uid("binance-pairs")
                .keyBy(trade -> trade.feed, Types.STRING)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
                .process(new SecondCollector())
                .name("seconds")
                .uid("seconds")
                .keyBy(second -> second.base, Types.STRING)
                .process(new RoundLevelTester(rules))
                .name("round-levels")
                .uid("round-levels");
    }
}
