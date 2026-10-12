package ticklab.flink;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The moves of one feed, from its 1-second windows of trades (decision note 0020):
 *
 * <ul>
 *   <li>A second's close is its last trade by ID. Its reference is the close of the feed's last
 *       earlier second with trades, if that second began at most {@code staleAfter} before; an
 *       older price is too stale to compare with, and the second only sets a new reference.
 *   <li>The second is a move when its close is more than the base's threshold from the
 *       reference, in bps. The move is timed by the second's first trade (by ID) already beyond
 *       the threshold in the move's direction: when this exchange's price first showed it.
 * </ul>
 *
 * <p>The windows of one feed fire in time order, so the reference kept in the feed's state is
 * always that of its previous second with trades.
 */
class MoveDetector extends ProcessWindowFunction<TradeEvent, Move, String, TimeWindow> {
    private static final BigDecimal BPS = new BigDecimal(10_000);

    private final Map<String, BigDecimal> thresholds;
    private final long staleAfterMillis;
    private final ValueStateDescriptor<BigDecimal> reference =
            new ValueStateDescriptor<>("reference", Types.BIG_DEC);
    private final ValueStateDescriptor<Long> referenceSecond =
            new ValueStateDescriptor<>("reference-second", Types.LONG);

    MoveDetector(LeadLagRules rules) {
        this.thresholds = rules.thresholds();
        this.staleAfterMillis = rules.staleAfter().toMillis();
    }

    @Override
    public void process(String feed, Context context, Iterable<TradeEvent> window,
            Collector<Move> out) throws Exception {
        List<TradeEvent> trades = new ArrayList<>();
        window.forEach(trades::add);
        trades.sort(Comparator.comparingLong(trade -> trade.tradeId));
        TradeEvent last = trades.get(trades.size() - 1);
        long second = context.window().getStart();

        ValueState<BigDecimal> referenceState = context.globalState().getState(reference);
        ValueState<Long> referenceSecondState = context.globalState().getState(referenceSecond);
        BigDecimal before = referenceState.value();
        Long beforeSecond = referenceSecondState.value();
        referenceState.update(last.price);
        referenceSecondState.update(second);
        BigDecimal threshold = thresholds.get(last.base);
        if (before == null || second - beforeSecond > staleAfterMillis || threshold == null) {
            return;
        }
        BigDecimal bps = bps(last.price, before);
        if (bps.abs().compareTo(threshold) <= 0) {
            return;
        }
        boolean up = bps.signum() > 0;
        for (TradeEvent trade : trades) {
            BigDecimal change = bps(trade.price, before);
            if (up ? change.compareTo(threshold) > 0 : change.compareTo(threshold.negate()) < 0) {
                out.collect(new Move(
                        trade.exchange, trade.base, second, trade.tradeTimeMicros, up, bps));
                return;
            }
        }
        throw new IllegalStateException("the closing trade itself is beyond the threshold");
    }

    /** (price - reference) / reference in bps, rounded half-even to 12 places. */
    static BigDecimal bps(BigDecimal price, BigDecimal reference) {
        return Decimals.divide(price.subtract(reference).multiply(BPS), reference);
    }
}
