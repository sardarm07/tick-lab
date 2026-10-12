package ticklab.flink;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import ticklab.v1.Direction;
import ticklab.v1.Exchange;
import ticklab.v1.LeadLag;

/**
 * Pairs the moves of one base across the two exchanges (decision note 0020). In event-time
 * order, each move takes the earliest unpaired move of the other exchange in the same direction
 * at most {@code window} before it, and the earlier of the two leads. A move that no move of
 * the other exchange reaches within the window is reported unpaired.
 *
 * <p>A move's Flink timestamp is the end of its second, when that second's window fired, so the
 * moves of one second are put in order of their own times here, then by exchange. A timer at the
 * end of each second in which a waiting move's window closes reports it unpaired: by then every
 * move that could still reach it has been seen.
 */
class LeadLagMatcher extends InEventTimeOrder<String, Move, LeadLag> {
    private static final Comparator<Move> BY_TIME =
            Comparator.comparingLong((Move move) -> move.timeMicros).thenComparing(m -> m.exchange);

    private final Map<String, BigDecimal> thresholds;
    private final long windowMicros;
    private transient ListState<Move> waiting;

    LeadLagMatcher(LeadLagRules rules) {
        super(TypeInformation.of(Move.class));
        this.thresholds = rules.thresholds();
        this.windowMicros = rules.window().toNanos() / 1000;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        waiting = getRuntimeContext().getListState(
                new ListStateDescriptor<>("waiting-moves", TypeInformation.of(Move.class)));
    }

    @Override
    void order(List<Move> sameInstant) {
        sameInstant.sort(BY_TIME);
    }

    @Override
    void onEvent(Move move, long time, OnTimerContext context, Collector<LeadLag> out)
            throws Exception {
        List<Move> moves = new ArrayList<>();
        waiting.get().forEach(moves::add);
        for (Move earlier : moves) {
            if (!earlier.exchange.equals(move.exchange) && earlier.up == move.up
                    && move.timeMicros - earlier.timeMicros <= windowMicros) {
                moves.remove(earlier);
                waiting.update(moves);
                out.collect(record(earlier, move));
                return;
            }
        }
        moves.add(move);
        waiting.update(moves);
        context.timerService().registerEventTimeTimer(secondEnd(move.timeMicros + windowMicros));
    }

    @Override
    void onTime(long time, OnTimerContext context, Collector<LeadLag> out) throws Exception {
        // Every move from now on is at least this late, in microseconds.
        long horizon = (time + 1) * 1000;
        List<Move> keep = new ArrayList<>();
        for (Move move : waiting.get()) {
            if (move.timeMicros + windowMicros < horizon) {
                out.collect(record(move, null));
            } else {
                keep.add(move);
            }
        }
        waiting.update(keep);
    }

    /** The end of the second containing {@code micros}, in milliseconds: a move's timestamp. */
    static long secondEnd(long micros) {
        return Math.floorDiv(micros, 1_000_000L) * 1000 + 999;
    }

    private LeadLag record(Move leader, Move follower) {
        return LeadLag.newBuilder()
                .setBase(leader.base)
                .setDirection(leader.up ? Direction.UP : Direction.DOWN)
                .setLeader(Exchange.valueOf(leader.exchange))
                .setLeaderTime(TradeEvent.instant(leader.timeMicros))
                .setLeaderBps(leader.bps)
                .setFollowerTime(follower == null ? null : TradeEvent.instant(follower.timeMicros))
                .setFollowerBps(follower == null ? null : follower.bps)
                .setLeadUs(follower == null ? null : follower.timeMicros - leader.timeMicros)
                .setThresholdBps(Decimals.exact(thresholds.get(leader.base)))
                .build();
    }
}
