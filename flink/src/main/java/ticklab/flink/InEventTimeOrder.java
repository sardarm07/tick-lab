package ticklab.flink;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A keyed function that sees its input in event-time order, interleaved with its own timers, no
 * matter how the input arrives or how far the watermark jumps at once.
 *
 * <p>Each element waits in state until the watermark passes its timestamp; a timer then hands it
 * to {@link #onEvent}. Flink fires timers in timestamp order, so elements and the subclass's own
 * timers ({@link #onTime}) are handled strictly by event time. Handling elements as they arrive
 * would make the output depend on the watermark's progress: in a fast replay it jumps many
 * seconds at once, and an element can then be handled before a timer that comes earlier in
 * event time. Two replays of the same data would then disagree.
 *
 * <p>At one instant, the elements come first, then the subclass's timer.
 */
abstract class InEventTimeOrder<K, I, O> extends KeyedProcessFunction<K, I, O> {
    private final TypeInformation<I> type;
    private transient MapState<Long, List<I>> waiting;

    InEventTimeOrder(TypeInformation<I> type) {
        this.type = type;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        waiting = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("waiting", Types.LONG, Types.LIST(type)));
    }

    @Override
    public final void processElement(I value, Context context, Collector<O> out) throws Exception {
        long time = context.timestamp();
        List<I> atTime = waiting.get(time);
        if (atTime == null) {
            atTime = new ArrayList<>();
        }
        atTime.add(value);
        waiting.put(time, atTime);
        context.timerService().registerEventTimeTimer(time);
    }

    @Override
    public final void onTimer(long time, OnTimerContext context, Collector<O> out)
            throws Exception {
        List<I> due = waiting.get(time);
        if (due != null) {
            waiting.remove(time);
            List<I> ordered = new ArrayList<>(due);
            order(ordered);
            for (I value : ordered) {
                onEvent(value, time, context, out);
            }
        }
        onTime(time, context, out);
    }

    /**
     * Puts the elements of one instant in the order to handle them. They stay in arrival order
     * unless a subclass sorts them: needed when two inputs can arrive in either order.
     */
    void order(List<I> sameInstant) {}

    /** One element, at its event time; the watermark has passed it. */
    abstract void onEvent(I value, long time, OnTimerContext context, Collector<O> out)
            throws Exception;

    /** A timer the subclass registered, or an element's time: check what is due. */
    abstract void onTime(long time, OnTimerContext context, Collector<O> out) throws Exception;
}
