package ticklab.flink;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import ticklab.v1.Side;
import ticklab.v1.Whale;

/**
 * Whale aftershocks (decision note 0022), the same as {@code ticklab/whales.py}: one Binance
 * pair's trades, a second at a time.
 *
 * <ul>
 *   <li>Fills with the same time, the same taker side and consecutive IDs are one taker order.
 *   <li>An order is a whale when it is worth more than the 99.9th percentile of the pair's
 *       orders in the 60 complete minutes before: an order at or above the top of the
 *       percentile's bucket in a histogram of buckets 5% wide, at edges 1.05^k USDT computed
 *       exactly. None until 30 minutes have been seen.
 *   <li>Its aftershocks: the last price at or before 10, 30, 60 and 120 s after it, against its
 *       price, in bps, signed by its side.
 * </ul>
 *
 * <p>Seconds and horizons are handled in event-time order ({@link InEventTimeOrder}): a horizon
 * in a quiet second takes the price of the seconds before it, whatever the watermark does.
 */
class WhaleTracker extends InEventTimeOrder<String, SecondOfTrades, Whale> {
    static final long[] HORIZONS_S = {10, 30, 60, 120};
    static final int WINDOW_MINUTES = 60;
    static final int WARM_UP_MINUTES = 30;
    /** Order values are counted in units of 1e-10 USDT: prices have 2 decimals, quantities 8. */
    static final long VALUE_UNIT = 10_000_000_000L;
    static final long[] EDGES = edges(400);

    private transient ValueState<Long> close;
    private transient ValueState<Long> firstMinute;
    private transient ValueState<Long> minute;
    private transient MapState<Long, Map<Integer, Long>> minutes;
    private transient MapState<Integer, Long> window;
    private transient ValueState<Long> windowOrders;
    private transient ValueState<Integer> threshold;
    private transient ListState<PendingWhale> pending;

    WhaleTracker() {
        super(TypeInformation.of(SecondOfTrades.class));
    }

    /** 1.05^k USDT for k = 0..last, in value units: 21^k * 10^10 / 20^k, rounded down. */
    static long[] edges(int last) {
        long[] edges = new long[last + 1];
        for (int k = 0; k <= last; k++) {
            edges[k] = BigInteger.valueOf(21).pow(k)
                    .multiply(BigInteger.valueOf(VALUE_UNIT))
                    .divide(BigInteger.valueOf(20).pow(k))
                    .longValueExact();
        }
        return edges;
    }

    /** How many edges are at or below a value: bucket k holds values from edge k-1 to edge k. */
    static int bucket(long value) {
        int low = 0;
        int high = EDGES.length;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (EDGES[middle] <= value) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        close = value("close");
        firstMinute = value("first-minute");
        minute = value("minute");
        windowOrders = value("window-orders");
        minutes = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "minutes", Types.LONG, Types.MAP(Types.INT, Types.LONG)));
        window = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("window", Types.INT, Types.LONG));
        threshold = getRuntimeContext().getState(
                new ValueStateDescriptor<>("threshold-bucket", Types.INT));
        pending = getRuntimeContext().getListState(
                new ListStateDescriptor<>("pending", TypeInformation.of(PendingWhale.class)));
    }

    private ValueState<Long> value(String name) {
        return getRuntimeContext().getState(new ValueStateDescriptor<>(name, Types.LONG));
    }

    @Override
    void onEvent(SecondOfTrades second, long time, OnTimerContext context, Collector<Whale> out)
            throws Exception {
        long startMicros = second.secondMillis * 1000;
        settle(startMicros - 1, out);
        int i = 0;
        int n = second.ids.length;
        while (i < n) {
            int j = i + 1;
            while (j < n && second.ids[j] == second.ids[j - 1] + 1
                    && second.timesMicros[j] == second.timesMicros[i]
                    && second.takerBuy[j] == second.takerBuy[i]) {
                j++;
            }
            order(second, i, j, context, out);
            i = j;
        }
        settle(startMicros + 999_999, out);
    }

    @Override
    void onTime(long time, OnTimerContext context, Collector<Whale> out) throws Exception {
        settle(time * 1000 + 999, out);
    }

    /** The order of fills {@code from} (inclusive) to {@code to} (exclusive) of a second. */
    private void order(SecondOfTrades second, int from, int to, OnTimerContext context,
            Collector<Whale> out) throws Exception {
        long timeMicros = second.timesMicros[from];
        // Horizons before this order's time take the price before it.
        settle(timeMicros - 1, out);
        long orderMinute = Math.floorDiv(timeMicros, 60_000_000L);
        roll(orderMinute);
        long quantity = 0;
        long value = 0;
        for (int i = from; i < to; i++) {
            quantity += second.quantityE8[i];
            value = Math.addExact(value, Math.multiplyExact(second.priceCents[i],
                    second.quantityE8[i]));
        }
        int k = bucket(value);
        Integer top = threshold.value();
        if (top != null && k > top) {
            PendingWhale whale = new PendingWhale();
            whale.base = second.base;
            whale.timeMicros = timeMicros;
            whale.buy = second.takerBuy[from];
            whale.firstId = second.ids[from];
            whale.lastId = second.ids[to - 1];
            whale.fills = to - from;
            whale.quantityE8 = quantity;
            whale.value = value;
            whale.thresholdBucket = top;
            whale.windowOrders = windowOrders.value();
            whale.pricesCents = new long[HORIZONS_S.length];
            java.util.Arrays.fill(whale.pricesCents, -1);
            pending.add(whale);
            for (long horizon : HORIZONS_S) {
                long due = timeMicros + horizon * 1_000_000;
                context.timerService().registerEventTimeTimer(
                        Math.floorDiv(due, 1_000_000L) * 1000 + 999);
            }
        }
        Map<Integer, Long> counts = minutes.get(orderMinute);
        if (counts == null) {
            counts = new HashMap<>();
        }
        counts.merge(k, 1L, Long::sum);
        minutes.put(orderMinute, counts);
        close.update(second.priceCents[to - 1]);
    }

    /**
     * Moves the window to the 60 complete minutes before {@code orderMinute}, and finds its 99.9th
     * percentile once per minute.
     */
    private void roll(long orderMinute) throws Exception {
        if (firstMinute.value() == null) {
            firstMinute.update(orderMinute);
        }
        Long current = minute.value();
        if (current != null && current == orderMinute) {
            return;
        }
        long orders = windowOrders.value() == null ? 0 : windowOrders.value();
        if (current != null) {
            Map<Integer, Long> done = minutes.get(current);
            if (done != null) {
                for (Map.Entry<Integer, Long> entry : done.entrySet()) {
                    Long before = window.get(entry.getKey());
                    window.put(entry.getKey(), (before == null ? 0 : before) + entry.getValue());
                    orders += entry.getValue();
                }
            }
        }
        List<Long> old = new ArrayList<>();
        for (long m : minutes.keys()) {
            if (m < orderMinute - WINDOW_MINUTES) {
                old.add(m);
            }
        }
        for (long m : old) {
            for (Map.Entry<Integer, Long> entry : minutes.get(m).entrySet()) {
                window.put(entry.getKey(), window.get(entry.getKey()) - entry.getValue());
                orders -= entry.getValue();
            }
            minutes.remove(m);
        }
        windowOrders.update(orders);
        minute.update(orderMinute);
        threshold.clear();
        if (orderMinute - firstMinute.value() >= WARM_UP_MINUTES && orders > 0) {
            long rank = (999 * orders + 999) / 1000;
            TreeMap<Integer, Long> sorted = new TreeMap<>();
            for (Map.Entry<Integer, Long> entry : window.entries()) {
                if (entry.getValue() > 0) {
                    sorted.put(entry.getKey(), entry.getValue());
                }
            }
            long seen = 0;
            for (Map.Entry<Integer, Long> entry : sorted.entrySet()) {
                seen += entry.getValue();
                if (seen >= rank) {
                    threshold.update(entry.getKey());
                    break;
                }
            }
        }
    }

    /**
     * Gives each waiting horizon due at or before {@code untilMicros} the price now, and reports
     * the whales whose last horizon is done.
     */
    private void settle(long untilMicros, Collector<Whale> out) throws Exception {
        Long now = close.value();
        List<PendingWhale> keep = new ArrayList<>();
        boolean changed = false;
        for (PendingWhale whale : pending.get()) {
            for (int i = 0; i < HORIZONS_S.length; i++) {
                long due = whale.timeMicros + HORIZONS_S[i] * 1_000_000;
                if (whale.pricesCents[i] < 0 && due <= untilMicros && now != null) {
                    whale.pricesCents[i] = now;
                    changed = true;
                }
            }
            if (whale.pricesCents[HORIZONS_S.length - 1] >= 0 && allSet(whale)) {
                out.collect(whale.toAvro());
                changed = true;
            } else {
                keep.add(whale);
            }
        }
        if (changed) {
            pending.update(keep);
        }
    }

    private static boolean allSet(PendingWhale whale) {
        for (long price : whale.pricesCents) {
            if (price < 0) {
                return false;
            }
        }
        return true;
    }

    /** A whale waiting for its horizons (a Flink POJO). */
    public static class PendingWhale {
        public String base;
        public long timeMicros;
        public boolean buy;
        public long firstId;
        public long lastId;
        public int fills;
        public long quantityE8;
        public long value;
        public int thresholdBucket;
        public long windowOrders;
        public long[] pricesCents;

        public PendingWhale() {}

        Whale toAvro() {
            BigDecimal valueUsdt = BigDecimal.valueOf(value, 10);
            BigDecimal quantity = BigDecimal.valueOf(quantityE8, 8);
            BigDecimal price = Decimals.divide(valueUsdt, quantity);
            List<BigDecimal> moves = new ArrayList<>();
            for (long cents : pricesCents) {
                BigDecimal move = MoveDetector.bps(BigDecimal.valueOf(cents, 2), price);
                moves.add(buy ? move : move.negate());
            }
            return Whale.newBuilder()
                    .setBase(base)
                    .setTime(TradeEvent.instant(timeMicros))
                    .setSide(buy ? Side.BUY : Side.SELL)
                    .setFirstId(firstId)
                    .setLastId(lastId)
                    .setFills(fills)
                    .setQuantity(quantity.setScale(Decimals.SCALE))
                    .setValue(valueUsdt.setScale(Decimals.SCALE))
                    .setPrice(price)
                    .setThreshold(BigDecimal.valueOf(EDGES[thresholdBucket], 10)
                            .setScale(Decimals.SCALE))
                    .setWindowOrders(windowOrders)
                    .setMove10s(moves.get(0))
                    .setMove30s(moves.get(1))
                    .setMove60s(moves.get(2))
                    .setMove120s(moves.get(3))
                    .build();
        }
    }
}
