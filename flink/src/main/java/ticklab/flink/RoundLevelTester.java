package ticklab.flink;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import ticklab.v1.Approach;
import ticklab.v1.LevelKind;
import ticklab.v1.LevelOutcome;
import ticklab.v1.LevelTier;
import ticklab.v1.RoundLevelTest;

/**
 * Round-number magnets (decision note 0021): one pair's trades, a second at a time, tested
 * against its round levels and random control levels in two tiers.
 *
 * <ul>
 *   <li>A test of a level starts at the first trade that reaches it from beyond its band. The
 *       level is then disarmed until the price is beyond its band again, on either side, and it
 *       is tested once at a time. A test is identified by the trade that reached the level.
 *   <li>The close at the end of the second 60 s later decides it: BREAK beyond the band on the
 *       far side, BOUNCE beyond it on the near side, NEUTRAL in between.
 *   <li>A test open when the trade IDs skip (a gap in the data) is discarded.
 * </ul>
 *
 * <p>Seconds and decisions are handled strictly in event-time order ({@link InEventTimeOrder}):
 * a decision due in a quiet second must see the close of the seconds before it, not of a later
 * one that a jump of the watermark delivered first.
 */
class RoundLevelTester extends InEventTimeOrder<String, SecondOfTrades, RoundLevelTest> {
    static final String[] TIERS = {"MAJOR", "MINOR"};
    static final String[] KINDS = {"ROUND", "RANDOM"};
    private static final int BELOW = 1;
    private static final int ABOVE = 2;
    private static final int DISARMED = 0;

    private final RoundLevelRules rules;
    private transient ValueState<Long> price;
    private transient ValueState<Long> lastId;
    private transient ValueState<Long> close;
    private transient Map<String, MapState<Long, Integer>> levels;
    private transient ListState<OpenTest> open;
    private transient Map<String, Long> randoms;
    private transient Counter discarded;

    RoundLevelTester(RoundLevelRules rules) {
        super(TypeInformation.of(SecondOfTrades.class));
        this.rules = rules;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        price = value("price");
        lastId = value("last-id");
        close = value("close");
        levels = new HashMap<>();
        for (String tier : TIERS) {
            for (String kind : KINDS) {
                levels.put(tier + ":" + kind, getRuntimeContext().getMapState(
                        new MapStateDescriptor<>(tier + "-" + kind, Types.LONG, Types.INT)));
            }
        }
        open = getRuntimeContext().getListState(
                new ListStateDescriptor<>("open-tests", TypeInformation.of(OpenTest.class)));
        randoms = new HashMap<>();
        discarded = getRuntimeContext().getMetricGroup().counter("discardedTests");
    }

    private ValueState<Long> value(String name) {
        return getRuntimeContext().getState(new ValueStateDescriptor<>(name, Types.LONG));
    }

    @Override
    void onEvent(SecondOfTrades second, long time, OnTimerContext context,
            Collector<RoundLevelTest> out) throws Exception {
        for (int i = 0; i < second.ids.length; i++) {
            trade(second.base, second.ids[i], second.timesMicros[i], second.priceCents[i],
                    context);
        }
    }

    /** Decides the open tests due by the end of this instant's second. */
    @Override
    void onTime(long time, OnTimerContext context, Collector<RoundLevelTest> out)
            throws Exception {
        List<OpenTest> keep = new ArrayList<>();
        for (OpenTest test : open.get()) {
            if (test.decideSecond * 1000 + 999 <= time) {
                out.collect(decided(context.getCurrentKey(), test, close.value()));
            } else {
                keep.add(test);
            }
        }
        open.update(keep);
    }

    private void trade(String base, long id, long timeMicros, long cents,
            OnTimerContext context) throws Exception {
        Long previousId = lastId.value();
        if (previousId != null && id != previousId + 1) {
            for (OpenTest ignored : open.get()) {
                discarded.inc();
            }
            open.clear();
        }
        lastId.update(id);
        close.update(cents);
        Long last = price.value();
        long before = last == null ? cents : last;
        price.update(cents);
        if (cents == before) {
            return;
        }
        long band = rules.bands().get(base);
        for (String tier : TIERS) {
            long step = rules.steps().get(base).get(tier);
            for (String kind : KINDS) {
                MapState<Long, Integer> states = levels.get(tier + ":" + kind);
                for (long level : candidates(base, tier, kind, before, cents, band)) {
                    Integer known = states.get(level);
                    int state = known == null ? side(before, level, band) : known;
                    if (state == BELOW && cents >= level) {
                        start(tier, kind, level, true, timeMicros, id, context);
                        state = DISARMED;
                    } else if (state == ABOVE && cents <= level) {
                        start(tier, kind, level, false, timeMicros, id, context);
                        state = DISARMED;
                    }
                    int now = side(cents, level, band);
                    states.put(level, now == DISARMED ? state : now);
                }
                List<Long> far = new ArrayList<>();
                for (long level : states.keys()) {
                    if (Math.abs(level - cents) > rules.keepSteps() * step) {
                        far.add(level);
                    }
                }
                for (long level : far) {
                    states.remove(level);
                }
            }
        }
    }

    /** The levels whose band the move from {@code a} to {@code b} touches, in increasing order. */
    List<Long> candidates(String base, String tier, String kind, long a, long b, long band) {
        long low = Math.min(a, b) - band;
        long high = Math.max(a, b) + band;
        long step = rules.steps().get(base).get(tier);
        List<Long> found = new ArrayList<>();
        for (long k = Math.floorDiv(low, step); k <= Math.floorDiv(high, step); k++) {
            long level;
            if (kind.equals("ROUND")) {
                level = k * step;
            } else {
                level = randomLevel(base, tier, k);
            }
            if (low <= level && level <= high) {
                found.add(level);
            }
        }
        return found;
    }

    /**
     * The control level in step {@code k} of a tier, in cents: a random place in one of the minor
     * steps inside it, at least a tenth of a minor step from either end. Drawn from SHA-256 of the
     * seed, base, tier and step, so it is the same in the job and in Python.
     */
    long randomLevel(String base, String tier, long k) {
        String key = base + ":" + tier + ":" + k;
        if (randoms == null) {
            randoms = new HashMap<>();
        }
        Long cached = randoms.get(key);
        if (cached != null) {
            return cached;
        }
        byte[] digest = sha256(rules.seed() + ":" + key);
        BigInteger first = new BigInteger(1, Arrays.copyOfRange(digest, 0, 8));
        BigInteger second = new BigInteger(1, Arrays.copyOfRange(digest, 8, 16));
        long minor = rules.steps().get(base).get("MINOR");
        long within = tier.equals("MAJOR") ? first.mod(BigInteger.TEN).longValueExact() : 0;
        BigInteger width = BigInteger.valueOf(minor * 8 / 10);
        long offset = minor / 10 + second.multiply(width).shiftRight(64).longValueExact();
        long level = k * rules.steps().get(base).get(tier) + within * minor + offset;
        randoms.put(key, level);
        return level;
    }

    private static byte[] sha256(String text) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(text.getBytes(UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int side(long price, long level, long band) {
        if (price <= level - band) {
            return BELOW;
        }
        if (price >= level + band) {
            return ABOVE;
        }
        return DISARMED;
    }

    private void start(String tier, String kind, long level, boolean up, long timeMicros,
            long tradeId, OnTimerContext context) throws Exception {
        for (OpenTest test : open.get()) {
            if (test.tier.equals(tier) && test.kind.equals(kind) && test.level == level) {
                return; // tested once at a time
            }
        }
        long decisionMicros = rules.decision().toNanos() / 1000;
        long decideSecond = Math.floorDiv(timeMicros + decisionMicros, 1_000_000L);
        open.add(new OpenTest(tier, kind, level, up, timeMicros, tradeId, decideSecond));
        context.timerService().registerEventTimeTimer(decideSecond * 1000 + 999);
    }

    private RoundLevelTest decided(String base, OpenTest test, long closeCents) {
        long band = rules.bands().get(base);
        long beyond = test.up ? closeCents - test.level : test.level - closeCents;
        LevelOutcome outcome = beyond >= band
                ? LevelOutcome.BREAK
                : beyond <= -band ? LevelOutcome.BOUNCE : LevelOutcome.NEUTRAL;
        return RoundLevelTest.newBuilder()
                .setBase(base)
                .setTier(LevelTier.valueOf(test.tier))
                .setKind(LevelKind.valueOf(test.kind))
                .setLevel(dollars(test.level))
                .setApproach(test.up ? Approach.FROM_BELOW : Approach.FROM_ABOVE)
                .setTouchTime(TradeEvent.instant(test.timeMicros))
                .setTouchTradeId(test.tradeId)
                .setDecideSecond(java.time.Instant.ofEpochSecond(test.decideSecond))
                .setClose(dollars(closeCents))
                .setOutcome(outcome)
                .setBand(dollars(band))
                .build();
    }

    private static BigDecimal dollars(long cents) {
        return BigDecimal.valueOf(cents, 2).setScale(Decimals.SCALE);
    }
}
