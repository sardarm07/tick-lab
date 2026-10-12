package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import ticklab.v1.Whale;

/** Whales on made-up BTC trades: the same cases as tests/test_whales.py. */
class WhaleTrackerTest {
    static final long S = 1_791_700_020L;

    record Trade(long id, long atMicros, String price, String quantity, boolean buy) {}

    static Trade t(long id, long atMicros, String price, String quantity) {
        return new Trade(id, atMicros, price, quantity, true);
    }

    /** One $100 order a minute, for {@code minutes} minutes. */
    static List<Trade> calm(int minutes) {
        List<Trade> trades = new ArrayList<>();
        for (int m = 0; m < minutes; m++) {
            trades.add(t(m + 1, m * 60_000_000L, "100", "1"));
        }
        return trades;
    }

    static List<Whale> run(List<Trade> trades) throws Exception {
        TreeMap<Long, List<Trade>> seconds = new TreeMap<>();
        for (Trade trade : trades) {
            long micros = S * 1_000_000 + trade.atMicros();
            seconds.computeIfAbsent(Math.floorDiv(micros, 1_000_000L), k -> new ArrayList<>())
                    .add(trade);
        }
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new WhaleTracker()),
                (SecondOfTrades second) -> second.base,
                Types.STRING)) {
            h.open();
            for (var entry : seconds.entrySet()) {
                List<Trade> in = entry.getValue();
                in.sort((a, b) -> Long.compare(a.id(), b.id()));
                SecondOfTrades second = new SecondOfTrades();
                second.base = "BTC";
                second.secondMillis = entry.getKey() * 1000;
                second.ids = in.stream().mapToLong(Trade::id).toArray();
                second.timesMicros = in.stream()
                        .mapToLong(x -> S * 1_000_000 + x.atMicros()).toArray();
                second.priceCents = in.stream()
                        .mapToLong(x -> new BigDecimal(x.price()).movePointRight(2).longValue())
                        .toArray();
                second.quantityE8 = in.stream()
                        .mapToLong(x -> new BigDecimal(x.quantity()).movePointRight(8).longValue())
                        .toArray();
                second.takerBuy = new boolean[in.size()];
                for (int i = 0; i < in.size(); i++) {
                    second.takerBuy[i] = in.get(i).buy();
                }
                h.processElement(second, second.secondMillis + 999);
            }
            h.processWatermark(new Watermark(Long.MAX_VALUE));
            return h.extractOutputValues();
        }
    }

    @Test
    void bucketEdgesAreExactPowersOfOnePointZeroFive() {
        assertThat(WhaleTracker.EDGES[0]).isEqualTo(10_000_000_000L);
        assertThat(WhaleTracker.EDGES[1]).isEqualTo(10_500_000_000L);
        assertThat(WhaleTracker.EDGES[2]).isEqualTo(11_025_000_000L);
        assertThat(WhaleTracker.bucket(9_999_999_999L)).isZero();
        assertThat(WhaleTracker.bucket(10_500_000_000L)).isEqualTo(2);
    }

    @Test
    void fillsOfOneTakerOrderAreGrouped() throws Exception {
        long big = 32 * 60_000_000L;
        List<Trade> trades = calm(32);
        for (int i = 0; i < 4; i++) {
            trades.add(t(100 + i, big, "100", "2.5"));
        }
        trades.add(new Trade(104, big, "100", "1", false));
        List<Whale> found = run(trades);
        assertThat(found).hasSize(1);
        Whale whale = found.get(0);
        assertThat(List.of(whale.getFirstId(), whale.getLastId(), (long) whale.getFills()))
                .containsExactly(100L, 103L, 4L);
        assertThat(whale.getValue()).isEqualByComparingTo("1000");
        assertThat(whale.getPrice()).isEqualByComparingTo("100");
    }

    @Test
    void aWhaleNeedsThirtyMinutesOfHistoryAndAnOrderAboveThePercentile() throws Exception {
        List<Trade> early = calm(20);
        early.add(t(100, 20 * 60_000_000L, "100", "50"));
        assertThat(run(early)).isEmpty();
        List<Trade> trades = calm(31);
        trades.add(t(100, 31 * 60_000_000L + 30_000_000, "100", "10"));
        List<Whale> found = run(trades);
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getWindowOrders()).isEqualTo(31);
        long top = WhaleTracker.EDGES[WhaleTracker.bucket(100 * WhaleTracker.VALUE_UNIT)];
        assertThat(found.get(0).getThreshold()).isEqualByComparingTo(BigDecimal.valueOf(top, 10));
    }

    @Test
    void ordersOfTheWhalesOwnMinuteAreNotInItsWindow() throws Exception {
        long at = 31 * 60_000_000L;
        List<Trade> trades = calm(31);
        trades.add(t(100, at, "100", "10"));
        trades.add(t(101, at + 1_000_000, "100", "10"));
        assertThat(run(trades)).extracting(Whale::getWindowOrders).containsExactly(31L, 31L);
    }

    @Test
    void aftershocksAreSignedByTheWhalesSide() throws Exception {
        long at = 31 * 60_000_000L + 30_000_000;
        List<Trade> trades = calm(31);
        trades.add(t(100, at, "100", "10"));
        trades.add(t(101, at + 10_000_000, "101", "1"));
        trades.add(t(102, at + 25_000_000, "99.5", "1"));
        trades.add(t(103, at + 60_000_001, "100", "1"));
        trades.add(t(104, at + 200_000_000, "98", "1"));
        Whale whale = run(trades).get(0);
        assertThat(List.of(whale.getMove10s(), whale.getMove30s(), whale.getMove60s(),
                        whale.getMove120s()))
                .usingElementComparator(BigDecimal::compareTo)
                .containsExactly(new BigDecimal(100), new BigDecimal(-50), new BigDecimal(-50),
                        BigDecimal.ZERO);
    }
}
