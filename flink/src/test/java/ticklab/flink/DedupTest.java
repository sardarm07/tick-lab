package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/** The deduplication operator under precise control of input, watermarks and timers. */
class DedupTest {
    private static final Duration BOUND = Duration.ofSeconds(2);
    private static final Duration LATENESS = Duration.ofMinutes(5);
    private static final long T0 = 1_791_504_000_000L; // 2026-10-09 00:00:00 UTC, in ms

    private static KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, TradeEvent> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new Dedup(BOUND, LATENESS)),
                trade -> trade.feed,
                Types.STRING);
    }

    static TradeEvent trade(long id, long timeMillis) {
        TradeEvent trade = new TradeEvent();
        trade.exchange = "BINANCE";
        trade.symbol = "BTC/USDT";
        trade.feed = "BINANCE:BTC/USDT";
        trade.tradeId = id;
        trade.price = new BigDecimal("83040.01");
        trade.quantity = new BigDecimal("0.001");
        trade.tradeTimeMicros = timeMillis * 1000;
        trade.receivedTimeMicros = -1;
        trade.source = "WS";
        return trade;
    }

    private static void send(
            KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, TradeEvent> h,
            long id,
            long timeMillis)
            throws Exception {
        h.processElement(trade(id, timeMillis), timeMillis);
    }

    private static List<Long> ids(
            KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, TradeEvent> h) {
        return h.extractOutputValues().stream().map(t -> t.tradeId).toList();
    }

    @Test
    void repeatsAreDroppedAndEverythingElsePassesOnce() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 100, T0);
            send(h, 101, T0 + 10);
            send(h, 101, T0 + 10); // repeat during the warm-up
            h.processWatermark(T0 + 5_000); // warm-up over
            send(h, 102, T0 + 20);
            send(h, 100, T0); // repeat after the warm-up
            send(h, 102, T0 + 20);
            assertThat(ids(h)).containsExactly(100L, 101L, 102L);
        }
    }

    @Test
    void tradesReorderedAtTheStartAreAllKept() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 105, T0 + 50);
            send(h, 103, T0 + 30);
            send(h, 104, T0 + 40);
            h.processWatermark(T0 + 5_000);
            send(h, 103, T0 + 30);
            send(h, 106, T0 + 60);
            assertThat(ids(h)).containsExactly(105L, 103L, 104L, 106L);
        }
    }

    @Test
    void tradesAheadOfAGapWaitUntilItFills() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1, T0);
            h.processWatermark(T0 + 5_000);
            send(h, 2, T0 + 10);
            send(h, 4, T0 + 30); // 3 is missing for now
            send(h, 4, T0 + 30); // a repeat of a trade ahead of the gap
            send(h, 3, T0 + 20); // the gap fills
            send(h, 3, T0 + 20);
            send(h, 5, T0 + 40);
            assertThat(ids(h)).containsExactly(1L, 2L, 4L, 3L, 5L);
        }
    }

    @Test
    void aGapOpenForLongerThanTheLatenessBudgetIsSkippedButLetThroughOnceIfItTurnsUp()
            throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1, T0);
            h.processWatermark(T0 + 5_000);
            send(h, 2, T0 + 10);
            send(h, 5, T0 + 50); // 3 and 4 are missing
            h.processWatermark(T0 + 50 + LATENESS.toMillis()); // the budget for 3 and 4 is gone
            send(h, 6, T0 + 60);
            send(h, 3, T0 + 30); // very late, but never seen: let through once
            send(h, 3, T0 + 30);
            send(h, 5, T0 + 50);
            assertThat(ids(h)).containsExactly(1L, 2L, 5L, 6L, 3L);
        }
    }

    private static List<String> gaps(
            KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, TradeEvent> h) {
        var records = h.getSideOutput(Dedup.GAPS);
        return records == null
                ? List.of()
                : records.stream()
                        .map(r -> r.getValue().status + " " + r.getValue().firstMissingId + ".."
                                + r.getValue().lastMissingId)
                        .toList();
    }

    @Test
    void reorderingWithinTheWatermarkBoundIsNotReportedAsAGap() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1, T0);
            h.processWatermark(T0 + 5_000);
            send(h, 3, T0 + 5_030); // 2 is missing for a moment
            send(h, 2, T0 + 5_020);
            h.processWatermark(T0 + 9_000);
            assertThat(gaps(h)).isEmpty();
        }
    }

    @Test
    void aGapIsDetectedOnceTheWatermarkPassesTheTradeAfterItAndClosedWhenFilled()
            throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1, T0);
            h.processWatermark(T0 + 5_000);
            send(h, 5, T0 + 6_000); // 2..4 missing
            h.processWatermark(T0 + 5_999);
            assertThat(gaps(h)).isEmpty(); // not yet past the trade after the gap
            h.processWatermark(T0 + 6_000);
            assertThat(gaps(h)).containsExactly("DETECTED 2..4");
            send(h, 3, T0 + 5_500); // repaired, in any order
            send(h, 2, T0 + 5_400);
            send(h, 4, T0 + 5_600);
            assertThat(gaps(h)).containsExactly("DETECTED 2..4", "CLOSED 2..4");
        }
    }

    @Test
    void aGapStillOpenAfterTheLatenessBudgetExpires() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1, T0);
            h.processWatermark(T0 + 5_000);
            send(h, 4, T0 + 6_000); // 2..3 missing
            h.processWatermark(T0 + 6_000 + LATENESS.toMillis());
            assertThat(gaps(h)).containsExactly("DETECTED 2..3", "EXPIRED 2..3");
        }
    }

    @Test
    void stateSurvivesARestore() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = harness()) {
            h.open();
            send(h, 1, T0);
            h.processWatermark(T0 + 5_000);
            send(h, 2, T0 + 10);
            send(h, 4, T0 + 30);
            snapshot = h.snapshot(1, T0 + 40);
        }
        try (var h = harness()) {
            h.initializeState(snapshot);
            h.open();
            send(h, 2, T0 + 10); // seen before the restore
            send(h, 4, T0 + 30); // waiting ahead of the gap before the restore
            send(h, 3, T0 + 20);
            send(h, 5, T0 + 40);
            assertThat(ids(h)).containsExactly(3L, 5L);
        }
    }
}
