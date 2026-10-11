package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import ticklab.v1.FeedHealth;

/** Silence detection on processing time, with the test harness's clock under control. */
class FeedHealthMonitorTest {
    private static KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, FeedHealth> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(
                        new FeedHealthMonitor(Map.of("BINANCE:BTC/USDT", 30))),
                trade -> trade.feed,
                Types.STRING);
    }

    private static List<String> events(
            KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, FeedHealth> h) {
        return h.extractOutputValues().stream()
                .map(e -> e.getEventType() + "@" + e.getEventId())
                .toList();
    }

    @Test
    void aFeedIsSilentAfterItsThresholdAndRecoversWithItsNextTrade() throws Exception {
        try (var h = harness()) {
            h.open();
            h.setProcessingTime(1_000);
            h.processElement(DedupTest.trade(1, 0), 0);
            h.setProcessingTime(20_000);
            h.processElement(DedupTest.trade(2, 1), 1); // pushes the deadline to 50 s
            h.setProcessingTime(49_999);
            assertThat(events(h)).isEmpty();
            h.setProcessingTime(50_000);
            assertThat(events(h)).containsExactly("SILENT@50000");
            h.setProcessingTime(61_000);
            h.processElement(DedupTest.trade(3, 2), 2);
            assertThat(events(h)).containsExactly("SILENT@50000", "RECOVERED@61000");
        }
    }
}
