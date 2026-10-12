package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ticklab.v1.LeadLag;

/**
 * The lead-lag analysis on a mini cluster, with made-up trades chosen so that each rule of a
 * move shows: the close is the highest trade ID, not the last to arrive; the move is timed by
 * its first trade beyond the threshold; a reference more than 10 s old is too stale; Kraken's
 * USDT/USD is no market here.
 */
class AnalysesPipelineTest {
    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(1)
                    .build());

    static final LeadLagRules RULES = new LeadLagRules(
            Map.of("BTC", new BigDecimal("10"), "ETH", new BigDecimal("10")),
            Duration.ofSeconds(10),
            Duration.ofSeconds(2));

    static TradeEvent trade(String feed, long id, long millis, String price) {
        return SignalsPipelineTest.trade(
                feed.split(":")[0], feed.split(":")[1], id, millis, price, "1");
    }

    @Test
    void movesArePairedAcrossTheExchangesByTheirFirstTradeBeyondTheThreshold() throws Exception {
        List<TradeEvent> input = List.of(
                trade("BINANCE:BTC/USDT", 1, 100, "100.00"),
                trade("KRAKEN:BTC/USD", 10, 500, "100.00"),
                trade("BINANCE:BTC/USDT", 3, 1_200, "100.15"), // the close: +15 bps
                trade("BINANCE:BTC/USDT", 2, 1_100, "99.99"), // earlier ID, arrives later
                trade("KRAKEN:BTC/USD", 11, 1_600, "100.20"), // +20 bps
                trade("KRAKEN:USDT/USD", 1, 1_700, "0.99"),
                trade("KRAKEN:USDT/USD", 2, 2_700, "1.01"),
                trade("BINANCE:ETH/USDT", 1, 100, "2000"),
                trade("BINANCE:ETH/USDT", 2, 12_000, "2100"), // 12 s on: a stale reference
                trade("BINANCE:ETH/USDT", 3, 13_000, "2000")); // -476 bps, unpaired

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<TradeEvent> trades = env
                .fromData(input, TypeInformation.of(TradeEvent.class))
                .assignTimestampsAndWatermarks(
                        CorePipeline.watermarks(CorePipeline.WATERMARK_BOUND, Duration.ZERO));
        CloseableIterator<LeadLag> out = AnalysesPipeline.leadLag(trades, RULES).collectAsync();
        env.execute("analyses-pipeline-test");

        List<String> rows = new ArrayList<>();
        out.forEachRemaining(row -> rows.add(String.join(" ",
                row.getBase(), row.getDirection().toString(), row.getLeader().toString(),
                String.valueOf(TradeEvent.micros(row.getLeaderTime())),
                SignalsPipelineTest.plain(row.getLeaderBps()),
                String.valueOf(row.getLeadUs()))));
        // BTC: Binance's first trade beyond +10 bps is ID 3 at 1.2 s (ID 2 was 1 bp down), 0.4 s
        // before Kraken's. ETH: a stale reference at 12 s, then 2,100 to 2,000 at 13 s.
        assertThat(rows).containsExactlyInAnyOrder(
                "BTC UP BINANCE 1200000 15 400000",
                "ETH DOWN BINANCE 13000000 -476.190476190476 null");
    }
}
