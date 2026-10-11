package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ticklab.v1.Candle;

/**
 * The whole core pipeline on a real Flink mini cluster: three minutes of real Binance trades,
 * shuffled within the watermark bound and with 10% repeated, must give exactly the candles
 * Binance itself published for those minutes.
 */
class CorePipelineTest {
    private static final String FIXTURE = "/fixtures/binance-2026-10-10T0000-3m";
    private static final Map<String, String> SYMBOLS =
            Map.of("BTCUSDT", "BTC/USDT", "SOLUSDT", "SOL/USDT");

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(1)
                    .build());

    @Test
    void shuffledRepeatedTradesGiveBinancesOfficialCandles() throws Exception {
        List<TradeEvent> trades = loadTrades();
        Random random = new Random(42);
        List<TradeEvent> input = shuffleWithinBound(withRepeats(trades, 0.1, random), random);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<TradeEvent> stream = env
                .fromData(input, TypeInformation.of(TradeEvent.class))
                .assignTimestampsAndWatermarks(
                        CorePipeline.watermarks(CorePipeline.WATERMARK_BOUND, Duration.ZERO));
        CorePipeline.Streams out = CorePipeline.build(
                stream, CorePipeline.WATERMARK_BOUND, CorePipeline.LATENESS);
        CloseableIterator<Candle> candles = out.candles().collectAsync();
        CloseableIterator<TradeEvent> late = out.lateTrades().collectAsync();
        env.execute("core-pipeline-test");

        Map<String, Candle> latest = new HashMap<>(); // the last version of each candle wins
        candles.forEachRemaining(candle -> latest.merge(
                candle.getSymbol() + " " + candle.getMinute(),
                candle,
                (a, b) -> a.getTradeCount() >= b.getTradeCount() ? a : b));
        assertThat(late.hasNext()).as("no trade should be too late").isFalse();

        List<String[]> official = loadCsv(FIXTURE + ".klines.csv");
        assertThat(latest).hasSize(official.size());
        for (String[] k : official) {
            String key = SYMBOLS.get(k[0]) + " " + Instant.ofEpochMilli(Long.parseLong(k[1]));
            Candle ours = latest.get(key);
            assertThat(ours).as(key).isNotNull();
            assertThat(ours.getOpen()).as(key + " open").isEqualByComparingTo(k[2]);
            assertThat(ours.getHigh()).as(key + " high").isEqualByComparingTo(k[3]);
            assertThat(ours.getLow()).as(key + " low").isEqualByComparingTo(k[4]);
            assertThat(ours.getClose()).as(key + " close").isEqualByComparingTo(k[5]);
            assertThat(ours.getVolume()).as(key + " volume").isEqualByComparingTo(k[6]);
            assertThat(ours.getQuoteVolume()).as(key + " quote volume").isEqualByComparingTo(k[8]);
            assertThat(ours.getTradeCount()).as(key + " count").isEqualTo(Long.parseLong(k[9]));
            assertThat(ours.getTakerBuyVolume()).as(key + " taker buy").isEqualByComparingTo(k[10]);
            assertThat(ours.getTakerBuyQuoteVolume())
                    .as(key + " taker buy quote")
                    .isEqualByComparingTo(k[11]);
        }
    }

    /** Each trade arrives a random 0 to 2 s after it happened: disorder within the bound. */
    private static List<TradeEvent> shuffleWithinBound(List<TradeEvent> trades, Random random) {
        long bound = CorePipeline.WATERMARK_BOUND.toMillis();
        record Arrival(long at, int order, TradeEvent trade) {}
        List<Arrival> arrivals = new ArrayList<>();
        for (int i = 0; i < trades.size(); i++) {
            TradeEvent trade = trades.get(i);
            long delay = (long) (random.nextDouble() * (bound - 1));
            arrivals.add(new Arrival(trade.eventTimeMillis() + delay, i, trade));
        }
        arrivals.sort(Comparator.comparingLong(Arrival::at).thenComparingInt(Arrival::order));
        return arrivals.stream().map(Arrival::trade).toList();
    }

    private static List<TradeEvent> withRepeats(List<TradeEvent> trades, double share, Random random) {
        List<TradeEvent> all = new ArrayList<>(trades);
        for (TradeEvent trade : trades) {
            if (random.nextDouble() < share) {
                all.add(trade);
            }
        }
        return all;
    }

    /** Rows of the daily history files, with the market in front. */
    private static List<TradeEvent> loadTrades() throws IOException {
        List<TradeEvent> trades = new ArrayList<>();
        for (String[] row : loadCsv(FIXTURE + ".trades.csv")) {
            TradeEvent trade = new TradeEvent();
            trade.exchange = "BINANCE";
            trade.symbol = SYMBOLS.get(row[0]);
            trade.feed = trade.exchange + ":" + trade.symbol;
            trade.base = trade.symbol.split("/")[0];
            trade.quote = trade.symbol.split("/")[1];
            trade.tradeId = Long.parseLong(row[1]);
            trade.price = new BigDecimal(row[2]);
            trade.quantity = new BigDecimal(row[3]);
            trade.tradeTimeMicros = Long.parseLong(row[5]);
            trade.takerBuy = row[6].equals("False"); // isBuyerMaker
            trade.receivedTimeMicros = -1;
            trade.source = "HISTORY";
            trades.add(trade);
        }
        return trades;
    }

    private static List<String[]> loadCsv(String resource) throws IOException {
        try (InputStream in = CorePipelineTest.class.getResourceAsStream(resource)) {
            assertThat(in).as(resource).isNotNull();
            String text = new String(in.readAllBytes(), StandardCharsets.US_ASCII);
            List<String[]> rows = new ArrayList<>();
            for (String line : text.split("\n")) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    rows.add(line.split(","));
                }
            }
            return rows;
        }
    }
}
