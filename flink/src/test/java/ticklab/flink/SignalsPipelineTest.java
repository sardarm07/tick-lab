package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ticklab.v1.Alert;
import ticklab.v1.Spread;

/**
 * The signals SQL on a mini cluster, with a handful of made-up trades chosen so that each rule
 * shows: the last price is the highest trade ID, not the last to arrive; a Kraken price is
 * converted with the USDT/USD rate as of its own time; Binance is compared with the newest
 * Kraken price as of each second; a stale Kraken price gives no spread.
 */
class SignalsPipelineTest {
    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(1)
                    .build());

    static TradeEvent trade(String exchange, String symbol, long id, long millis, String price,
            String quantity) {
        TradeEvent trade = new TradeEvent();
        trade.exchange = exchange;
        trade.symbol = symbol;
        trade.feed = exchange + ":" + symbol;
        trade.base = symbol.split("/")[0];
        trade.quote = symbol.split("/")[1];
        trade.tradeId = id;
        trade.price = new BigDecimal(price);
        trade.quantity = new BigDecimal(quantity);
        trade.tradeTimeMicros = millis * 1000;
        trade.receivedTimeMicros = -1;
        trade.source = "WS";
        return trade;
    }

    @Test
    void krakenIsConvertedAtItsOwnTimeAndComparedAsOfEachBinanceSecond() throws Exception {
        List<TradeEvent> input = List.of(
                trade("KRAKEN", "USDT/USD", 1, 500, "0.9990", "100"),
                trade("KRAKEN", "BTC/USD", 10, 700, "83000.0", "0.001"),
                trade("BINANCE", "BTC/USDT", 100, 900, "83050.00", "0.001"),
                trade("KRAKEN", "BTC/USD", 11, 1_100, "83010.0", "0.001"),
                trade("KRAKEN", "BTC/USD", 13, 1_200, "83020.0", "0.001"),
                trade("KRAKEN", "BTC/USD", 12, 1_150, "83015.0", "0.001"), // arrives after 13
                trade("BINANCE", "BTC/USDT", 101, 1_500, "83060.00", "0.001"),
                trade("KRAKEN", "USDT/USD", 2, 2_200, "0.9995", "100"),
                trade("BINANCE", "BTC/USDT", 102, 2_500, "83070.00", "0.001"),
                trade("BINANCE", "BTC/USDT", 103, 14_000, "83080.00", "0.001"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(env);
        DataStream<TradeEvent> trades = env
                .fromData(input, TypeInformation.of(TradeEvent.class))
                .assignTimestampsAndWatermarks(
                        CorePipeline.watermarks(CorePipeline.WATERMARK_BOUND, Duration.ZERO));
        SignalsPipeline.Streams out =
                SignalsPipeline.build(tables, trades, SignalsPipeline.KRAKEN_STALE_AFTER);
        CloseableIterator<Spread> spreads = out.spreads().collectAsync();
        CloseableIterator<MinuteBar> minutes = out.minuteBars().collectAsync();
        CloseableIterator<Alert> alerts = out.alerts().collectAsync();
        env.execute("signals-pipeline-test");

        List<String> rows = new ArrayList<>();
        spreads.forEachRemaining(s -> rows.add(String.join(" ",
                s.getBase(), String.valueOf(s.getSecond().toEpochMilli()),
                plain(s.getBinancePrice()), plain(s.getKrakenPrice()), plain(s.getUsdtUsd()),
                plain(s.getKrakenPriceUsdt()), plain(s.getSpreadBps()),
                String.valueOf(s.getKrakenAgeMs()), String.valueOf(s.getRateAgeMs()))));
        // Second 1: the last Kraken trade is ID 13 (83020.0), though ID 12 arrived after it.
        // Second 2: Kraken didn't trade, so its price from second 1 is compared, converted with
        // the rate as of second 1 (0.9990): the rate of 0.9995 from second 2 came after it.
        // Second 14: Kraken's price is 13 s old, too stale: no spread.
        assertThat(rows).containsExactly(
                expected(0, "83050", "83000", "0.999", 0, 0),
                expected(1000, "83060", "83020", "0.999", 0, 1000),
                expected(2000, "83070", "83020", "0.999", 1000, 2000));

        List<String> bars = new ArrayList<>();
        minutes.forEachRemaining(b -> bars.add(b.feed + " " + b.minuteMillis + " "
                + plain(b.close) + " " + plain(b.volume) + " " + b.trades));
        assertThat(bars).containsExactlyInAnyOrder(
                "BINANCE:BTC/USDT 0 83080 0.004 4",
                "KRAKEN:BTC/USD 0 83020 0.004 4");
        assertThat(alerts.hasNext()).isFalse();
    }

    /** The row the spread at {@code second} should be, by the definitions in spread.avsc. */
    static String expected(long second, String binance, String kraken, String rate,
            long krakenAge, long rateAge) {
        BigDecimal b = new BigDecimal(binance);
        BigDecimal converted = new BigDecimal(kraken).divide(new BigDecimal(rate), 12,
                RoundingMode.HALF_EVEN);
        BigDecimal bps = converted.subtract(b).multiply(new BigDecimal(10_000))
                .divide(b, 12, RoundingMode.HALF_EVEN);
        return String.join(" ", "BTC", String.valueOf(second), binance, kraken, rate,
                plain(converted), plain(bps), String.valueOf(krakenAge), String.valueOf(rateAge));
    }

    static String plain(BigDecimal value) {
        return value.stripTrailingZeros().toPlainString();
    }
}
