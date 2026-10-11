package ticklab.flink;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import ticklab.v1.Alert;
import ticklab.v1.Spread;

/**
 * The signals pipeline, separate from sources and sinks so tests can run it on any input.
 *
 * <p>Deduplicated trades become a table, and Flink SQL does the windowing and joining:
 *
 * <ol>
 *   <li>1-second bars per feed: the last price (by trade ID), volume and trade count.
 *   <li>Kraken's USDT/USD bars as a <em>versioned table</em>: for any moment, the rate that was
 *       current then.
 *   <li>Each Kraken bar joined with the rate as of its own time (an event-time temporal join):
 *       a price in USD becomes a price in USDT.
 *   <li>The converted Kraken prices as a second versioned table, and each Binance bar joined
 *       with the Kraken price as of its time: the two prices to compare.
 *   <li>1-minute bars, cascaded from the 1-second ones, for the price and volume alerts.
 * </ol>
 *
 * <p>The SQL results become DataStreams again for the arithmetic (exact decimals) and for the
 * alerts, which need keyed state and timers.
 */
public final class SignalsPipeline {
    /** A Kraken price older than this at the end of a second is too stale to compare. */
    public static final Duration KRAKEN_STALE_AFTER = Duration.ofSeconds(10);

    private static final BigDecimal BPS = new BigDecimal(10_000);

    static final String BARS_1S = """
            SELECT `exchange`, `symbol`, `base`, `quote`,
                   window_start AS bar_start, window_time AS bar_time,
                   LAST_BY_ID(price, tradeId) AS price, MAX(tradeId) AS last_id,
                   SUM(quantity) AS volume, COUNT(*) AS trade_count
            FROM TABLE(TUMBLE(TABLE trades, DESCRIPTOR(rowtime), INTERVAL '1' SECOND))
            GROUP BY `exchange`, `symbol`, `base`, `quote`, window_start, window_end, window_time
            """;

    /** Versioned: deduplicating on the newest time per key makes Flink keep every version. */
    static final String USD_PER_USDT = """
            SELECT `quote`, price AS rate, bar_time
            FROM (
                SELECT `quote`, price, bar_time,
                       ROW_NUMBER() OVER (PARTITION BY `quote` ORDER BY bar_time DESC) AS row_num
                FROM bars_1s
                WHERE `exchange` = 'KRAKEN' AND `base` = 'USDT')
            WHERE row_num = 1
            """;

    /**
     * The rate's time is cast to a plain timestamp: a joined row may carry only one event-time
     * attribute, and it is the Kraken bar's.
     */
    static final String KRAKEN_IN_USDT = """
            SELECT k.`base`, k.bar_time, k.price AS kraken_price, r.rate AS usdt_usd,
                   CAST(r.bar_time AS TIMESTAMP(3)) AS rate_time
            FROM bars_1s AS k
            JOIN usd_per_usdt FOR SYSTEM_TIME AS OF k.bar_time AS r ON k.`quote` = r.`quote`
            WHERE k.`exchange` = 'KRAKEN' AND k.`base` <> 'USDT'
            """;

    static final String KRAKEN_LATEST = """
            SELECT `base`, kraken_price, usdt_usd, rate_time, bar_time
            FROM (
                SELECT `base`, kraken_price, usdt_usd, rate_time, bar_time,
                       ROW_NUMBER() OVER (PARTITION BY `base` ORDER BY bar_time DESC) AS row_num
                FROM kraken_in_usdt)
            WHERE row_num = 1
            """;

    static final String PAIRS = """
            SELECT b.`base`, b.bar_start, b.bar_time, b.price AS binance_price,
                   l.kraken_price, l.usdt_usd, CAST(l.bar_time AS TIMESTAMP(3)) AS kraken_time,
                   l.rate_time
            FROM bars_1s AS b
            JOIN kraken_latest FOR SYSTEM_TIME AS OF b.bar_time AS l ON b.`base` = l.`base`
            WHERE b.`exchange` = 'BINANCE'
            """;

    /** With its window_time, so the rows keep their event time as a DataStream. */
    static final String BARS_1M = """
            SELECT `exchange`, `symbol`, window_start AS minute_start, window_time AS bar_time,
                   LAST_BY_ID(price, last_id) AS close_price, SUM(volume) AS volume,
                   SUM(trade_count) AS trade_count
            FROM TABLE(TUMBLE(TABLE bars_1s, DESCRIPTOR(bar_time), INTERVAL '1' MINUTE))
            WHERE `base` <> 'USDT'
            GROUP BY `exchange`, `symbol`, window_start, window_end, window_time
            """;

    private SignalsPipeline() {}

    public record Streams(
            DataStream<Spread> spreads, DataStream<MinuteBar> minuteBars, DataStream<Alert> alerts) {}

    public static Streams build(
            StreamTableEnvironment tables, DataStream<TradeEvent> unique, Duration staleAfter) {
        // Window starts and plain timestamps are wall-clock times in the session's time zone.
        tables.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        tables.createTemporarySystemFunction("LAST_BY_ID", LastPriceById.class);
        // The trades keep the DataStream's timestamps and watermarks as their event time.
        tables.createTemporaryView("trades", tables.fromDataStream(unique, Schema.newBuilder()
                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build()));
        tables.createTemporaryView("bars_1s", tables.sqlQuery(BARS_1S));
        tables.createTemporaryView("usd_per_usdt", tables.sqlQuery(USD_PER_USDT));
        tables.createTemporaryView("kraken_in_usdt", tables.sqlQuery(KRAKEN_IN_USDT));
        tables.createTemporaryView("kraken_latest", tables.sqlQuery(KRAKEN_LATEST));

        DataStream<Spread> spreads = tables.toDataStream(tables.sqlQuery(PAIRS))
                .flatMap(new ToSpread(staleAfter.toMillis()))
                .returns(TypeInformation.of(Spread.class))
                .name("spreads")
                .uid("spreads");
        DataStream<MinuteBar> minuteBars = tables.toDataStream(tables.sqlQuery(BARS_1M))
                .map(SignalsPipeline::minuteBar)
                .returns(MinuteBar.class)
                .name("minute-bars")
                .uid("minute-bars");

        DataStream<Alert> divergence = spreads
                .keyBy(Spread::getBase, Types.STRING)
                .process(new DivergenceAlerts(AlertRules.DIVERGENCE))
                .name("divergence-alerts")
                .uid("divergence-alerts");
        DataStream<Alert> moves = minuteBars
                .keyBy(bar -> bar.feed, Types.STRING)
                .process(new PriceMoveAlerts(AlertRules.PRICE_MOVE))
                .name("price-move-alerts")
                .uid("price-move-alerts");
        DataStream<Alert> spikes = minuteBars
                .keyBy(bar -> bar.feed, Types.STRING)
                .process(new VolumeSpikeAlerts(AlertRules.VOLUME_SPIKE))
                .name("volume-spike-alerts")
                .uid("volume-spike-alerts");
        return new Streams(spreads, minuteBars, divergence.union(moves, spikes));
    }

    static MinuteBar minuteBar(Row row) {
        return new MinuteBar(
                row.getFieldAs("exchange") + ":" + row.getFieldAs("symbol"),
                utc(row, "minute_start").toEpochMilli(),
                row.getFieldAs("close_price"),
                row.getFieldAs("volume"),
                row.<Long>getFieldAs("trade_count"));
    }

    /** A plain timestamp column (a window start, or a time cast to TIMESTAMP), in UTC. */
    static Instant utc(Row row, String column) {
        return row.<LocalDateTime>getFieldAs(column).toInstant(ZoneOffset.UTC);
    }

    /** The prices of one second, compared; nothing when Kraken's price is stale. */
    static final class ToSpread implements FlatMapFunction<Row, Spread> {
        private final long staleAfterMillis;

        ToSpread(long staleAfterMillis) {
            this.staleAfterMillis = staleAfterMillis;
        }

        @Override
        public void flatMap(Row row, Collector<Spread> out) {
            Instant end = row.getFieldAs("bar_time");
            Instant krakenTime = utc(row, "kraken_time");
            Instant rateTime = utc(row, "rate_time");
            long krakenAge = end.toEpochMilli() - krakenTime.toEpochMilli();
            if (krakenAge > staleAfterMillis) {
                return;
            }
            BigDecimal binance = row.getFieldAs("binance_price");
            BigDecimal kraken = row.getFieldAs("kraken_price");
            BigDecimal rate = row.getFieldAs("usdt_usd");
            BigDecimal converted = Decimals.divide(kraken, rate);
            BigDecimal bps = Decimals.divide(converted.subtract(binance).multiply(BPS), binance);
            out.collect(Spread.newBuilder()
                    .setBase(row.getFieldAs("base"))
                    .setSecond(utc(row, "bar_start"))
                    .setBinancePrice(Decimals.exact(binance))
                    .setKrakenPrice(Decimals.exact(kraken))
                    .setUsdtUsd(Decimals.exact(rate))
                    .setKrakenPriceUsdt(converted)
                    .setSpreadBps(bps)
                    .setKrakenAgeMs(krakenAge)
                    .setRateAgeMs(end.toEpochMilli() - rateTime.toEpochMilli())
                    .build());
        }
    }
}
