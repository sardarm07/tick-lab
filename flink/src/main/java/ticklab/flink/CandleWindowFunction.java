package ticklab.flink;

import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ticklab.v1.Candle;
import ticklab.v1.Exchange;

/**
 * Turns a finished (or, after a late trade, updated) candle into the Avro record. Each version
 * of a candle carries its trade count, which only grows, so storage keeps the latest.
 */
public class CandleWindowFunction
        extends ProcessWindowFunction<CandleAccumulator, Candle, String, TimeWindow> {

    @Override
    public void process(
            String feed,
            Context context,
            Iterable<CandleAccumulator> candles,
            Collector<Candle> out) {
        CandleAccumulator candle = candles.iterator().next();
        String[] parts = feed.split(":", 2);
        out.collect(Candle.newBuilder()
                .setExchange(Exchange.valueOf(parts[0]))
                .setSymbol(parts[1])
                .setMinute(Instant.ofEpochMilli(context.window().getStart()))
                .setOpen(Decimals.exact(candle.open))
                .setHigh(Decimals.exact(candle.high))
                .setLow(Decimals.exact(candle.low))
                .setClose(Decimals.exact(candle.close))
                .setVolume(Decimals.exact(candle.volume))
                .setQuoteVolume(Decimals.exact(candle.quoteVolume))
                .setTakerBuyVolume(Decimals.exact(candle.takerBuyVolume))
                .setTakerBuyQuoteVolume(Decimals.exact(candle.takerBuyQuoteVolume))
                .setVwap(Decimals.divide(candle.quoteVolume, candle.volume))
                .setTradeCount(candle.count)
                .setFirstTradeId(candle.firstTradeId)
                .setLastTradeId(candle.lastTradeId)
                .setEmittedAt(Instant.ofEpochMilli(context.currentProcessingTime()))
                .build());
    }
}
