package ticklab.flink;

import java.math.BigDecimal;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Builds a candle one trade at a time, in any order: open and close come from the lowest and
 * highest trade ID, not from arrival order, and every sum is exact. So a candle is the same
 * whether its trades arrive in order, shuffled, or partly late.
 */
public class CandleAggregate
        implements AggregateFunction<TradeEvent, CandleAccumulator, CandleAccumulator> {

    @Override
    public CandleAccumulator createAccumulator() {
        return new CandleAccumulator();
    }

    @Override
    public CandleAccumulator add(TradeEvent trade, CandleAccumulator candle) {
        BigDecimal notional = Decimals.exact(trade.price.multiply(trade.quantity));
        candle.count++;
        if (trade.tradeId < candle.firstTradeId) {
            candle.firstTradeId = trade.tradeId;
            candle.open = trade.price;
        }
        if (trade.tradeId > candle.lastTradeId) {
            candle.lastTradeId = trade.tradeId;
            candle.close = trade.price;
        }
        candle.high = candle.high == null ? trade.price : candle.high.max(trade.price);
        candle.low = candle.low == null ? trade.price : candle.low.min(trade.price);
        candle.volume = candle.volume.add(trade.quantity);
        candle.quoteVolume = candle.quoteVolume.add(notional);
        if (trade.takerBuy) {
            candle.takerBuyVolume = candle.takerBuyVolume.add(trade.quantity);
            candle.takerBuyQuoteVolume = candle.takerBuyQuoteVolume.add(notional);
        }
        return candle;
    }

    @Override
    public CandleAccumulator getResult(CandleAccumulator candle) {
        return candle;
    }

    @Override
    public CandleAccumulator merge(CandleAccumulator a, CandleAccumulator b) {
        if (b.count == 0) {
            return a;
        }
        if (a.count == 0) {
            return b;
        }
        CandleAccumulator merged = new CandleAccumulator();
        merged.count = a.count + b.count;
        boolean aFirst = a.firstTradeId <= b.firstTradeId;
        merged.firstTradeId = aFirst ? a.firstTradeId : b.firstTradeId;
        merged.open = aFirst ? a.open : b.open;
        boolean aLast = a.lastTradeId >= b.lastTradeId;
        merged.lastTradeId = aLast ? a.lastTradeId : b.lastTradeId;
        merged.close = aLast ? a.close : b.close;
        merged.high = a.high.max(b.high);
        merged.low = a.low.min(b.low);
        merged.volume = a.volume.add(b.volume);
        merged.quoteVolume = a.quoteVolume.add(b.quoteVolume);
        merged.takerBuyVolume = a.takerBuyVolume.add(b.takerBuyVolume);
        merged.takerBuyQuoteVolume = a.takerBuyQuoteVolume.add(b.takerBuyQuoteVolume);
        return merged;
    }
}
