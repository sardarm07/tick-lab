package ticklab.flink;

import java.math.BigDecimal;

/** The running state of one 1-minute candle (a Flink POJO). */
public class CandleAccumulator {
    public long count;
    public long firstTradeId = Long.MAX_VALUE;
    public BigDecimal open;
    public long lastTradeId = Long.MIN_VALUE;
    public BigDecimal close;
    public BigDecimal high;
    public BigDecimal low;
    public BigDecimal volume = Decimals.ZERO;
    public BigDecimal quoteVolume = Decimals.ZERO;
    public BigDecimal takerBuyVolume = Decimals.ZERO;
    public BigDecimal takerBuyQuoteVolume = Decimals.ZERO;

    public CandleAccumulator() {}
}
