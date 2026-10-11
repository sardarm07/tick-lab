package ticklab.flink;

import java.math.BigDecimal;

/** One minute of one feed, for the price and volume alerts: its last price and its volume. */
public class MinuteBar {
    /** {@code exchange:symbol}. */
    public String feed;

    /** Start of the minute, in milliseconds since the epoch. */
    public long minuteMillis;

    /** Price of the trade with the highest ID. */
    public BigDecimal close;

    /** Sum of quantities, in the base asset. */
    public BigDecimal volume;

    public long trades;

    public MinuteBar() {}

    MinuteBar(String feed, long minuteMillis, BigDecimal close, BigDecimal volume, long trades) {
        this.feed = feed;
        this.minuteMillis = minuteMillis;
        this.close = close;
        this.volume = volume;
        this.trades = trades;
    }

    @Override
    public String toString() {
        return feed + "@" + minuteMillis + " close " + close + " volume " + volume;
    }
}
