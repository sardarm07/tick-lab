package ticklab.flink;

/**
 * One feed's trades of one second, in trade-ID order, with prices in whole cents (a Flink POJO).
 */
public class SecondOfTrades {
    public String base;
    /** Start of the second, in milliseconds since the epoch. */
    public long secondMillis;
    public long[] ids;
    public long[] timesMicros;
    public long[] priceCents;

    public SecondOfTrades() {}
}
