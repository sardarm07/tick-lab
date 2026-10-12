package ticklab.flink;

/**
 * One feed's trades of one second, in trade-ID order (a Flink POJO): prices in whole cents,
 * quantities in units of 1e-8.
 */
public class SecondOfTrades {
    public String base;
    /** Start of the second, in milliseconds since the epoch. */
    public long secondMillis;
    public long[] ids;
    public long[] timesMicros;
    public long[] priceCents;
    public long[] quantityE8;
    public boolean[] takerBuy;

    public SecondOfTrades() {}
}
