package ticklab.flink;

import java.math.BigDecimal;

/**
 * A price move of one feed (a Flink POJO): its second closed more than the base's threshold away
 * from its reference price (decision note 0020).
 */
public class Move {
    public String exchange;
    public String base;
    /** Start of the second, in milliseconds since the epoch. */
    public long secondMillis;
    /** Trade time of the second's first trade beyond the threshold, in microseconds. */
    public long timeMicros;
    public boolean up;
    /** The second's close against the reference, in bps. */
    public BigDecimal bps;

    public Move() {}

    Move(String exchange, String base, long secondMillis, long timeMicros, boolean up,
            BigDecimal bps) {
        this.exchange = exchange;
        this.base = base;
        this.secondMillis = secondMillis;
        this.timeMicros = timeMicros;
        this.up = up;
        this.bps = bps;
    }

    @Override
    public String toString() {
        return exchange + ":" + base + (up ? " up " : " down ") + bps + " at " + timeMicros;
    }
}
