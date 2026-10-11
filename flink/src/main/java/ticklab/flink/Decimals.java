package ticklab.flink;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Exact decimal arithmetic at the scale of the Avro schemas: decimal(28, 12). */
final class Decimals {
    static final int SCALE = 12;
    static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE);

    private Decimals() {}

    /**
     * The value at scale 12. Throws if that would need rounding: every price, quantity and
     * product of the two must be exact, or the candles can't be proven equal to the exchange's.
     */
    static BigDecimal exact(BigDecimal value) {
        return value.setScale(SCALE, RoundingMode.UNNECESSARY);
    }

    /** A ratio such as VWAP can't be exact; it is rounded half-even to 12 decimal places. */
    static BigDecimal divide(BigDecimal numerator, BigDecimal denominator) {
        return numerator.divide(denominator, SCALE, RoundingMode.HALF_EVEN);
    }
}
