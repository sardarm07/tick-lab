package ticklab.flink;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Exact decimal arithmetic at the scale of the Avro schemas: decimal(28, 12). */
final class Decimals {
    static final int SCALE = 12;
    static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE);

    private Decimals() {}

    /**
     * The value at scale 12. Throws if that would need rounding: every price and quantity must be
     * exact, or the candles can't be proven equal to the exchange's.
     */
    static BigDecimal exact(BigDecimal value) {
        return value.setScale(SCALE, RoundingMode.UNNECESSARY);
    }

    /**
     * A sum of prices times quantities, at scale 12: exact when it fits, else rounded half-even.
     * Of the pairs here, only Kraken's USDT/USD needs more: prices to 5 places and quantities to
     * 8. Kraken doesn't publish quote volumes, so nothing is compared at the 13th place. Binance
     * does, and its quote volumes are reconciled exactly, so any rounding there would show.
     */
    static BigDecimal notional(BigDecimal value) {
        return value.setScale(SCALE, RoundingMode.HALF_EVEN);
    }

    /** A ratio such as VWAP can't be exact; it is rounded half-even to 12 decimal places. */
    static BigDecimal divide(BigDecimal numerator, BigDecimal denominator) {
        return numerator.divide(denominator, SCALE, RoundingMode.HALF_EVEN);
    }
}
