package ticklab.flink;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/**
 * The rules of the lead-lag analysis (decision note 0020): thresholds chosen from a week of
 * history (docs/data-study.md), the same in {@code ticklab/lead_lag.py}.
 *
 * @param thresholds by base: a second whose close is more than this many bps from its reference
 *     is a move
 * @param staleAfter a reference from a second that began longer ago than this is too stale to
 *     compare with
 * @param window moves of the two exchanges at most this far apart can pair
 */
public record LeadLagRules(Map<String, BigDecimal> thresholds, Duration staleAfter,
        Duration window) implements java.io.Serializable {

    /**
     * Binance's 99.9th percentile of 1-second moves in the week of 2026-10-04 (4.1, 6.6 and 8.1
     * bps), rounded up to a whole basis point.
     */
    public static final LeadLagRules LEAD_LAG = new LeadLagRules(
            Map.of("BTC", new BigDecimal("5"), "ETH", new BigDecimal("7"),
                    "SOL", new BigDecimal("9")),
            Duration.ofSeconds(10),
            Duration.ofSeconds(2));
}
