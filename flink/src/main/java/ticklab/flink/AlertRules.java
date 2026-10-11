package ticklab.flink;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/**
 * Alert thresholds, chosen from a week of history (2026-10-04 to 10) so that each alert is rare
 * but does happen: about one or two a day per asset or feed. docs/data-study.md has the
 * distributions, decision note 0018 the reasoning.
 */
public final class AlertRules {
    private AlertRules() {}

    /**
     * Opens when |spread| stays above {@code openBps} for {@code hold}; closes when it stays
     * below {@code closeBps} for {@code hold}, or after {@code silence} without spreads. A pause
     * longer than {@code gap} between spreads breaks a run.
     */
    public record Divergence(
            BigDecimal openBps, BigDecimal closeBps, Duration hold, Duration gap, Duration silence)
            implements java.io.Serializable {}

    /**
     * A minute's log return against the root mean square of the previous {@code window}
     * one-minute returns (at least {@code minReturns} of them): opens at |z| of at least
     * {@code openZ}, closes below {@code closeZ}.
     */
    public record PriceMove(double openZ, double closeZ, int window, int minReturns)
            implements java.io.Serializable {}

    /**
     * A minute's volume against the average of the previous {@code window} minutes, minutes
     * without trades counting as zero: opens at a ratio of at least {@code openRatio}, closes
     * below {@code closeRatio}.
     */
    public record VolumeSpike(double openRatio, double closeRatio, int window)
            implements java.io.Serializable {}

    public static final Divergence DIVERGENCE = new Divergence(
            new BigDecimal("2"),
            new BigDecimal("1"),
            Duration.ofSeconds(30),
            Duration.ofSeconds(5),
            Duration.ofSeconds(60));

    public static final PriceMove PRICE_MOVE = new PriceMove(8.0, 4.0, 60, 30);

    /** By exchange: Kraken's thinner markets spike far more often than Binance's. */
    public static final Map<String, VolumeSpike> VOLUME_SPIKE = Map.of(
            "BINANCE", new VolumeSpike(20.0, 10.0, 60),
            "KRAKEN", new VolumeSpike(50.0, 25.0, 60));
}
