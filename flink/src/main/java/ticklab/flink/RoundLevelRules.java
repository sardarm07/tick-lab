package ticklab.flink;

import java.time.Duration;
import java.util.Map;

/**
 * The rules of the round-level tests (decision note 0021), the same in
 * {@code ticklab/round_levels.py}.
 *
 * @param steps by base, then tier (MAJOR, MINOR): the distance between levels, in cents
 * @param bands by base: how far beyond a level the price must be to arm it, or to decide a test
 *     either way, in cents: the pair's median 60-second move over four weeks
 * @param seed draws the random control levels
 * @param decision how long after it began a test is decided
 * @param keepSteps levels further than this many steps from the price are forgotten
 */
public record RoundLevelRules(Map<String, Map<String, Long>> steps, Map<String, Long> bands,
        String seed, Duration decision, int keepSteps) implements java.io.Serializable {

    public static final RoundLevelRules ROUND_LEVELS = new RoundLevelRules(
            Map.of(
                    "BTC", Map.of("MAJOR", 100_000L, "MINOR", 10_000L),
                    "ETH", Map.of("MAJOR", 10_000L, "MINOR", 1_000L),
                    "SOL", Map.of("MAJOR", 1_000L, "MINOR", 100L)),
            Map.of("BTC", 1_650L, "ETH", 77L, "SOL", 5L),
            "ticklab-round-levels-1",
            Duration.ofSeconds(60),
            2);
}
