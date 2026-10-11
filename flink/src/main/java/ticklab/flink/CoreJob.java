package ticklab.flink;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import ticklab.v1.Gap;
import ticklab.v1.Trade;

/**
 * The core job: trades from Kafka, deduplicated per feed, into 1-minute candles. Candles go to
 * the {@code candles} topic, trades too late for their candle to {@code late-trades}.
 *
 * <p>Gaps in trade IDs go to {@code gaps} (gap repair fetches the missing trades), and feeds that
 * go quiet to {@code feed-health}.
 *
 * <p>Arguments: {@code --bootstrap host:port --registry url --partitions 0,1,2
 * --idleness-seconds 10 --silence feed=seconds,...}, where a feed is {@code exchange:symbol}.
 * Checkpointing and restarts come from the cluster's configuration (compose.yaml).
 */
public final class CoreJob {
    private CoreJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        String bootstrap = options.getOrDefault("bootstrap", "redpanda:9092");
        String registry = options.getOrDefault("registry", "http://redpanda:8081");
        Duration idleness = idleness(options);
        Map<String, Integer> silence = thresholds(options.getOrDefault("silence", ""));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Only the partitions of the feeds this stack carries (all of them by default).
        KafkaSource<TradeEvent> source = Sources.trades(bootstrap, registry,
                options.get("partitions"), "flink-core", OffsetResetStrategy.EARLIEST);
        // Watermarks are generated per partition (per feed) inside the source.
        DataStream<TradeEvent> trades = env.fromSource(
                        source,
                        CorePipeline.watermarks(CorePipeline.WATERMARK_BOUND, idleness),
                        "trades")
                .uid("trades-source");

        CorePipeline.Streams streams = CorePipeline.build(
                trades, CorePipeline.WATERMARK_BOUND, CorePipeline.LATENESS, silence);
        streams.candles()
                .sinkTo(Sinks.candles(bootstrap, registry))
                .name("candles-sink")
                .uid("candles-sink");
        streams.lateTrades()
                .map(TradeEvent::toAvro)
                .returns(Trade.class)
                .name("late-trades-to-avro")
                .uid("late-trades-to-avro")
                .sinkTo(Sinks.lateTrades(bootstrap, registry))
                .name("late-trades-sink")
                .uid("late-trades-sink");
        streams.gaps()
                .map(GapEvent::toAvro)
                .returns(Gap.class)
                .name("gaps-to-avro")
                .uid("gaps-to-avro")
                .sinkTo(Sinks.gaps(bootstrap, registry))
                .name("gaps-sink")
                .uid("gaps-sink");
        streams.health()
                .sinkTo(Sinks.health(bootstrap, registry))
                .name("feed-health-sink")
                .uid("feed-health-sink");
        env.execute("ticklab-core");
    }

    /** {@code --idleness-seconds}: 10 s by default (live data); 0 turns idleness off. */
    static Duration idleness(Map<String, String> options) {
        return Duration.ofSeconds(Long.parseLong(options.getOrDefault(
                "idleness-seconds", String.valueOf(CorePipeline.IDLENESS.toSeconds()))));
    }

    /** {@code BINANCE:BTC/USDT=30,BINANCE:ETH/USDT=75} into a map. */
    static Map<String, Integer> thresholds(String spec) {
        Map<String, Integer> result = new HashMap<>();
        for (String item : spec.split(",")) {
            if (!item.isBlank()) {
                String[] parts = item.split("=", 2);
                result.put(parts[0], Integer.parseInt(parts[1]));
            }
        }
        return result;
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("expected --name value pairs, got " + args[i]);
            }
            options.put(args[i].substring(2), args[i + 1]);
        }
        return options;
    }
}
