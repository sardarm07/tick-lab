package ticklab.flink;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import ticklab.v1.Trade;

/**
 * The core job: trades from Kafka, deduplicated per feed, into 1-minute candles. Candles go to
 * the {@code candles} topic, trades too late for their candle to {@code late-trades}.
 *
 * <p>Arguments: {@code --bootstrap host:port --registry url}. Checkpointing and restarts come
 * from the cluster's configuration (compose.yaml).
 */
public final class CoreJob {
    private CoreJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        String bootstrap = options.getOrDefault("bootstrap", "redpanda:9092");
        String registry = options.getOrDefault("registry", "http://redpanda:8081");
        Duration idleness = Duration.ofSeconds(Long.parseLong(
                options.getOrDefault("idleness-seconds", String.valueOf(CorePipeline.IDLENESS.toSeconds()))));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<TradeEvent> source = KafkaSource.<TradeEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("trades")
                .setGroupId("flink-core")
                // After a restart without state, continue where the group left off.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new TradeDeserializer(registry))
                .build();
        // Watermarks are generated per partition (per feed) inside the source.
        DataStream<TradeEvent> trades = env.fromSource(
                        source, CorePipeline.watermarks(CorePipeline.WATERMARK_BOUND, idleness), "trades")
                .uid("trades-source");

        CorePipeline.Streams streams = CorePipeline.build(
                trades, CorePipeline.WATERMARK_BOUND, CorePipeline.LATENESS);
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
        env.execute("ticklab-core");
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
