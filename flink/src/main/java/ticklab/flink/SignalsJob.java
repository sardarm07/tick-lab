package ticklab.flink;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * The signals job: the same trades as the core job, read again under its own consumer group and
 * deduplicated the same way, into the per-second spread between Binance and Kraken
 * ({@code spread-1s}) and alerts ({@code alerts}).
 *
 * <p>Arguments: {@code --bootstrap host:port --registry url --partitions 0,1,2
 * --idleness-seconds 10 --start-at latest|earliest}.
 */
public final class SignalsJob {
    private SignalsJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = CoreJob.parse(args);
        String bootstrap = options.getOrDefault("bootstrap", "redpanda:9092");
        String registry = options.getOrDefault("registry", "http://redpanda:8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tables = StreamTableEnvironment.create(env);
        // The first time, live: start at the newest trade (signals are about now); replays and
        // tests: from the start of their empty topic.
        OffsetResetStrategy firstStart = "latest".equals(options.get("start-at"))
                ? OffsetResetStrategy.LATEST
                : OffsetResetStrategy.EARLIEST;
        KafkaSource<TradeEvent> source = Sources.trades(
                bootstrap, registry, options.get("partitions"), "flink-signals", firstStart);
        DataStream<TradeEvent> unique = env.fromSource(
                        source,
                        CorePipeline.watermarks(
                                CorePipeline.WATERMARK_BOUND, CoreJob.idleness(options)),
                        "trades")
                .uid("trades-source")
                .keyBy(trade -> trade.feed, Types.STRING)
                .process(new Dedup(CorePipeline.WATERMARK_BOUND, CorePipeline.LATENESS))
                .name("dedup")
                .uid("dedup");

        SignalsPipeline.Streams streams =
                SignalsPipeline.build(tables, unique, SignalsPipeline.KRAKEN_STALE_AFTER);
        streams.spreads()
                .sinkTo(Sinks.spreads(bootstrap, registry))
                .name("spread-sink")
                .uid("spread-sink");
        streams.alerts()
                .sinkTo(Sinks.alerts(bootstrap, registry))
                .name("alerts-sink")
                .uid("alerts-sink");
        env.execute("ticklab-signals");
    }
}
