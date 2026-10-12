package ticklab.flink;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * The analyses job: the same trades again, under its own consumer group and deduplicated the
 * same way, into the analyses of milestones 6 to 8. So far: who moves first
 * ({@code analysis-lead-lag}).
 *
 * <p>Arguments: {@code --bootstrap host:port --registry url --partitions 0,1,2
 * --idleness-seconds 10 --start-at latest|earliest}.
 */
public final class AnalysesJob {
    private AnalysesJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = CoreJob.parse(args);
        String bootstrap = options.getOrDefault("bootstrap", "redpanda:9092");
        String registry = options.getOrDefault("registry", "http://redpanda:8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // As for the signals: live, the first start is at the newest trade; replays and tests
        // read their empty topic from the start.
        OffsetResetStrategy firstStart = "latest".equals(options.get("start-at"))
                ? OffsetResetStrategy.LATEST
                : OffsetResetStrategy.EARLIEST;
        KafkaSource<TradeEvent> source = Sources.trades(
                bootstrap, registry, options.get("partitions"), "flink-analyses", firstStart);
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

        AnalysesPipeline.leadLag(unique, LeadLagRules.LEAD_LAG)
                .sinkTo(Sinks.leadLag(bootstrap, registry))
                .name("lead-lag-sink")
                .uid("lead-lag-sink");
        env.execute("ticklab-analyses");
    }
}
