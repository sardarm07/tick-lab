package ticklab.flink;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

/** The {@code trades} topic as a source, for every job. */
final class Sources {
    private Sources() {}

    /**
     * @param partitions only these partitions ({@code 0,1,2}), or every one when null: an empty
     *     partition would hold the watermark back for good when idleness is off (replays)
     * @param groupId each job reads the topic in full, under its own consumer group
     * @param firstStart where to begin when the group has no committed offsets yet
     */
    static KafkaSource<TradeEvent> trades(String bootstrap, String registry, String partitions,
            String groupId, OffsetResetStrategy firstStart) {
        KafkaSourceBuilder<TradeEvent> builder = KafkaSource.<TradeEvent>builder();
        if (partitions != null) {
            Set<TopicPartition> chosen = new HashSet<>();
            for (String partition : partitions.split(",")) {
                chosen.add(new TopicPartition("trades", Integer.parseInt(partition.trim())));
            }
            builder.setPartitions(chosen);
        } else {
            builder.setTopics("trades");
        }
        return builder
                .setBootstrapServers(bootstrap)
                .setGroupId(groupId)
                // After a restart without state, continue where the group left off.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(firstStart))
                .setDeserializer(new TradeDeserializer(registry))
                .build();
    }
}
