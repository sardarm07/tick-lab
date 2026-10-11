package ticklab.flink;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ticklab.v1.Trade;

/**
 * Reads {@code trades} messages: Avro in the Confluent wire format, decoded with the writer's
 * schema fetched from the registry by the ID in each message. Keeps the Kafka timestamp, the
 * moment the trade was published, for latency measurements.
 */
public class TradeDeserializer implements KafkaRecordDeserializationSchema<TradeEvent> {
    private final String registryUrl;
    private transient DeserializationSchema<Trade> avro;

    public TradeDeserializer(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        avro = ConfluentRegistryAvroDeserializationSchema.forSpecific(Trade.class, registryUrl);
        avro.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TradeEvent> out)
            throws IOException {
        out.collect(TradeEvent.from(avro.deserialize(record.value()), record.timestamp()));
    }

    @Override
    public TypeInformation<TradeEvent> getProducedType() {
        return TypeInformation.of(TradeEvent.class);
    }
}
