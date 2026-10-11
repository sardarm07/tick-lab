package ticklab.flink;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.avro.specific.SpecificRecord;
import ticklab.v1.Alert;
import ticklab.v1.Candle;
import ticklab.v1.FeedHealth;
import ticklab.v1.Gap;
import ticklab.v1.Spread;
import ticklab.v1.Trade;

/**
 * Kafka sinks. At-least-once is enough: every output has a deterministic key, and storage keeps
 * one row per key (decision note 0003), so a repeat after a restart changes nothing.
 */
final class Sinks {
    private Sinks() {}

    static KafkaSink<Candle> candles(String bootstrap, String registry) {
        return sink(bootstrap, registry, "candles", Candle.class, new CandleKey());
    }

    static KafkaSink<Trade> lateTrades(String bootstrap, String registry) {
        return sink(bootstrap, registry, "late-trades", Trade.class, new TradeKey());
    }

    static KafkaSink<Gap> gaps(String bootstrap, String registry) {
        return sink(bootstrap, registry, "gaps", Gap.class, new GapKey());
    }

    static KafkaSink<FeedHealth> health(String bootstrap, String registry) {
        return sink(bootstrap, registry, "feed-health", FeedHealth.class, new HealthKey());
    }

    static KafkaSink<Spread> spreads(String bootstrap, String registry) {
        return sink(bootstrap, registry, "spread-1s", Spread.class, new SpreadKey());
    }

    static KafkaSink<Alert> alerts(String bootstrap, String registry) {
        return sink(bootstrap, registry, "alerts", Alert.class, new AlertKey());
    }

    private static <T extends SpecificRecord> KafkaSink<T> sink(
            String bootstrap,
            String registry,
            String topic,
            Class<T> type,
            SerializationSchema<T> key) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(key)
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema
                                .forSpecific(type, topic + "-value", registry))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /** {@code exchange:symbol:minute}, the candle's identity. */
    static final class CandleKey implements SerializationSchema<Candle> {
        @Override
        public byte[] serialize(Candle candle) {
            return (candle.getExchange() + ":" + candle.getSymbol() + ":" + candle.getMinute())
                    .getBytes(UTF_8);
        }
    }

    /** {@code exchange:symbol:first_missing_id}, the gap's identity. */
    static final class GapKey implements SerializationSchema<Gap> {
        @Override
        public byte[] serialize(Gap gap) {
            return (gap.getExchange() + ":" + gap.getSymbol() + ":" + gap.getFirstMissingId())
                    .getBytes(UTF_8);
        }
    }

    /** {@code exchange:symbol}. */
    static final class HealthKey implements SerializationSchema<FeedHealth> {
        @Override
        public byte[] serialize(FeedHealth event) {
            return (event.getExchange() + ":" + event.getSymbol()).getBytes(UTF_8);
        }
    }

    /** {@code base}: one partition key per asset. */
    static final class SpreadKey implements SerializationSchema<Spread> {
        @Override
        public byte[] serialize(Spread spread) {
            return spread.getBase().getBytes(UTF_8);
        }
    }

    /** {@code type:key}. */
    static final class AlertKey implements SerializationSchema<Alert> {
        @Override
        public byte[] serialize(Alert alert) {
            return (alert.getType() + ":" + alert.getKey()).getBytes(UTF_8);
        }
    }

    /** {@code exchange:symbol}, as on the trades topic. */
    static final class TradeKey implements SerializationSchema<Trade> {
        @Override
        public byte[] serialize(Trade trade) {
            return (trade.getExchange() + ":" + trade.getSymbol()).getBytes(UTF_8);
        }
    }
}
