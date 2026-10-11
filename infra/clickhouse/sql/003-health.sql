-- Gaps in trade IDs and feed health events, from the core job and gap repair.
--
-- `gaps` keeps one row per (gap, status): DETECTED and CLOSED or EXPIRED from Flink, REPAIRED or
-- FAILED from gap repair. A gap's history stays queryable, for example how long repair took.

CREATE TABLE IF NOT EXISTS ticklab.gaps
(
    exchange         Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol           LowCardinality(String),
    first_missing_id Int64,
    last_missing_id  Int64,
    status           Enum8('DETECTED' = 1, 'REPAIRED' = 2, 'FAILED' = 3, 'CLOSED' = 4, 'EXPIRED' = 5),
    after_trade_time DateTime64(6, 'UTC'),
    trades_found     Int64,
    detail           String,
    updated_at       DateTime64(3, 'UTC'),
    inserted_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(after_trade_time)
ORDER BY (exchange, symbol, first_missing_id, status);

CREATE TABLE IF NOT EXISTS ticklab.gaps_queue
(
    exchange         Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol           String,
    first_missing_id Int64,
    last_missing_id  Int64,
    status           Enum8('DETECTED' = 1, 'REPAIRED' = 2, 'FAILED' = 3, 'CLOSED' = 4, 'EXPIRED' = 5),
    after_trade_time DateTime64(6, 'UTC'),
    trades_found     Int64,
    detail           String,
    updated_at       DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'gaps',
    kafka_group_name = 'clickhouse-gaps',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 2000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.gaps_mv TO ticklab.gaps AS
SELECT exchange, symbol, first_missing_id, last_missing_id, status, after_trade_time,
       trades_found, detail, updated_at
FROM ticklab.gaps_queue;

CREATE TABLE IF NOT EXISTS ticklab.feed_health
(
    exchange          Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol            LowCardinality(String),
    event_type        Enum8('SILENT' = 1, 'RECOVERED' = 2),
    event_id          Int64,
    last_trade_seen_at DateTime64(3, 'UTC'),
    threshold_seconds Int32,
    at                DateTime64(3, 'UTC'),
    inserted_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(at)
ORDER BY (exchange, symbol, event_type, event_id);

CREATE TABLE IF NOT EXISTS ticklab.feed_health_queue
(
    exchange           Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol             String,
    event_type         Enum8('SILENT' = 1, 'RECOVERED' = 2),
    event_id           Int64,
    last_trade_seen_at DateTime64(3, 'UTC'),
    threshold_seconds  Int32,
    at                 DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'feed-health',
    kafka_group_name = 'clickhouse-feed-health',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 2000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.feed_health_mv TO ticklab.feed_health AS
SELECT exchange, symbol, event_type, event_id, last_trade_seen_at, threshold_seconds, at
FROM ticklab.feed_health_queue;
