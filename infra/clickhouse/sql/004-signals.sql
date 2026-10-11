-- The signals job's output: the spread between Binance and Kraken each second, and alerts.
--
-- `spread_1s` keeps one row per (base, second); a repeat after a restart replaces it. It grows
-- by about 260,000 rows a day, so rows are kept for 30 days.
-- `alerts` keeps one row per alert: the CLOSED record (update_seq 1) replaces the OPEN one.

CREATE TABLE IF NOT EXISTS ticklab.spread_1s
(
    base              LowCardinality(String),
    second            DateTime64(3, 'UTC'),
    binance_price     Decimal(28, 12),
    kraken_price      Decimal(28, 12),
    usdt_usd          Decimal(28, 12),
    kraken_price_usdt Decimal(28, 12),
    spread_bps        Decimal(28, 12),
    kraken_age_ms     Int64,
    rate_age_ms       Int64,
    published_at      DateTime64(3, 'UTC'),
    inserted_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(second)
ORDER BY (base, second)
TTL toDateTime(inserted_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS ticklab.spread_1s_queue
(
    base              String,
    second            DateTime64(3, 'UTC'),
    binance_price     Decimal(28, 12),
    kraken_price      Decimal(28, 12),
    usdt_usd          Decimal(28, 12),
    kraken_price_usdt Decimal(28, 12),
    spread_bps        Decimal(28, 12),
    kraken_age_ms     Int64,
    rate_age_ms       Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'spread-1s',
    kafka_group_name = 'clickhouse-spread-1s',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 2000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.spread_1s_mv TO ticklab.spread_1s AS
SELECT base, second, binance_price, kraken_price, usdt_usd, kraken_price_usdt, spread_bps,
       kraken_age_ms, rate_age_ms, assumeNotNull(_timestamp_ms) AS published_at
FROM ticklab.spread_1s_queue;

CREATE TABLE IF NOT EXISTS ticklab.alerts
(
    type        Enum8('DIVERGENCE' = 1, 'PRICE_MOVE' = 2, 'VOLUME_SPIKE' = 3),
    key         LowCardinality(String),
    opened_at   DateTime64(3, 'UTC'),
    status      Enum8('OPEN' = 1, 'CLOSED' = 2),
    update_seq  UInt64,
    at          DateTime64(3, 'UTC'),
    value       Float64,
    peak        Float64,
    explanation String,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(update_seq)
PARTITION BY toYYYYMM(opened_at)
ORDER BY (type, key, opened_at);

CREATE TABLE IF NOT EXISTS ticklab.alerts_queue
(
    type        Enum8('DIVERGENCE' = 1, 'PRICE_MOVE' = 2, 'VOLUME_SPIKE' = 3),
    key         String,
    opened_at   DateTime64(3, 'UTC'),
    status      Enum8('OPEN' = 1, 'CLOSED' = 2),
    update_seq  Int64,
    at          DateTime64(3, 'UTC'),
    value       Float64,
    peak        Float64,
    explanation String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'alerts',
    kafka_group_name = 'clickhouse-alerts',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 2000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.alerts_mv TO ticklab.alerts AS
SELECT type, key, opened_at, status, toUInt64(update_seq) AS update_seq, at, value, peak,
       explanation
FROM ticklab.alerts_queue;
