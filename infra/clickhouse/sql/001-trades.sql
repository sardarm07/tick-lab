-- Raw trades, fed from the `trades` topic.
--
-- Three objects work together:
--   trades_queue  a Kafka-engine table: reading from it consumes the topic;
--   trades_mv     a materialised view that moves every consumed block into
--   trades        the table that keeps them.
--
-- `trades` is a ReplacingMergeTree ordered by (exchange, symbol, trade_id). Rows with the
-- same key collapse into one when background merges combine parts, and a query with FINAL
-- collapses them at read time. Replays, retries and the duplicates from overlapping
-- connections therefore count once. Collapsing only happens within a partition, so the
-- partition key comes from the trade itself (its date): every copy of a trade lands in the
-- same partition, wherever and whenever it was inserted.
--
-- Every statement must be safe to repeat: the init service runs this file at each start.

CREATE DATABASE IF NOT EXISTS ticklab;

CREATE TABLE IF NOT EXISTS ticklab.stack
(
    name String COMMENT 'live or replay, written by the init service'
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS ticklab.trades
(
    exchange      Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol        LowCardinality(String),
    base          LowCardinality(String),
    quote         LowCardinality(String),
    trade_id      Int64,
    price         Decimal(28, 12),
    quantity      Decimal(28, 12),
    taker_side    Enum8('BUY' = 1, 'SELL' = 2),
    trade_time    DateTime64(6, 'UTC'),
    received_time Nullable(DateTime64(6, 'UTC')),
    source        Enum8('WS' = 1, 'WS_SNAPSHOT' = 2, 'REST' = 3, 'HISTORY' = 4),
    published_at  DateTime64(3, 'UTC') COMMENT 'Kafka timestamp: when the trade was published',
    inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (exchange, symbol, trade_id)
-- Counted from insertion rather than trade time, so replayed history isn't deleted on arrival.
TTL toDateTime(inserted_at) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS ticklab.trades_queue
(
    exchange      Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol        String,
    base          String,
    quote         String,
    trade_id      Int64,
    price         Decimal(28, 12),
    quantity      Decimal(28, 12),
    taker_side    Enum8('BUY' = 1, 'SELL' = 2),
    trade_time    DateTime64(6, 'UTC'),
    received_time Nullable(DateTime64(6, 'UTC')),
    source        Enum8('WS' = 1, 'WS_SNAPSHOT' = 2, 'REST' = 3, 'HISTORY' = 4)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'trades',
    kafka_group_name = 'clickhouse-trades',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 5000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.trades_mv TO ticklab.trades AS
SELECT
    exchange, symbol, base, quote, trade_id, price, quantity, taker_side,
    trade_time, received_time, source,
    assumeNotNull(_timestamp_ms) AS published_at
FROM ticklab.trades_queue;
