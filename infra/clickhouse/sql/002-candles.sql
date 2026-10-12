-- Candles from Flink, trades too late for them, and the reconciliation against the exchanges'
-- official candles.
--
-- `candles` keeps one row per (exchange, symbol, minute). A late trade makes Flink emit the
-- candle again with a higher trade_count; ReplacingMergeTree(trade_count) keeps the version
-- with the highest count, so the latest candle wins however many times each is delivered.

CREATE TABLE IF NOT EXISTS ticklab.candles
(
    exchange               Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol                 LowCardinality(String),
    minute                 DateTime64(3, 'UTC'),
    open                   Decimal(28, 12),
    high                   Decimal(28, 12),
    low                    Decimal(28, 12),
    close                  Decimal(28, 12),
    volume                 Decimal(28, 12),
    quote_volume           Decimal(28, 12),
    taker_buy_volume       Decimal(28, 12),
    taker_buy_quote_volume Decimal(28, 12),
    vwap                   Decimal(28, 12),
    trade_count            UInt64,
    first_trade_id         Int64,
    last_trade_id          Int64,
    emitted_at             DateTime64(3, 'UTC'),
    input_published_at     DateTime64(3, 'UTC') COMMENT 'Kafka timestamp of the newest trade in it',
    published_at           DateTime64(3, 'UTC') COMMENT 'Kafka timestamp, which Flink sets to the event time: the end of the minute',
    inserted_at            DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(trade_count)
PARTITION BY toYYYYMM(minute)
ORDER BY (exchange, symbol, minute);

CREATE TABLE IF NOT EXISTS ticklab.candles_queue
(
    exchange               Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol                 String,
    minute                 DateTime64(3, 'UTC'),
    open                   Decimal(28, 12),
    high                   Decimal(28, 12),
    low                    Decimal(28, 12),
    close                  Decimal(28, 12),
    volume                 Decimal(28, 12),
    quote_volume           Decimal(28, 12),
    taker_buy_volume       Decimal(28, 12),
    taker_buy_quote_volume Decimal(28, 12),
    vwap                   Decimal(28, 12),
    trade_count            Int64,
    first_trade_id         Int64,
    last_trade_id          Int64,
    emitted_at             DateTime64(3, 'UTC'),
    input_published_at     DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'candles',
    kafka_group_name = 'clickhouse-candles',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 2000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.candles_mv TO ticklab.candles AS
SELECT
    exchange, symbol, minute, open, high, low, close, volume, quote_volume,
    taker_buy_volume, taker_buy_quote_volume, vwap,
    toUInt64(trade_count) AS trade_count, first_trade_id, last_trade_id, emitted_at,
    input_published_at, assumeNotNull(_timestamp_ms) AS published_at
FROM ticklab.candles_queue;

-- Trades that reached Flink after their candle's lateness budget: kept, never dropped.
CREATE TABLE IF NOT EXISTS ticklab.late_trades
(
    exchange      Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol        LowCardinality(String),
    trade_id      Int64,
    price         Decimal(28, 12),
    quantity      Decimal(28, 12),
    taker_side    Enum8('BUY' = 1, 'SELL' = 2),
    trade_time    DateTime64(6, 'UTC'),
    received_time Nullable(DateTime64(6, 'UTC')),
    source        Enum8('WS' = 1, 'WS_SNAPSHOT' = 2, 'REST' = 3, 'HISTORY' = 4),
    published_at  DateTime64(3, 'UTC') COMMENT 'Kafka timestamp, which Flink sets to the event time: the trade time',
    inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_time)
ORDER BY (exchange, symbol, trade_id);

CREATE TABLE IF NOT EXISTS ticklab.late_trades_queue
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
    kafka_topic_list = 'late-trades',
    kafka_group_name = 'clickhouse-late-trades',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.late_trades_mv TO ticklab.late_trades AS
SELECT
    exchange, symbol, trade_id, price, quantity, taker_side, trade_time, received_time, source,
    assumeNotNull(_timestamp_ms) AS published_at
FROM ticklab.late_trades_queue;

-- The exchanges' own candles, as fetched by the reconciler. Fields an exchange doesn't publish
-- are NULL (Binance has no VWAP; Kraken has no quote volume).
CREATE TABLE IF NOT EXISTS ticklab.official_candles
(
    exchange               Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol                 LowCardinality(String),
    minute                 DateTime64(3, 'UTC'),
    open                   Decimal(28, 12),
    high                   Decimal(28, 12),
    low                    Decimal(28, 12),
    close                  Decimal(28, 12),
    volume                 Decimal(28, 12),
    quote_volume           Nullable(Decimal(28, 12)),
    taker_buy_volume       Nullable(Decimal(28, 12)),
    taker_buy_quote_volume Nullable(Decimal(28, 12)),
    vwap                   Nullable(Decimal(28, 12)),
    trade_count            UInt64,
    fetched_at             DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(minute)
ORDER BY (exchange, symbol, minute);

-- One row per checked minute; a re-check replaces the first result.
CREATE TABLE IF NOT EXISTS ticklab.reconciliation
(
    exchange    Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    symbol      LowCardinality(String),
    minute      DateTime64(3, 'UTC'),
    checked_at  DateTime64(3, 'UTC'),
    status      Enum8('MATCH' = 1, 'MISMATCH' = 2),
    cause       LowCardinality(String) COMMENT 'empty for a match',
    differences String COMMENT 'field: ours / theirs, for each field that differs',
    attempt     UInt8 COMMENT '1 when the minute settles, 2 for the re-check'
)
ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(minute)
ORDER BY (exchange, symbol, minute);
