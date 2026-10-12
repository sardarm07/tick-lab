-- The analyses job's output (milestones 6 to 8).
--
-- `lead_lag` keeps one row per leading move: a move on one exchange paired with the same move on
-- the other, or a move the other exchange didn't follow (no follower). The leader's move is its
-- identity, so a repeat after a restart replaces the row. Times are on each exchange's own
-- clock (decision note 0020).

CREATE TABLE IF NOT EXISTS ticklab.lead_lag
(
    base          LowCardinality(String),
    direction     Enum8('UP' = 1, 'DOWN' = 2),
    leader        Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    leader_time   DateTime64(6, 'UTC'),
    leader_bps    Decimal(28, 12),
    follower_time Nullable(DateTime64(6, 'UTC')),
    follower_bps  Nullable(Decimal(28, 12)),
    lead_us       Nullable(Int64) COMMENT 'follower_time - leader_time; 0 is a tie',
    threshold_bps Decimal(28, 12),
    inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(leader_time)
ORDER BY (base, leader, leader_time);

CREATE TABLE IF NOT EXISTS ticklab.lead_lag_queue
(
    base          String,
    direction     Enum8('UP' = 1, 'DOWN' = 2),
    leader        Enum8('BINANCE' = 1, 'KRAKEN' = 2),
    leader_time   DateTime64(6, 'UTC'),
    leader_bps    Decimal(28, 12),
    follower_time Nullable(DateTime64(6, 'UTC')),
    follower_bps  Nullable(Decimal(28, 12)),
    lead_us       Nullable(Int64),
    threshold_bps Decimal(28, 12)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'analysis-lead-lag',
    kafka_group_name = 'clickhouse-lead-lag',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 2000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticklab.lead_lag_mv TO ticklab.lead_lag AS
SELECT base, direction, leader, leader_time, leader_bps, follower_time, follower_bps, lead_us,
       threshold_bps
FROM ticklab.lead_lag_queue;
