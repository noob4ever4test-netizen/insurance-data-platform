CREATE TABLE IF NOT EXISTS core.user_events
(
    event_time DateTime('UTC'),
    user_id Int64,
    session_id String,
    event_type LowCardinality(String),
    channel LowCardinality(String),
    premium_amount Int64,
    event_key UInt64 MATERIALIZED cityHash64(user_id, session_id, event_time, event_type),
    _topic String DEFAULT '',
    _partition UInt64 DEFAULT 0,
    _offset UInt64 DEFAULT 0,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, user_id, event_time, session_id);

CREATE TABLE IF NOT EXISTS dq.kafka_dead_letter
(
    topic String,
    partition UInt64,
    offset UInt64,
    raw_message String,
    error String,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS raw.kafka_user_events
(
    event_time DateTime('UTC'),
    user_id Int64,
    session_id String,
    event_type LowCardinality(String),
    channel LowCardinality(String),
    premium_amount Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'azki.user_events',
    kafka_group_name = 'azki-events',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream',
    kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1,
    input_format_defaults_for_omitted_fields = 0,
    date_time_input_format = 'best_effort';
