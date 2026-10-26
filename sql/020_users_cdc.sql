CREATE TABLE IF NOT EXISTS core.users
(
    user_id Int64,
    signup_date Date,
    city LowCardinality(String),
    device_type LowCardinality(String),
    _version UInt64,
    _is_deleted UInt8 DEFAULT 0,
    _topic String DEFAULT '',
    _partition UInt64 DEFAULT 0,
    _offset UInt64 DEFAULT 0,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY tuple()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS raw.kafka_users
(
    user_id Int64,
    signup_date Int32,
    city LowCardinality(String),
    device_type LowCardinality(String),
    __ts_ms UInt64,
    __source_ts_ms UInt64,
    __deleted String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'mysql.azki.users',
    kafka_group_name = 'azki-users',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream',
    kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.users_mv TO core.users AS
SELECT user_id, toDate(signup_date) AS signup_date, city, device_type,
    __ts_ms AS _version, toUInt8(__deleted = 'true') AS _is_deleted,
    _topic, _partition, _offset, now64(6) AS ingested_at
FROM raw.kafka_users
WHERE _error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.users_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_users
WHERE _error != '';

CREATE VIEW IF NOT EXISTS core.users_latest AS
SELECT user_id, signup_date, city, device_type, _version
FROM core.users FINAL
WHERE _is_deleted = 0;
