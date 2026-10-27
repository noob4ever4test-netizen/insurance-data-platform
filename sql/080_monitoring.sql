CREATE TABLE IF NOT EXISTS dq.check_results
(
    name LowCardinality(String),
    status LowCardinality(String),
    observed String,
    threshold String,
    details String,
    checked_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(checked_at)
ORDER BY (name, checked_at);

CREATE TABLE IF NOT EXISTS dq.event_samples
(
    topic String,
    partition UInt64,
    offset UInt64,
    raw_message String,
    captured_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(captured_at)
PARTITION BY toYYYYMM(captured_at)
ORDER BY (topic, partition, offset)
TTL toDateTime(captured_at) + INTERVAL 1 DAY;

CREATE TABLE IF NOT EXISTS raw.kafka_event_samples (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'azki.user_events',
    kafka_group_name = 'azki-dq-samples', kafka_format = 'JSONAsString',
    kafka_handle_error_mode = 'stream', kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.event_samples_mv TO dq.event_samples AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    if(_error = '', payload, _raw_message) AS raw_message, now64(6) AS captured_at
FROM raw.kafka_event_samples;

CREATE TABLE IF NOT EXISTS dq.cdc_heartbeats
(
    source LowCardinality(String),
    ts_ms UInt64,
    captured_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(captured_at)
ORDER BY (source, ts_ms)
TTL toDateTime(captured_at) + INTERVAL 1 DAY;

CREATE TABLE IF NOT EXISTS raw.kafka_cdc_heartbeats (ts_ms UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = '__debezium-heartbeat.mysql',
    kafka_group_name = 'azki-dq-heartbeats', kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream', kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.cdc_heartbeats_mv TO dq.cdc_heartbeats AS
SELECT 'mysql' AS source, ts_ms, now64(6) AS captured_at
FROM raw.kafka_cdc_heartbeats WHERE _error = '' AND ts_ms > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.heartbeat_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_cdc_heartbeats WHERE _error != '';

CREATE VIEW IF NOT EXISTS dq.freshness_metrics AS
SELECT minOrNull(event_time) AS first_event_at, maxOrNull(event_time) AS last_event_at,
    maxOrNull(ingested_at) AS last_ingested_at
FROM core.user_events FINAL;

CREATE VIEW IF NOT EXISTS dq.cdc_metrics AS
SELECT maxOrNull(fromUnixTimestamp64Milli(toInt64(ts_ms), 'UTC')) AS last_heartbeat_at
FROM dq.cdc_heartbeats;

CREATE VIEW IF NOT EXISTS dq.offset_metrics AS
SELECT topic, partition, min(offset) AS min_offset, max(offset) AS max_offset,
    uniqExact(offset) AS offset_count
FROM dq.event_samples FINAL
GROUP BY topic, partition;

CREATE VIEW IF NOT EXISTS dq.hourly_event_metrics AS
SELECT toStartOfHour(event_time) AS hour, count() AS events
FROM core.user_events FINAL
GROUP BY hour;

CREATE VIEW IF NOT EXISTS dq.orphan_metrics AS
SELECT count() AS events, countIf(NOT dictHas('core.users_dict', user_id)) AS orphans,
    (SELECT count() FROM core.users FINAL WHERE _is_deleted = 0) AS users_loaded
FROM core.user_events FINAL;

CREATE VIEW IF NOT EXISTS dq.kafka_consumer_metrics AS
SELECT database, table, consumer_id, assignments.topic AS topics,
    assignments.partition_id AS partitions, assignments.current_offset AS offsets
FROM system.kafka_consumers;

CREATE VIEW IF NOT EXISTS dq.insert_parts_metrics AS
SELECT database, table, partition, count() AS active_parts
FROM system.parts WHERE active
GROUP BY database, table, partition;

GRANT SELECT ON system.kafka_consumers TO role_engineer;
GRANT SELECT ON system.parts TO role_engineer;
GRANT SELECT ON system.part_log TO role_engineer;
GRANT SELECT ON system.tables TO role_engineer;
