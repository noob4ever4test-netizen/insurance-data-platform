CREATE TABLE IF NOT EXISTS core.third_orders
(
    order_id Int64,
    user_id Int64,
    session_id String,
    created_at DateTime('UTC'),
    partner_id LowCardinality(String),
    national_id String,
    phone String,
    policy_no String,
    premium_amount Int64,
    plate_no String,
    _version UInt64,
    _is_deleted UInt8 DEFAULT 0,
    _topic String DEFAULT '',
    _partition UInt64 DEFAULT 0,
    _offset UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY tuple()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS core.body_orders
(
    order_id Int64,
    user_id Int64,
    session_id String,
    created_at DateTime('UTC'),
    partner_id LowCardinality(String),
    national_id String,
    phone String,
    policy_no String,
    premium_amount Int64,
    car_value Int64,
    _version UInt64,
    _is_deleted UInt8 DEFAULT 0,
    _topic String DEFAULT '',
    _partition UInt64 DEFAULT 0,
    _offset UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY tuple()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS core.medical_orders
(
    order_id Int64,
    user_id Int64,
    session_id String,
    created_at DateTime('UTC'),
    partner_id LowCardinality(String),
    national_id String,
    phone String,
    policy_no String,
    premium_amount Int64,
    coverage_limit Int64,
    _version UInt64,
    _is_deleted UInt8 DEFAULT 0,
    _topic String DEFAULT '',
    _partition UInt64 DEFAULT 0,
    _offset UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY tuple()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS core.fire_orders
(
    order_id Int64,
    user_id Int64,
    session_id String,
    created_at DateTime('UTC'),
    partner_id LowCardinality(String),
    national_id String,
    phone String,
    policy_no String,
    premium_amount Int64,
    property_value Int64,
    _version UInt64,
    _is_deleted UInt8 DEFAULT 0,
    _topic String DEFAULT '',
    _partition UInt64 DEFAULT 0,
    _offset UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY tuple()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS core.orders_unified
(
    product_type LowCardinality(String),
    order_id Int64,
    user_id Int64,
    session_id String,
    created_at DateTime('UTC'),
    partner_id LowCardinality(String),
    national_id String,
    phone String,
    policy_no String,
    premium_amount Int64,
    product_attributes Map(String, String),
    _version UInt64,
    _is_deleted UInt8,
    _topic String,
    _partition UInt64,
    _offset UInt64
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY tuple()
ORDER BY (product_type, order_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS core.third_orders_union_mv TO core.orders_unified AS
SELECT 'third' AS product_type, order_id, user_id, session_id, created_at,
    partner_id, national_id, phone, policy_no, premium_amount,
    map('plate_no', plate_no) AS product_attributes,
    _version, _is_deleted, _topic, _partition, _offset
FROM core.third_orders;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.body_orders_union_mv TO core.orders_unified AS
SELECT 'body' AS product_type, order_id, user_id, session_id, created_at,
    partner_id, national_id, phone, policy_no, premium_amount,
    map('car_value', toString(car_value)) AS product_attributes,
    _version, _is_deleted, _topic, _partition, _offset
FROM core.body_orders;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.medical_orders_union_mv TO core.orders_unified AS
SELECT 'medical' AS product_type, order_id, user_id, session_id, created_at,
    partner_id, national_id, phone, policy_no, premium_amount,
    map('coverage_limit', toString(coverage_limit)) AS product_attributes,
    _version, _is_deleted, _topic, _partition, _offset
FROM core.medical_orders;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.fire_orders_union_mv TO core.orders_unified AS
SELECT 'fire' AS product_type, order_id, user_id, session_id, created_at,
    partner_id, national_id, phone, policy_no, premium_amount,
    map('property_value', toString(property_value)) AS product_attributes,
    _version, _is_deleted, _topic, _partition, _offset
FROM core.fire_orders;

CREATE TABLE IF NOT EXISTS raw.kafka_third_orders
(
    order_id Int64, user_id Int64, session_id String, created_at Int64,
    partner_id String, national_id String, phone String, policy_no String,
    premium_amount Int64, plate_no String,
    __ts_ms UInt64, __deleted String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'mysql.azki.third_orders',
    kafka_group_name = 'azki-third-orders', kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream', kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE TABLE IF NOT EXISTS raw.kafka_body_orders
(
    order_id Int64, user_id Int64, session_id String, created_at Int64,
    partner_id String, national_id String, phone String, policy_no String,
    premium_amount Int64, car_value Int64,
    __ts_ms UInt64, __deleted String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'mysql.azki.body_orders',
    kafka_group_name = 'azki-body-orders', kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream', kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE TABLE IF NOT EXISTS raw.kafka_medical_orders
(
    order_id Int64, user_id Int64, session_id String, created_at Int64,
    partner_id String, national_id String, phone String, policy_no String,
    premium_amount Int64, coverage_limit Int64,
    __ts_ms UInt64, __deleted String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'mysql.azki.medical_orders',
    kafka_group_name = 'azki-medical-orders', kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream', kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE TABLE IF NOT EXISTS raw.kafka_fire_orders
(
    order_id Int64, user_id Int64, session_id String, created_at Int64,
    partner_id String, national_id String, phone String, policy_no String,
    premium_amount Int64, property_value Int64,
    __ts_ms UInt64, __deleted String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'mysql.azki.fire_orders',
    kafka_group_name = 'azki-fire-orders', kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream', kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.third_orders_mv TO core.third_orders AS
SELECT order_id, user_id, session_id, toDateTime(fromUnixTimestamp64Milli(created_at), 'UTC') AS created_at,
    partner_id, national_id, phone, policy_no, premium_amount, plate_no,
    __ts_ms AS _version, toUInt8(__deleted = 'true') AS _is_deleted, _topic, _partition, _offset
FROM raw.kafka_third_orders WHERE _error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.body_orders_mv TO core.body_orders AS
SELECT order_id, user_id, session_id, toDateTime(fromUnixTimestamp64Milli(created_at), 'UTC') AS created_at,
    partner_id, national_id, phone, policy_no, premium_amount, car_value,
    __ts_ms AS _version, toUInt8(__deleted = 'true') AS _is_deleted, _topic, _partition, _offset
FROM raw.kafka_body_orders WHERE _error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.medical_orders_mv TO core.medical_orders AS
SELECT order_id, user_id, session_id, toDateTime(fromUnixTimestamp64Milli(created_at), 'UTC') AS created_at,
    partner_id, national_id, phone, policy_no, premium_amount, coverage_limit,
    __ts_ms AS _version, toUInt8(__deleted = 'true') AS _is_deleted, _topic, _partition, _offset
FROM raw.kafka_medical_orders WHERE _error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.fire_orders_mv TO core.fire_orders AS
SELECT order_id, user_id, session_id, toDateTime(fromUnixTimestamp64Milli(created_at), 'UTC') AS created_at,
    partner_id, national_id, phone, policy_no, premium_amount, property_value,
    __ts_ms AS _version, toUInt8(__deleted = 'true') AS _is_deleted, _topic, _partition, _offset
FROM raw.kafka_fire_orders WHERE _error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.third_orders_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_third_orders WHERE _error != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.body_orders_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_body_orders WHERE _error != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.medical_orders_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_medical_orders WHERE _error != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.fire_orders_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_fire_orders WHERE _error != '';
