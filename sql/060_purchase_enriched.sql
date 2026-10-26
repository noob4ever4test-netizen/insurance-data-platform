CREATE TABLE IF NOT EXISTS core.financial_order
(
    order_id Int64,
    user_id Int64,
    session_id String,
    created_at DateTime('UTC'),
    product_type LowCardinality(String),
    payment_status LowCardinality(String),
    paid_amount Int64,
    _version UInt64,
    _is_deleted UInt8 DEFAULT 0,
    _topic String DEFAULT '',
    _partition UInt64 DEFAULT 0,
    _offset UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY tuple()
ORDER BY (product_type, order_id);

CREATE TABLE IF NOT EXISTS raw.kafka_financial_order
(
    order_id Int64, user_id Int64, session_id String, created_at Int64,
    product_type String, payment_status String, paid_amount Int64,
    __ts_ms UInt64, __deleted String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'mysql.azki.financial_order',
    kafka_group_name = 'azki-financial-order', kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream', kafka_flush_interval_ms = 500,
    input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.financial_order_mv TO core.financial_order AS
SELECT order_id, user_id, session_id, toDateTime(fromUnixTimestamp64Milli(created_at), 'UTC') AS created_at,
    product_type, payment_status, paid_amount, __ts_ms AS _version,
    toUInt8(__deleted = 'true') AS _is_deleted, _topic, _partition, _offset
FROM raw.kafka_financial_order WHERE _error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.financial_order_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_financial_order WHERE _error != '';

CREATE VIEW IF NOT EXISTS core.purchase_order_lookup AS
SELECT orders.user_id AS user_id, orders.session_id AS session_id,
    any(orders.order_id) AS order_id, any(orders.created_at) AS order_created_at,
    any(orders.product_type) AS product_type, any(orders.partner_id) AS partner_id,
    any(orders.national_id) AS national_id, any(orders.phone) AS phone,
    any(orders.policy_no) AS policy_no, any(orders.premium_amount) AS order_premium_amount,
    any(toJSONString(orders.product_attributes)) AS product_attributes_json,
    any(financial.created_at) AS financial_created_at,
    any(financial.payment_status) AS payment_status, any(financial.paid_amount) AS paid_amount
FROM
    (SELECT order_id, user_id, session_id, created_at, product_type, partner_id,
        national_id, phone, policy_no, premium_amount, product_attributes
     FROM core.orders_unified FINAL WHERE _is_deleted = 0) AS orders
INNER JOIN
    (SELECT order_id, user_id, session_id, created_at, product_type, payment_status, paid_amount
     FROM core.financial_order FINAL WHERE _is_deleted = 0) AS financial
ON orders.product_type = financial.product_type AND orders.order_id = financial.order_id
    AND orders.user_id = financial.user_id AND orders.session_id = financial.session_id
GROUP BY orders.user_id, orders.session_id
HAVING count() = 1;

CREATE DICTIONARY IF NOT EXISTS core.purchase_orders_dict
(
    user_id Int64,
    session_id String,
    order_id Int64,
    order_created_at DateTime('UTC'),
    product_type String,
    partner_id String,
    national_id String,
    phone String,
    policy_no String,
    order_premium_amount Int64,
    product_attributes_json String DEFAULT '{}',
    financial_created_at DateTime('UTC'),
    payment_status String,
    paid_amount Int64
)
PRIMARY KEY user_id, session_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' DB 'core' TABLE 'purchase_order_lookup'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 1 MAX 5);

CREATE TABLE IF NOT EXISTS mart.purchase_events_enriched
(
    event_date Date,
    event_time DateTime('UTC'),
    event_key UInt64,
    user_id Int64,
    session_id String,
    event_type LowCardinality(String),
    channel LowCardinality(String),
    premium_amount Int64,
    city LowCardinality(String),
    device_type LowCardinality(String),
    order_id Int64,
    order_created_at DateTime('UTC'),
    product_type LowCardinality(String),
    partner_id LowCardinality(String),
    national_id String,
    phone String,
    policy_no String,
    order_premium_amount Int64,
    product_attributes Map(String, String),
    financial_created_at DateTime('UTC'),
    payment_status LowCardinality(String),
    paid_amount Int64,
    _is_deleted UInt8,
    enriched_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(enriched_at, _is_deleted)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, session_id, event_time, event_key);

CREATE MATERIALIZED VIEW IF NOT EXISTS core.purchase_events_enriched_mv
TO mart.purchase_events_enriched AS
WITH dictGet('core.purchase_orders_dict',
    ('order_id', 'order_created_at', 'product_type', 'partner_id', 'national_id', 'phone',
     'policy_no', 'order_premium_amount', 'product_attributes_json', 'financial_created_at',
     'payment_status', 'paid_amount'), (user_id, session_id)) AS purchase_order
SELECT toDate(event_time) AS event_date, event_time, event_key, user_id, session_id,
    event_type, channel, premium_amount,
    dictGetOrDefault('core.users_dict', 'city', user_id, 'unknown') AS city,
    dictGetOrDefault('core.users_dict', 'device_type', user_id, 'unknown') AS device_type,
    purchase_order.1 AS order_id, purchase_order.2 AS order_created_at,
    purchase_order.3 AS product_type, purchase_order.4 AS partner_id,
    purchase_order.5 AS national_id, purchase_order.6 AS phone, purchase_order.7 AS policy_no,
    purchase_order.8 AS order_premium_amount,
    JSONExtract(purchase_order.9, 'Map(String, String)') AS product_attributes,
    purchase_order.10 AS financial_created_at, purchase_order.11 AS payment_status,
    purchase_order.12 AS paid_amount,
    toUInt8(NOT dictHas('core.purchase_orders_dict', (user_id, session_id))
        OR abs(dateDiff('second', event_time, purchase_order.2)) > 3600) AS _is_deleted,
    now64(6) AS enriched_at
FROM core.user_events
WHERE event_type = 'purchase';

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.purchase_events_repair_mv
REFRESH EVERY 1 MINUTE APPEND TO mart.purchase_events_enriched AS
WITH dictGet('core.purchase_orders_dict',
    ('order_id', 'order_created_at', 'product_type', 'partner_id', 'national_id', 'phone',
     'policy_no', 'order_premium_amount', 'product_attributes_json', 'financial_created_at',
     'payment_status', 'paid_amount'), (user_id, session_id)) AS purchase_order
SELECT toDate(event_time) AS event_date, event_time, event_key, user_id, session_id,
    event_type, channel, premium_amount,
    dictGetOrDefault('core.users_dict', 'city', user_id, 'unknown') AS city,
    dictGetOrDefault('core.users_dict', 'device_type', user_id, 'unknown') AS device_type,
    purchase_order.1 AS order_id, purchase_order.2 AS order_created_at,
    purchase_order.3 AS product_type, purchase_order.4 AS partner_id,
    purchase_order.5 AS national_id, purchase_order.6 AS phone, purchase_order.7 AS policy_no,
    purchase_order.8 AS order_premium_amount,
    JSONExtract(purchase_order.9, 'Map(String, String)') AS product_attributes,
    purchase_order.10 AS financial_created_at, purchase_order.11 AS payment_status,
    purchase_order.12 AS paid_amount,
    toUInt8(NOT dictHas('core.purchase_orders_dict', (user_id, session_id))
        OR abs(dateDiff('second', event_time, purchase_order.2)) > 3600) AS _is_deleted,
    now64(6) AS enriched_at
FROM core.user_events FINAL
WHERE event_type = 'purchase'
    AND event_time >= toStartOfMonth(now('UTC') - INTERVAL 2 MONTH);

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.user_events_mv TO core.user_events AS
SELECT event_time, user_id, session_id, event_type, channel, premium_amount,
    _topic, _partition, _offset, now64(6) AS ingested_at
FROM raw.kafka_user_events
WHERE _error = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.user_events_dead_letter_mv TO dq.kafka_dead_letter AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
    _raw_message AS raw_message, _error AS error, now64(6) AS ingested_at
FROM raw.kafka_user_events
WHERE _error != '';
