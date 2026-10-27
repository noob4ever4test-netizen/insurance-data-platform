TRUNCATE TABLE mart.purchase_events_backfill;

INSERT INTO mart.purchase_events_backfill
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
WHERE event_type = 'purchase' AND toYYYYMM(event_time) = {partition:UInt32};
