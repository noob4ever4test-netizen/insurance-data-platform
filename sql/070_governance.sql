CREATE ROLE IF NOT EXISTS role_admin;
CREATE ROLE IF NOT EXISTS role_engineer;
CREATE ROLE IF NOT EXISTS role_analyst;
CREATE ROLE IF NOT EXISTS role_partner_1;
CREATE ROLE IF NOT EXISTS role_partner_2;

GRANT CURRENT GRANTS ON *.* TO role_admin WITH GRANT OPTION;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY,
    DROP TABLE, DROP VIEW, DROP DICTIONARY, TRUNCATE, OPTIMIZE, DICTGET ON raw.* TO role_engineer;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY,
    DROP TABLE, DROP VIEW, DROP DICTIONARY, TRUNCATE, OPTIMIZE, DICTGET ON core.* TO role_engineer;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW,
    TRUNCATE, OPTIMIZE ON mart.* TO role_engineer;
GRANT SELECT, INSERT ON dq.* TO role_engineer;

CREATE VIEW IF NOT EXISTS gov.purchase_events_masked
DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT event_date, event_time, event_key, user_id, session_id, event_type,
    channel, premium_amount, city, device_type, order_id, order_created_at,
    product_type, partner_id, '***' AS national_id, '***' AS phone,
    '***' AS policy_no, order_premium_amount,
    mapUpdate(
        mapFilter((attribute, value) -> attribute IN ('car_value', 'coverage_limit', 'property_value'),
            product_attributes),
        map('plate_no', '***')) AS product_attributes,
    financial_created_at, payment_status, paid_amount
FROM mart.purchase_events_enriched FINAL
WHERE _is_deleted = 0;

GRANT SELECT ON gov.purchase_events_masked TO role_analyst;
GRANT SELECT(name, value, readonly) ON system.settings TO role_analyst;
GRANT SELECT(event_date, event_time, event_key, user_id, session_id, event_type,
    channel, premium_amount, city, device_type, order_id, order_created_at,
    product_type, partner_id, order_premium_amount, financial_created_at,
    payment_status, paid_amount, _is_deleted)
ON mart.purchase_events_enriched TO role_partner_1, role_partner_2;

CREATE ROW POLICY IF NOT EXISTS azki_governance_partner_1 ON mart.purchase_events_enriched
USING partner_id = 'partner_1' TO role_partner_1;
CREATE ROW POLICY IF NOT EXISTS azki_governance_partner_2 ON mart.purchase_events_enriched
USING partner_id = 'partner_2' TO role_partner_2;
CREATE ROW POLICY IF NOT EXISTS azki_governance_unrestricted ON mart.purchase_events_enriched
USING 1 TO ALL EXCEPT role_partner_1, role_partner_2;

CREATE SETTINGS PROFILE IF NOT EXISTS azki_governance_analyst_limits SETTINGS
    max_execution_time = 10 MIN 1 MAX 10, max_result_rows = 10000 MIN 1 MAX 10000,
    max_memory_usage = 268435456 MIN 1 MAX 268435456,
    log_queries = 1 READONLY, log_queries_probability = 1 READONLY,
    log_queries_min_query_duration_ms = 0 READONLY, log_queries_min_type = 'QUERY_START' READONLY
TO role_analyst;
CREATE QUOTA IF NOT EXISTS azki_governance_analyst_quota KEYED BY user_name
FOR INTERVAL 1 HOUR MAX queries = 1000 TO NONE;
