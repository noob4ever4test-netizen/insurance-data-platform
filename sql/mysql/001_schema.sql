CREATE DATABASE IF NOT EXISTS azki;

CREATE TABLE IF NOT EXISTS azki.users (
    user_id BIGINT PRIMARY KEY,
    signup_date DATE NOT NULL,
    city VARCHAR(64) NOT NULL,
    device_type VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS azki.third_orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    created_at DATETIME NOT NULL,
    partner_id VARCHAR(32) NOT NULL,
    national_id VARCHAR(10) NOT NULL,
    phone VARCHAR(16) NOT NULL,
    policy_no VARCHAR(64) NOT NULL,
    premium_amount BIGINT NOT NULL,
    plate_no VARCHAR(32) NOT NULL,
    KEY event_join (user_id, session_id, created_at)
);

CREATE TABLE IF NOT EXISTS azki.body_orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    created_at DATETIME NOT NULL,
    partner_id VARCHAR(32) NOT NULL,
    national_id VARCHAR(10) NOT NULL,
    phone VARCHAR(16) NOT NULL,
    policy_no VARCHAR(64) NOT NULL,
    premium_amount BIGINT NOT NULL,
    car_value BIGINT NOT NULL,
    KEY event_join (user_id, session_id, created_at)
);

CREATE TABLE IF NOT EXISTS azki.medical_orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    created_at DATETIME NOT NULL,
    partner_id VARCHAR(32) NOT NULL,
    national_id VARCHAR(10) NOT NULL,
    phone VARCHAR(16) NOT NULL,
    policy_no VARCHAR(64) NOT NULL,
    premium_amount BIGINT NOT NULL,
    coverage_limit BIGINT NOT NULL,
    KEY event_join (user_id, session_id, created_at)
);

CREATE TABLE IF NOT EXISTS azki.fire_orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    created_at DATETIME NOT NULL,
    partner_id VARCHAR(32) NOT NULL,
    national_id VARCHAR(10) NOT NULL,
    phone VARCHAR(16) NOT NULL,
    policy_no VARCHAR(64) NOT NULL,
    premium_amount BIGINT NOT NULL,
    property_value BIGINT NOT NULL,
    KEY event_join (user_id, session_id, created_at)
);

CREATE TABLE IF NOT EXISTS azki.financial_order (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    created_at DATETIME NOT NULL,
    product_type VARCHAR(16) NOT NULL,
    payment_status VARCHAR(16) NOT NULL,
    paid_amount BIGINT NOT NULL,
    KEY event_join (user_id, session_id, created_at)
);

REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'debezium'@'%';
GRANT SELECT, LOCK TABLES ON azki.* TO 'debezium'@'%';
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
