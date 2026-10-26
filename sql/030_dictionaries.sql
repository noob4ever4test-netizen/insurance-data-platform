CREATE DICTIONARY IF NOT EXISTS core.users_dict
(
    user_id Int64,
    city String DEFAULT 'unknown',
    device_type String DEFAULT 'unknown'
)
PRIMARY KEY user_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' DB 'core' TABLE 'users_latest'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 1 MAX 5);

CREATE VIEW IF NOT EXISTS dq.user_event_orphans AS
SELECT count() AS event_count,
    countIf(NOT dictHas('core.users_dict', user_id)) AS orphan_count,
    if(event_count = 0, 0., orphan_count / event_count) AS orphan_ratio
FROM core.user_events FINAL;
