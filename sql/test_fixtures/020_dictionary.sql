CREATE DICTIONARY IF NOT EXISTS core.runner_probe_dict (user_id Int64)
PRIMARY KEY user_id
SOURCE(CLICKHOUSE(DB 'core' TABLE 'runner_probe'))
LAYOUT(HASHED())
LIFETIME(0);

SELECT dictGet('core.runner_probe_dict', 'user_id', toUInt64(3));
