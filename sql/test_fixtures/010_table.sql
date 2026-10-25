CREATE TABLE IF NOT EXISTS core.runner_probe
(
    user_id Int64,
    note String DEFAULT 'core.runner_probe; unchanged'
)
ENGINE = Memory;

SELECT note FROM core.runner_probe WHERE note = 'core.runner_probe; unchanged';
