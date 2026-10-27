ALTER TABLE core.user_events REPLACE PARTITION {partition:UInt32} FROM core.user_events_staging;
