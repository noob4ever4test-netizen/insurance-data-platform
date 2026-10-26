ALTER TABLE mart.user_event_agg
REPLACE PARTITION {partition:UInt32} FROM mart.user_event_agg_rebuild;
