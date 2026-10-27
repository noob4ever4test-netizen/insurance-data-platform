ALTER TABLE mart.purchase_events_enriched
REPLACE PARTITION {partition:UInt32} FROM mart.purchase_events_backfill;
