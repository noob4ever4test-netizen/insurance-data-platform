SELECT countIf(event_type = 'NewPart') AS inserted_parts,
    countIf(error != 0) AS insert_errors
FROM system.part_log WHERE database IN {databases:Array(String)}
    AND event_time >= {window_start:DateTime('UTC')}
    AND event_time < {as_of:DateTime64(6, 'UTC')};
