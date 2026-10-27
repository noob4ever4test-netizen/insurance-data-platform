SELECT coalesce(max(active_parts), 0) AS max_parts FROM dq.insert_parts_metrics
WHERE database IN {databases:Array(String)};
