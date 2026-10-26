SELECT user_id, city, device_type,
    dictGetOrDefault('core.users_dict', 'city', user_id, 'unknown') AS dictionary_city,
    dictGetOrDefault('core.users_dict', 'device_type', user_id, 'unknown') AS dictionary_device_type
FROM core.users FINAL
WHERE _is_deleted = 0;
