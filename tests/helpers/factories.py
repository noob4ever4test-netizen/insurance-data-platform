def make_event(**overrides) -> dict:
    """Return a fresh synthetic event in the contract's pre-serialization types."""
    return {
        "event_time": "2025-10-16 17:17:33",
        "user_id": 3,
        "session_id": "s_1",
        "event_type": "policy_click",
        "channel": "Google Ads",
        "premium_amount": 1000,
    } | overrides


def make_user(**overrides) -> dict:
    """Return a fresh synthetic user in the contract's pre-serialization types."""
    return {
        "user_id": 3,
        "signup_date": "2023-09-28",
        "city": "Shiraz",
        "device_type": "Desktop",
    } | overrides
