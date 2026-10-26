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


PRODUCT_ATTRIBUTES = {
    "third": ("plate_no", "TEST-PLATE-1"),
    "body": ("car_value", 100000000),
    "medical": ("coverage_limit", 200000000),
    "fire": ("property_value", 300000000),
}


def make_order(product_type="third", **overrides) -> dict:
    attribute, value = PRODUCT_ATTRIBUTES[product_type]
    return {
        "product_type": product_type,
        "order_id": 1,
        "user_id": 3,
        "session_id": "s_1",
        "created_at": "2025-10-16 17:17:33",
        "partner_id": "partner_1",
        "national_id": "0000000001",
        "phone": "09000000001",
        "policy_no": "SYNTHETIC-1",
        "premium_amount": 1200,
        attribute: value,
        "_version": 1,
        "_is_deleted": 0,
    } | overrides


def make_financial(product_type="third", **overrides) -> dict:
    return {
        "order_id": 1,
        "user_id": 3,
        "session_id": "s_1",
        "created_at": "2025-10-16 17:17:33",
        "product_type": product_type,
        "payment_status": "paid",
        "paid_amount": 1200,
        "_version": 1,
        "_is_deleted": 0,
    } | overrides
