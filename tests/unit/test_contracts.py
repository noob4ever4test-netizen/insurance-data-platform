import pytest
from jsonschema import ValidationError

from azki_pipeline.contracts import load_schema, schema_diff, validate_event, validate_user
from tests.helpers.factories import make_event, make_user


@pytest.mark.unit
def test_U01_valid_event_passes_contract():
    event = make_event()

    assert validate_event(event) is None
    assert load_schema("user_event")["required"] == list(event)


@pytest.mark.unit
def test_U01_valid_user_passes_contract():
    assert validate_user(make_user()) is None


@pytest.mark.unit
def test_U01_schema_loads_are_independent():
    schema = load_schema("user_event")

    schema["properties"]["user_id"]["type"] = "string"

    assert load_schema("user_event")["properties"]["user_id"]["type"] == "integer"


@pytest.mark.unit
def test_U01_unknown_schema_name_is_rejected():
    with pytest.raises(ValueError, match="schema"):
        load_schema("../user_event")


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": None},
        {"user_id": 2**63},
        {"event_time": "not a timestamp"},
        {"event_time": "2025-02-30 00:00:00"},
        {"event_time": "2025-1-1 00:00:00"},
        {"event_type": "unknown"},
        {"premium_amount": -1},
        {"premium_amount": 1.5},
    ],
)
def test_U02_invalid_event_values_fail(overrides):
    with pytest.raises(ValidationError):
        validate_event(make_event(**overrides))


@pytest.mark.unit
def test_U02_missing_event_time_fails():
    event = make_event()
    del event["event_time"]

    with pytest.raises(ValidationError, match="event_time"):
        validate_event(event)


@pytest.mark.unit
def test_U02_invalid_signup_calendar_date_fails():
    with pytest.raises(ValidationError):
        validate_user(make_user(signup_date="2025-02-30"))


@pytest.mark.unit
@pytest.mark.parametrize(
    "timestamp",
    ["2025-10-16 17:17:33", "2025-02-30T17:17:33Z", "2025-10-16T17:17:33+03:30"],
)
def test_U02_wire_timestamps_must_be_valid_canonical_utc(timestamp):
    with pytest.raises(ValidationError):
        validate_event(make_event(event_time=timestamp), wire=True)


@pytest.mark.unit
def test_U06_schema_diff_reports_field_and_required_changes():
    old = load_schema("user_event")
    new = load_schema("user_event")
    new["properties"]["utm_campaign"] = {"type": "string"}
    del new["properties"]["channel"]
    new["properties"]["user_id"]["type"] = "string"
    new["required"].remove("channel")
    new["required"].append("utm_campaign")

    differences = schema_diff(old, new)

    assert differences == {
        "added": ["utm_campaign"],
        "removed": ["channel"],
        "type_changed": {"user_id": {"old": "integer", "new": "string"}},
        "required_added": ["utm_campaign"],
        "required_removed": ["channel"],
    }
    assert "channel" in old["properties"]


@pytest.mark.unit
def test_U06_schema_diff_ignores_union_type_order():
    old = load_schema("user_event")
    new = load_schema("user_event")
    old["properties"]["user_id"]["type"] = ["integer", "null"]
    new["properties"]["user_id"]["type"] = ["null", "integer"]

    assert schema_diff(old, new)["type_changed"] == {}


@pytest.mark.unit
def test_U06_identical_schemas_have_no_changes():
    schema = load_schema("user_event")

    assert schema_diff(schema, schema) == {
        "added": [],
        "removed": [],
        "type_changed": {},
        "required_added": [],
        "required_removed": [],
    }
