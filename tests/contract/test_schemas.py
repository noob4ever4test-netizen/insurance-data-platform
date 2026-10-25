import csv
import json
from datetime import datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from tests.helpers.factories import make_event, make_user

ROOT = Path(__file__).resolve().parents[2]
FORMATS = FormatChecker()


@FORMATS.checks("clickhouse-datetime", raises=ValueError)
def valid_datetime(value):
    if not isinstance(value, str):
        return True
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") == value


def validator(name):
    schema = json.loads((ROOT / "contracts" / f"{name}.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FORMATS)


@pytest.mark.contract
@pytest.mark.parametrize(("name", "factory"), [("user_event", make_event), ("user", make_user)])
def test_C11_schema_accepts_synthetic_factory(name, factory):
    validator(name).validate(factory())


@pytest.mark.contract
@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": None},
        {"event_time": "2025-02-30 12:00:00"},
        {"event_time": "2025-10-16T17:17:33Z"},
        {"event_type": "unknown"},
        {"premium_amount": -1},
        {"premium_amount": 0.5},
        {"user_id": 2**63},
    ],
)
def test_C11_event_schema_rejects_invalid_values(overrides):
    with pytest.raises(ValidationError):
        validator("user_event").validate(make_event(**overrides))


@pytest.mark.contract
@pytest.mark.parametrize(("name", "factory"), [("user_event", make_event), ("user", make_user)])
def test_C11_schema_requires_every_contract_field(name, factory):
    row = factory()

    for field in row:
        incomplete = row.copy()
        del incomplete[field]
        with pytest.raises(ValidationError):
            validator(name).validate(incomplete)


@pytest.mark.contract
@pytest.mark.parametrize(
    "overrides", [{"user_id": None}, {"signup_date": "2025-02-30"}, {"city": 3}]
)
def test_C11_user_schema_rejects_invalid_values(overrides):
    with pytest.raises(ValidationError):
        validator("user").validate(make_user(**overrides))


@pytest.mark.contract
@pytest.mark.parametrize(
    ("name", "filename"), [("user_event", "user_events.csv"), ("user", "users.csv")]
)
def test_C11_schema_accepts_all_synthetic_fixture_rows(name, filename):
    check = validator(name)

    with (ROOT / "tests" / "fixtures" / filename).open(newline="") as stream:
        for row in csv.DictReader(stream):
            row["user_id"] = int(row["user_id"])
            if "premium_amount" in row:
                row["premium_amount"] = int(row["premium_amount"])
            check.validate(row)


@pytest.mark.contract
def test_C11_schema_keeps_channels_and_dimensions_open_for_drift_detection():
    validator("user_event").validate(make_event(channel="New channel", utm_campaign="new"))
    validator("user").validate(make_user(city="New city", device_type="New device"))
