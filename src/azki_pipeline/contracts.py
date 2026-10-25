import json
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from clickhouse_cityhash.cityhash import CityHash64
from jsonschema import Draft202012Validator, FormatChecker

_UINT64_MASK = (1 << 64) - 1
_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "contracts"
_FORMATS = FormatChecker()


@_FORMATS.checks("clickhouse-datetime", raises=ValueError)
def _valid_csv_datetime(value: object) -> bool:
    if not isinstance(value, str):
        return True
    return datetime.strptime(value, _DATETIME_FORMAT).strftime(_DATETIME_FORMAT) == value


@_FORMATS.checks("utc-datetime", raises=ValueError)
def _valid_utc_datetime(value: object) -> bool:
    if not isinstance(value, str):
        return True
    return datetime.strptime(value, _UTC_FORMAT).strftime(_UTC_FORMAT) == value


def load_schema(name: str) -> dict:
    """Load an independent copy of one of the repository's documented contracts."""
    if name not in {"user_event", "user"}:
        raise ValueError(f"Unknown schema: {name}")
    return json.loads((_SCHEMA_DIR / f"{name}.schema.json").read_text(encoding="utf-8"))


@lru_cache(maxsize=3)
def _validator(name: str, wire: bool = False) -> Draft202012Validator:
    schema = load_schema(name)
    if wire:
        schema["properties"]["event_time"] = {"$ref": "#/$defs/utc_event_time"}
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=_FORMATS)


def validate_event(event: dict, *, wire: bool = False) -> None:
    """Validate CSV-typed events, or canonical UTC JSON events when wire=True."""
    _validator("user_event", wire).validate(event)


def validate_user(user: dict) -> None:
    _validator("user").validate(user)


def schema_diff(old: dict, new: dict) -> dict:
    """Describe top-level field types and required-field changes without mutation."""
    old_fields = old.get("properties", {})
    new_fields = new.get("properties", {})
    changed = {}
    for field in sorted(old_fields.keys() & new_fields.keys()):
        old_type = old_fields[field].get("type")
        new_type = new_fields[field].get("type")
        normalized_old = sorted(old_type) if isinstance(old_type, list) else old_type
        normalized_new = sorted(new_type) if isinstance(new_type, list) else new_type
        if normalized_old != normalized_new:
            changed[field] = {"old": old_type, "new": new_type}
    old_required = set(old.get("required", []))
    new_required = set(new.get("required", []))
    return {
        "added": sorted(new_fields.keys() - old_fields.keys()),
        "removed": sorted(old_fields.keys() - new_fields.keys()),
        "type_changed": changed,
        "required_added": sorted(new_required - old_required),
        "required_removed": sorted(old_required - new_required),
    }


def _int_hash64(value: int) -> int:
    value = (value & _UINT64_MASK) ^ 0x4CF2D2BAAE6DA887
    value ^= value >> 33
    value = (value * 0xFF51AFD7ED558CCD) & _UINT64_MASK
    value ^= value >> 33
    value = (value * 0xC4CEB9FE1A85EC53) & _UINT64_MASK
    return value ^ (value >> 33)


def _hash128to64(first: int, second: int) -> int:
    multiplier = 0x9DDFEA08EB382D69
    lower = ((first ^ second) * multiplier) & _UINT64_MASK
    lower ^= lower >> 47
    upper = ((second ^ lower) * multiplier) & _UINT64_MASK
    upper ^= upper >> 47
    return (upper * multiplier) & _UINT64_MASK


def event_key(user_id: int, session_id: str, event_time: str | datetime, event_type: str) -> int:
    """Match cityHash64(Int64, String, DateTime, String), with naive times in UTC."""
    if not -(1 << 63) <= user_id < (1 << 63):
        raise ValueError("user_id must fit ClickHouse Int64")
    if isinstance(event_time, str):
        parsed_time = datetime.strptime(event_time, _DATETIME_FORMAT)
        if parsed_time.strftime(_DATETIME_FORMAT) != event_time:
            raise ValueError("event_time must use YYYY-MM-DD HH:MM:SS")
    else:
        parsed_time = event_time
    if parsed_time.tzinfo is None:
        parsed_time = parsed_time.replace(tzinfo=UTC)
    epoch_seconds = int(parsed_time.timestamp())
    if not 0 <= epoch_seconds < (1 << 32):
        raise ValueError("event_time must fit ClickHouse DateTime")

    combined = _int_hash64(user_id)
    for component in (
        CityHash64(session_id.encode("utf-8")),
        _int_hash64(epoch_seconds),
        CityHash64(event_type.encode("utf-8")),
    ):
        combined = _hash128to64(combined, component)
    return combined
