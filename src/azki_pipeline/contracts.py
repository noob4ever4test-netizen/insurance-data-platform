from datetime import UTC, datetime

from clickhouse_cityhash.cityhash import CityHash64

_UINT64_MASK = (1 << 64) - 1
_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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
