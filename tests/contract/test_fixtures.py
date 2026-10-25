import csv
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
HEADERS = {
    "user_events.csv": [
        "event_time",
        "user_id",
        "session_id",
        "event_type",
        "channel",
        "premium_amount",
    ],
    "users.csv": ["user_id", "signup_date", "city", "device_type"],
}


@pytest.mark.contract
@pytest.mark.parametrize("filename", HEADERS)
def test_C01_synthetic_and_available_real_headers_match_contract(filename):
    paths = [ROOT / "tests" / "fixtures" / filename]
    real_path = ROOT / "data" / filename
    if real_path.exists():
        paths.append(real_path)

    for path in paths:
        with path.open(newline="", encoding="utf-8-sig") as stream:
            header = next(csv.reader(stream))

        assert header == HEADERS[filename], f"Header mismatch: {path.name}"
