import csv
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
EVENT_COLUMNS = ["event_time", "user_id", "session_id", "event_type", "channel", "premium_amount"]
USER_COLUMNS = ["user_id", "signup_date", "city", "device_type", "_version", "_is_deleted"]


def fixture_rows(name):
    with (FIXTURES / name).open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    for row in rows:
        row["user_id"] = int(row["user_id"])
        if "premium_amount" in row:
            row["premium_amount"] = int(row["premium_amount"])
    return rows


def insert_users(client, database, users, version=1, deleted=0):
    rows = [
        [
            user["user_id"],
            datetime.strptime(user["signup_date"], "%Y-%m-%d").date(),
            user["city"],
            user["device_type"],
            version,
            deleted,
        ]
        for user in users
    ]
    client.insert(f"{database}.users", rows, column_names=USER_COLUMNS)
    client.command(f"SYSTEM RELOAD DICTIONARY {database}.users_dict")


def insert_events(client, database, events):
    rows = [
        [
            datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC),
            *[event[column] for column in EVENT_COLUMNS[1:]],
        ]
        for event in events
    ]
    client.insert(f"{database}.user_events", rows, column_names=EVENT_COLUMNS)


def aggregate_rows(client, database):
    return client.query(
        f"SELECT event_date, event_type, channel, city, device_type, "
        f"countMerge(event_count), uniqMerge(user_count), uniqMerge(session_count), "
        f"sumMerge(premium_sum), avgMerge(premium_avg) FROM {database}.user_event_agg "
        "GROUP BY event_date, event_type, channel, city, device_type ORDER BY 1, 2, 3, 4, 5"
    ).result_rows


def expected_aggregates(events, users):
    dimensions = {user["user_id"]: user for user in users}
    buckets = defaultdict(list)
    for event in events:
        user = dimensions.get(event["user_id"], {})
        key = (
            datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S").date(),
            event["event_type"],
            event["channel"],
            user.get("city", "unknown"),
            user.get("device_type", "unknown"),
        )
        buckets[key].append(event)
    return [
        (
            *key,
            len(rows),
            len({row["user_id"] for row in rows}),
            len({row["session_id"] for row in rows}),
            sum(row["premium_amount"] for row in rows),
            sum(row["premium_amount"] for row in rows) / len(rows),
        )
        for key, rows in sorted(buckets.items())
    ]
