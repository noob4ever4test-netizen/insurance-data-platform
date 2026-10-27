import json
from statistics import median

from azki_pipeline.dq.models import CheckResult


def _severity(value, thresholds):
    if value > thresholds["fail"]:
        return "FAIL"
    return "WARN" if value > thresholds["warn"] else "PASS"


def _result(name, metrics, thresholds, observed, status, **details):
    return CheckResult(
        name=name,
        status=status,
        observed=observed,
        threshold=thresholds,
        details=details,
        checked_at=metrics["checked_at"],
    )


def _freshness(name, metrics, thresholds, fields):
    timestamps = [metrics.get(field) for field in fields]
    if any(value is None for value in timestamps):
        return _result(name, metrics, thresholds, {}, "WARN", reason="No freshness evidence")
    ages = [(metrics["checked_at"] - value).total_seconds() for value in timestamps]
    observed = {field: metrics[field] for field in fields}
    observed["age_seconds"] = max(0, max(ages))
    status = _severity(observed["age_seconds"], thresholds)
    if min(ages) < -thresholds.get("future_tolerance", 60):
        status = "FAIL"
    return _result(name, metrics, thresholds, observed, status)


def evaluate_freshness(metrics, thresholds):
    return _freshness("freshness", metrics, thresholds, ("last_event_at", "last_ingested_at"))


def evaluate_cdc_lag(metrics, thresholds):
    return _freshness("cdc_lag", metrics, thresholds, ("last_heartbeat_at",))


def evaluate_lag(metrics, thresholds):
    partitions = metrics["partitions"]
    lags = [max(0, row["high"] - max(row["committed"], row["low"])) for row in partitions]
    maximum = max(lags, default=0)
    return _result(
        "lag",
        metrics,
        thresholds,
        {"max_lag": maximum, "total_lag": sum(lags)},
        _severity(maximum, thresholds) if partitions else "WARN",
        partitions=len(lags),
    )


def evaluate_offset_gaps(metrics, thresholds):
    partitions = metrics["partitions"]
    gaps = sum(
        max(0, row["max_offset"] - row["min_offset"] + 1 - row["offset_count"])
        for row in partitions
    )
    return _result(
        "offset_gaps",
        metrics,
        thresholds,
        {"missing_offsets": gaps},
        _severity(gaps, thresholds) if partitions else "WARN",
    )


def evaluate_source_reconciliation(metrics, thresholds):
    source = metrics["source_counts"]
    sink = metrics["sink_counts"]
    if not source:
        return _result(
            "source_reconciliation",
            metrics,
            thresholds,
            {},
            "WARN",
            reason="Independent hourly source counts were not supplied",
        )
    mismatches = {
        hour: sink.get(hour, 0) - source.get(hour, 0)
        for hour in source.keys() | sink.keys()
        if sink.get(hour, 0) != source.get(hour, 0)
    }
    difference = sum(abs(value) for value in mismatches.values())
    return _result(
        "source_reconciliation",
        metrics,
        thresholds,
        {"difference": difference},
        _severity(difference, thresholds),
        mismatches=mismatches,
    )


def evaluate_volume(metrics, thresholds):
    history = metrics["history"]
    if len(history) < thresholds["min_history"]:
        return _result(
            "volume",
            metrics,
            thresholds,
            {"history_points": len(history)},
            "WARN",
            reason="Insufficient same-hour history",
        )
    baseline = median(history)
    mad = median(abs(value - baseline) for value in history)
    score = abs(metrics["current"] - baseline) / max(mad, thresholds["mad_floor"])
    return _result(
        "volume",
        metrics,
        thresholds,
        {"current": metrics["current"], "median": baseline, "mad": mad, "score": score},
        _severity(score, thresholds),
    )


def _ratio(name, metrics, thresholds, numerator, denominator, *, empty="PASS"):
    total = metrics[denominator]
    ratio = metrics[numerator] / total if total else 0
    status = _severity(ratio, thresholds) if total else empty
    return _result(
        name,
        metrics,
        thresholds,
        {numerator: metrics[numerator], denominator: total, "ratio": ratio},
        status,
    )


def evaluate_orphans(metrics, thresholds):
    if not metrics["users_loaded"]:
        return _result("orphans", metrics, thresholds, {}, "WARN", reason="Users are not loaded")
    return _ratio("orphans", metrics, thresholds, "orphans", "events")


def _json_type(value):
    if value is None:
        return "null"
    for value_type, name in (
        (bool, "boolean"),
        (int, "integer"),
        (float, "number"),
        (str, "string"),
        (dict, "object"),
        (list, "array"),
    ):
        if isinstance(value, value_type):
            return name
    return "unknown"


def evaluate_schema_drift(metrics, thresholds):
    schema = metrics["schema"]
    samples = metrics["samples"]
    added, removed, changed = set(), set(), set()
    invalid = 0
    for sample in samples:
        try:
            sample = json.loads(sample) if isinstance(sample, str) else sample
        except (ValueError, TypeError):
            invalid += 1
            continue
        if not isinstance(sample, dict):
            invalid += 1
            continue
        fields = schema["properties"]
        added.update(sample.keys() - fields.keys())
        removed.update(set(schema.get("required", [])) - sample.keys())
        for field in sample.keys() & fields.keys():
            expected = fields[field].get("type")
            allowed = expected if isinstance(expected, list) else [expected]
            actual = _json_type(sample[field])
            if actual not in allowed and not (actual == "integer" and "number" in allowed):
                changed.add(field)
    status = (
        "FAIL" if invalid or removed or changed else ("WARN" if added or not samples else "PASS")
    )
    return _result(
        "schema_drift",
        metrics,
        thresholds,
        {"sample_count": len(samples)},
        status,
        added=sorted(added),
        removed=sorted(removed),
        type_changed=sorted(changed),
        invalid_samples=invalid,
    )


def evaluate_duplicates(metrics, thresholds):
    rows = metrics["rows"]
    duplicates = max(0, rows - metrics["unique_keys"])
    ratio = duplicates / rows if rows else 0
    return _result(
        "duplicates",
        metrics,
        thresholds,
        {"duplicates": duplicates, "ratio": ratio},
        _severity(ratio, thresholds),
    )


def evaluate_dead_letters(metrics, thresholds):
    if metrics["errors"] and not metrics["received"]:
        return _result(
            "dead_letters",
            metrics,
            thresholds,
            {"errors": metrics["errors"], "received": 0, "ratio": None},
            "FAIL",
            reason="Dead letters exist without receipt evidence",
        )
    return _ratio("dead_letters", metrics, thresholds, "errors", "received", empty="WARN")


def evaluate_insert_health(metrics, thresholds):
    status = _severity(metrics["max_parts"], thresholds)
    if metrics["insert_errors"]:
        status = "FAIL"
    elif not metrics["part_log_available"] and status == "PASS":
        status = "WARN"
    return _result("insert_health", metrics, thresholds, dict(metrics), status)


def evaluate_purchase_enrichment(metrics, thresholds):
    return _ratio("purchase_enrichment", metrics, thresholds, "unmatched", "purchases")
