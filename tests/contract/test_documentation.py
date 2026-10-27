import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.contract


def test_C12_report_links_resolve_and_phase_evidence_is_preserved():
    report = ROOT / "docs/report.md"
    links = re.findall(r"\[[^\]]+\]\(([^)]+)\)", report.read_text())

    assert links, "Report must link its architecture, operating guide and evidence"
    for target in links:
        assert (report.parent / target).is_file(), target
    notes = ROOT / "docs/implementation-notes.md"
    assert notes.is_file(), "Preserve the phase-by-phase evidence outside the concise report"
    assert "Phase 7" in notes.read_text()


@pytest.mark.parametrize(
    "source,target",
    [
        ("CSV", "CONSUMERS"),
        ("MYSQL", "CONSUMERS"),
        ("RAW", "DQ"),
        ("CORE", "DQ_RUNNER"),
        ("DQ_RUNNER", "DQ"),
        ("CSV", "SPARK"),
        ("KAFKA", "SPARK"),
        ("SPARK", "MART"),
        ("SPARK", "DQ"),
    ],
)
def test_C12_architecture_has_connected_ingestion_quality_and_recovery_paths(source, target):
    diagram = (ROOT / "docs/architecture.mmd").read_text()
    nodes = set(re.findall(r'^\s*(\w+)\["', diagram, re.MULTILINE))
    edges = re.findall(
        r"^\s*(\w+)\s+(?:-->|-\.->)(?:\|[^|]+\|)?\s+(\w+)\s*$", diagram, re.MULTILINE
    )
    assert source in nodes and target in nodes, (source, target)
    assert all(left in nodes and right in nodes for left, right in edges)
    visited = set()
    pending = [source]
    while pending:
        current = pending.pop()
        if current not in visited:
            visited.add(current)
            pending.extend(right for left, right in edges if left == current)

    assert target in visited, f"Missing architecture path: {source} -> {target}"
