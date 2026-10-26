import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def compose_stack():
    from azki_pipeline.seed import prepare_environment

    prepare_environment(ROOT)
    command = [
        "docker",
        "compose",
        "--progress",
        "quiet",
        "-f",
        str(ROOT / "docker" / "compose.yml"),
    ]
    subprocess.run(
        [*command, "up", "-d", "--build", "--wait", "--wait-timeout", "240"],
        check=True,
        timeout=900,
        cwd=ROOT,
    )
    return command


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.failed and item.get_closest_marker("e2e"):
        logs = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(ROOT / "docker" / "compose.yml"),
                "logs",
                "--tail",
                "100",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        report.sections.append(("Compose logs", logs.stdout + logs.stderr))
