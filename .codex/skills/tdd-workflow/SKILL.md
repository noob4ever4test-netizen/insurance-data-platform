---
name: tdd-workflow
description: Red-green-refactor loop for this repo. Use for every coding task.
---
# TDD workflow

## Loop
1. Pick test IDs from `docs/TEST_PLAN.md` for the current phase.
2. Write the tests. Run only them (`pytest -k "U03 or U04"`). Paste the failure summary. A failure must be an
   assertion or missing-symbol error — not a syntax error or import typo in the test itself.
3. Implement the minimum to pass. Re-run. Paste the pass summary.
4. Refactor; re-run the whole layer (`make test-unit` etc.).
5. Update TEST_PLAN.md status column (`todo → red → green`).

## Test-writing rules
- One behaviour per test; Arrange / Act / Assert with blank lines between.
- Name: `test_<ID>_<behaviour>`; mark with `@pytest.mark.<layer>`.
- Build data with factories in `tests/helpers/factories.py` (`make_event(**overrides)`, `make_user(...)`), never inline dicts copied across tests.
- Use `pytest.mark.parametrize` for edge-case tables (nulls, bad timestamps, unknown event_type).
- Assert exact values for aggregates — compute expected values in the test from the fixture, not by hard-coding magic numbers.
- Integration/e2e: unique database or topic name per test (uuid suffix) so tests are independent and parallel-safe.

## Definition of done for a phase
All phase tests green · `make lint` clean · no skipped tests without a linked reason · short summary of decisions in `docs/report.md` notes section.
