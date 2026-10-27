from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from azki_pipeline.sql_runner import load_sql

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.contract
def test_C08_governance_uses_masked_views_and_explicit_safe_grants():
    queries = load_sql(ROOT / "sql/070_governance.sql")

    assert queries, "Phase 5 governance SQL is missing"
    grants = [query for query in queries if query.startswith("GRANT")]
    analyst = [query for query in grants if "TO role_analyst" in query]
    assert analyst
    for grant in analyst:
        assert "WITH GRANT OPTION" not in grant
        if "ON gov." not in grant:
            assert "ON system.settings" in grant
            assert "SELECT(" in grant
            assert not any(
                name in grant
                for name in ("national_id", "phone", "plate_no", "policy_no", "product_attributes")
            )
    for role in ("role_admin", "role_engineer", "role_analyst", "role_partner_1", "role_partner_2"):
        assert f"CREATE ROLE IF NOT EXISTS {role}" in queries
    views = [sqlglot.parse_one(query, "clickhouse") for query in queries if "CREATE VIEW" in query]
    assert views
    assert all(not list(view.find_all(exp.Star)) for view in views)
    assert any("SQL SECURITY DEFINER" in query for query in queries)
    assert any("CREATE ROW POLICY" in query for query in queries)
    assert any("CREATE SETTINGS PROFILE" in query for query in queries)
    assert any("CREATE QUOTA" in query for query in queries)
