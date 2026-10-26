from unittest.mock import Mock

import pytest


@pytest.mark.unit
@pytest.mark.parametrize("partition", [202500, 202513, "202510;DROP TABLE core.users", -1])
def test_U31_rebuild_rejects_invalid_partition_before_sql(partition):
    from azki_pipeline.reconcile import rebuild_aggregate_partition

    client = Mock()

    with pytest.raises(ValueError, match="partition"):
        rebuild_aggregate_partition(client, partition)

    client.command.assert_not_called()


@pytest.mark.unit
def test_U31_rebuild_uses_deduped_source_and_atomic_partition_replace():
    from azki_pipeline.reconcile import rebuild_aggregate_partition

    client = Mock()
    client.query.return_value.result_rows = [(1,)]

    rebuild_aggregate_partition(client, 202510, database_overrides={"core": "core_test"})

    statements = [call.args[0] for call in client.command.call_args_list]
    assert len(statements) == 3
    assert statements[0].startswith("TRUNCATE TABLE mart.user_event_agg_rebuild")
    assert "FROM core_test.user_events FINAL" in statements[1]
    assert "REPLACE PARTITION" in statements[2]
    assert all(
        call.kwargs["parameters"] == {"partition": 202510} for call in client.command.call_args_list
    )


@pytest.mark.unit
def test_U31_empty_source_drops_obsolete_aggregate_partition():
    from azki_pipeline.reconcile import rebuild_aggregate_partition

    client = Mock()
    client.query.return_value.result_rows = [(0,)]

    rebuild_aggregate_partition(client, 202510)

    assert "DROP PARTITION" in client.command.call_args.args[0]


@pytest.mark.unit
def test_U31_invalid_database_override_never_reaches_server():
    from azki_pipeline.reconcile import rebuild_aggregate_partition

    client = Mock()
    client.query.return_value.result_rows = [(1,)]

    with pytest.raises(ValueError, match="database"):
        rebuild_aggregate_partition(client, 202510, database_overrides={"core": "bad;DROP core"})

    client.command.assert_not_called()
