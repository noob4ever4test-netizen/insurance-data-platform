from datetime import UTC, datetime


def insert_order(client, database, order):
    row = {key: value for key, value in order.items() if key != "product_type"}
    row["created_at"] = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=UTC
    )
    client.insert(
        f"{database}.{order['product_type']}_orders", [list(row.values())], column_names=list(row)
    )


def insert_financial(client, database, financial):
    row = dict(financial)
    row["created_at"] = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=UTC
    )
    client.insert(f"{database}.financial_order", [list(row.values())], column_names=list(row))


def reload_orders(client, database):
    client.command(f"SYSTEM RELOAD DICTIONARY {database}.purchase_orders_dict")


def refresh_purchases(client, databases):
    reload_orders(client, databases["core"])
    client.command(f"SYSTEM START VIEW {databases['mart']}.purchase_events_repair_mv")
    client.command(f"SYSTEM REFRESH VIEW {databases['mart']}.purchase_events_repair_mv")
    client.command(f"SYSTEM WAIT VIEW {databases['mart']}.purchase_events_repair_mv")
    client.command(f"SYSTEM STOP VIEW {databases['mart']}.purchase_events_repair_mv")


def active_purchases(client, database):
    return client.query(
        f"SELECT event_key, order_id, product_type, paid_amount, policy_no "
        f"FROM {database}.purchase_events_enriched FINAL "
        "WHERE _is_deleted = 0 ORDER BY event_key"
    ).result_rows
