import sqlite3

from text_to_vocabulary.db_manager import DatabaseManager


def test_db_manager_crud_and_sql(tmp_path):
    db_path = tmp_path / "items.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER)"
    )
    conn.executemany(
        "INSERT INTO items (name, qty) VALUES (?, ?)",
        [("Apple", 2), ("Banana", 5)],
    )
    conn.commit()
    conn.close()

    manager = DatabaseManager(str(db_path))
    try:
        assert manager.list_tables() == ["items"]

        info = manager.get_table_info("items")
        assert [col["name"] for col in info] == ["id", "name", "qty"]

        identifier = manager.get_row_identifier("items")
        assert identifier is not None
        assert identifier.kind == "pk"
        assert identifier.column == "id"

        assert manager.count_rows("items") == 2

        columns, rows = manager.fetch_rows("items", limit=1, offset=0, order_by="id")
        assert columns == ["id", "name", "qty"]
        assert rows[0]["name"] == "Apple"

        new_id = manager.insert_row(
            "items",
            {"id": None, "name": "Cherry", "qty": 3},
        )
        assert new_id is not None
        assert manager.count_rows("items") == 3

        updated = manager.update_row(
            "items",
            {"id": 1, "name": "Apple", "qty": 7},
            identifier,
            1,
        )
        assert updated == 1

        deleted = manager.delete_row("items", identifier, 2)
        assert deleted == 1
        assert manager.count_rows("items") == 2

        result = manager.execute_sql("SELECT name FROM items ORDER BY id")
        assert result["kind"] == "select"
        assert result["columns"] == ["name"]
        assert [row[0] for row in result["rows"]] == ["Apple", "Cherry"]

        modify = manager.execute_sql("UPDATE items SET qty = qty + 1")
        assert modify["kind"] == "modify"
        assert modify["rowcount"] == 2
    finally:
        manager.close()


def test_db_manager_rowid_fallback(tmp_path):
    db_path = tmp_path / "rowid.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE logs (message TEXT NOT NULL)")
    conn.executemany("INSERT INTO logs (message) VALUES (?)", [("First",), ("Second",)])
    conn.commit()
    conn.close()

    manager = DatabaseManager(str(db_path))
    try:
        identifier = manager.get_row_identifier("logs")
        assert identifier is not None
        assert identifier.kind == "rowid"

        columns, rows = manager.fetch_rows(
            "logs",
            limit=2,
            offset=0,
            include_rowid=True,
            order_by="rowid",
        )
        assert columns == ["message"]
        rowid_value = rows[0]["__rowid__"]

        updated = manager.update_row(
            "logs",
            {"message": "First updated"},
            identifier,
            rowid_value,
        )
        assert updated == 1

        deleted = manager.delete_row("logs", identifier, rowid_value)
        assert deleted == 1
    finally:
        manager.close()
