import os
import sqlite3
from dataclasses import dataclass


def quote_identifier(name: str) -> str:
    escaped = name.replace("\"", "\"\"")
    return f"\"{escaped}\""


@dataclass(frozen=True)
class RowIdentifier:
    kind: str  # "pk" or "rowid"
    column: str
    is_rowid_alias: bool = False


class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = None
        self.connection = None
        self.open(db_path)

    def open(self, db_path: str) -> None:
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        self.close()
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        self.db_path = db_path

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None

    def list_tables(self) -> list[str]:
        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_table_info(self, table: str) -> list[dict]:
        cursor = self.connection.execute(f"PRAGMA table_info({quote_identifier(table)})")
        return [dict(row) for row in cursor.fetchall()]

    def get_table_columns(self, table: str) -> list[str]:
        return [col["name"] for col in self.get_table_info(table)]

    def table_has_rowid(self, table: str) -> bool:
        cursor = self.connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        row = cursor.fetchone()
        if not row or row[0] is None:
            return True
        return "WITHOUT ROWID" not in row[0].upper()

    def get_row_identifier(self, table: str) -> RowIdentifier | None:
        table_info = self.get_table_info(table)
        pk_cols = [col for col in table_info if col["pk"]]
        if len(pk_cols) == 1:
            pk_col = pk_cols[0]
            pk_type = (pk_col["type"] or "").upper()
            return RowIdentifier(
                kind="pk",
                column=pk_col["name"],
                is_rowid_alias="INT" in pk_type,
            )
        if self.table_has_rowid(table):
            # When no primary key is present, fall back to rowid if available.
            return RowIdentifier(kind="rowid", column="rowid", is_rowid_alias=False)
        return None

    def count_rows(self, table: str) -> int:
        cursor = self.connection.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(table)}"
        )
        return int(cursor.fetchone()[0])

    def fetch_rows(
        self,
        table: str,
        limit: int,
        offset: int,
        *,
        include_rowid: bool = False,
        order_by: str | None = None,
    ) -> tuple[list[str], list[sqlite3.Row]]:
        columns = self.get_table_columns(table)
        select_cols = [quote_identifier(col) for col in columns]
        if include_rowid:
            select_cols.insert(0, "rowid AS __rowid__")
        sql = f"SELECT {', '.join(select_cols)} FROM {quote_identifier(table)}"
        if order_by:
            sql = f"{sql} ORDER BY {order_by}"
        sql = f"{sql} LIMIT ? OFFSET ?"
        cursor = self.connection.execute(sql, (limit, offset))
        return columns, cursor.fetchall()

    def insert_row(self, table: str, values_by_column: dict) -> int:
        table_info = self.get_table_info(table)
        columns = [col["name"] for col in table_info]
        integer_pk = None
        pk_cols = [col for col in table_info if col["pk"]]
        if len(pk_cols) == 1 and "INT" in (pk_cols[0]["type"] or "").upper():
            integer_pk = pk_cols[0]["name"]

        insert_cols = []
        params = []
        for col in columns:
            value = values_by_column.get(col)
            if col == integer_pk and value is None:
                continue
            insert_cols.append(col)
            params.append(value)

        if not insert_cols:
            cursor = self.connection.execute(
                f"INSERT INTO {quote_identifier(table)} DEFAULT VALUES"
            )
        else:
            placeholders = ", ".join(["?"] * len(insert_cols))
            quoted_cols = ", ".join(quote_identifier(col) for col in insert_cols)
            sql = (
                f"INSERT INTO {quote_identifier(table)} ({quoted_cols}) "
                f"VALUES ({placeholders})"
            )
            cursor = self.connection.execute(sql, params)
        self.connection.commit()
        return cursor.lastrowid

    def update_row(
        self,
        table: str,
        values_by_column: dict,
        identifier: RowIdentifier,
        identifier_value,
    ) -> int:
        table_info = self.get_table_info(table)
        columns = [col["name"] for col in table_info]
        set_cols = []
        params = []
        for col in columns:
            if identifier.kind == "rowid" and col == identifier.column:
                continue
            set_cols.append(col)
            params.append(values_by_column.get(col))

        assignments = ", ".join(f"{quote_identifier(col)} = ?" for col in set_cols)
        where_col = "rowid" if identifier.kind == "rowid" else quote_identifier(identifier.column)
        sql = f"UPDATE {quote_identifier(table)} SET {assignments} WHERE {where_col} = ?"
        params.append(identifier_value)
        cursor = self.connection.execute(sql, params)
        self.connection.commit()
        return cursor.rowcount

    def delete_row(self, table: str, identifier: RowIdentifier, identifier_value) -> int:
        where_col = "rowid" if identifier.kind == "rowid" else quote_identifier(identifier.column)
        sql = f"DELETE FROM {quote_identifier(table)} WHERE {where_col} = ?"
        cursor = self.connection.execute(sql, (identifier_value,))
        self.connection.commit()
        return cursor.rowcount

    def execute_sql(self, sql: str) -> dict:
        cleaned = sql.strip()
        cursor = self.connection.cursor()
        cursor.execute(cleaned)
        if self._is_read_query(cleaned):
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return {"kind": "select", "columns": columns, "rows": rows, "rowcount": len(rows)}
        self.connection.commit()
        return {"kind": "modify", "rowcount": cursor.rowcount}

    @staticmethod
    def _is_read_query(sql: str) -> bool:
        token = sql.lstrip().split(None, 1)[0].lower()
        return token in {"select", "pragma", "with", "explain"}
