import hashlib
import json
import sqlite3


CACHE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_accessed TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    hit_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_llm_cache_last_accessed
    ON llm_cache(last_accessed);
"""


def build_cache_key(signature: dict) -> str:
    payload = json.dumps(
        signature,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class LLMResponseCache:
    def __init__(self, db_path: str, *, max_entries: int = 0):
        self.db_path = db_path
        self.max_entries = max_entries
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(CACHE_SCHEMA_SQL)

    def get(self, cache_key: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT response_json FROM llm_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            if not row:
                return None
            conn.execute(
                """
                UPDATE llm_cache
                SET last_accessed = CURRENT_TIMESTAMP,
                    hit_count = hit_count + 1
                WHERE cache_key = ?
                """,
                (cache_key,),
            )
        try:
            parsed = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def set(self, cache_key: str, response: dict) -> None:
        payload = json.dumps(response, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO llm_cache(cache_key, response_json)
                VALUES (?, ?)
                ON CONFLICT(cache_key)
                DO UPDATE SET
                    response_json = excluded.response_json,
                    last_accessed = CURRENT_TIMESTAMP
                """,
                (cache_key, payload),
            )
            self._prune(conn)

    def _prune(self, conn: sqlite3.Connection) -> None:
        if not self.max_entries or self.max_entries <= 0:
            return
        row = conn.execute("SELECT COUNT(1) FROM llm_cache").fetchone()
        if not row:
            return
        overage = row[0] - self.max_entries
        if overage <= 0:
            return
        conn.execute(
            """
            DELETE FROM llm_cache
            WHERE cache_key IN (
                SELECT cache_key
                FROM llm_cache
                ORDER BY last_accessed ASC
                LIMIT ?
            )
            """,
            (overage,),
        )
