import os
import sqlite3
from typing import Iterable

from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES
from text_to_vocabulary.storage.vocabulary_storage import VocabularyStorage


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS words (
    id INTEGER PRIMARY KEY,
    normalized_text TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS category_words (
    id INTEGER PRIMARY KEY,
    category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    word_id INTEGER NOT NULL REFERENCES words(id) ON DELETE CASCADE,
    display_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    frequency INTEGER NOT NULL DEFAULT 1,
    UNIQUE(category_id, word_id)
);

CREATE INDEX IF NOT EXISTS idx_category_words_category
    ON category_words(category_id);

CREATE INDEX IF NOT EXISTS idx_words_normalized
    ON words(normalized_text);

CREATE INDEX IF NOT EXISTS idx_category_words_display
    ON category_words(display_text);
"""


class SQLiteVocabularyStorage(VocabularyStorage):
    def __init__(self, db_path: str, *, casefold: bool = True):
        self.db_path = db_path
        self.casefold = casefold
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)
            conn.executemany(
                "INSERT OR IGNORE INTO categories(name) VALUES (?)",
                [(category,) for category in LEXICAL_CATEGORIES],
            )

    def _normalize_word(self, word: str) -> tuple[str, str] | None:
        if word is None:
            return None
        if not isinstance(word, str):
            word = str(word)
        display = word.strip()
        if not display:
            return None
        normalized = display.casefold() if self.casefold else display
        return normalized, display

    def _prepare_words(self, words: Iterable[str]) -> list[tuple[str, str]]:
        prepared = []
        seen = set()
        for word in words or []:
            normalized_pair = self._normalize_word(word)
            if not normalized_pair:
                continue
            normalized, display = normalized_pair
            if normalized in seen:
                continue
            seen.add(normalized)
            prepared.append((normalized, display))
        return prepared

    def _get_category_id(self, conn: sqlite3.Connection, category: str) -> int:
        row = conn.execute(
            "SELECT id FROM categories WHERE name = ?",
            (category,),
        ).fetchone()
        if row:
            return row[0]
        raise ValueError(f"Unknown category: {category}")

    def _fetch_word_ids(
        self, conn: sqlite3.Connection, normalized_values: list[str]
    ) -> dict[str, int]:
        if not normalized_values:
            return {}
        result = {}
        for chunk in _chunked(normalized_values, 900):
            placeholders = ",".join("?" for _ in chunk)
            query = f"SELECT id, normalized_text FROM words WHERE normalized_text IN ({placeholders})"
            for row in conn.execute(query, chunk):
                result[row[1]] = row[0]
        return result

    def is_empty(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM category_words LIMIT 1").fetchone()
        return row is None

    def get_categories(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT name FROM categories ORDER BY name").fetchall()
        return [row[0] for row in rows]

    def get_words(
        self,
        category: str,
        *,
        search: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[str]:
        with self._connect() as conn:
            category_id = self._get_category_id(conn, category)
            params = [category_id]
            search_clause = ""
            if search:
                normalized = search.strip()
                if normalized:
                    normalized = normalized.casefold() if self.casefold else normalized
                    search_clause = " AND (w.normalized_text LIKE ? OR cw.display_text LIKE ?)"
                    params.extend([f"%{normalized}%", f"%{search.strip()}%"])

            limit_clause = ""
            if limit is not None:
                limit_clause = " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            elif offset:
                limit_clause = " LIMIT -1 OFFSET ?"
                params.append(offset)

            query = f"""
                SELECT cw.display_text
                FROM category_words cw
                JOIN words w ON w.id = cw.word_id
                WHERE cw.category_id = ?{search_clause}
                ORDER BY cw.display_text COLLATE NOCASE, w.normalized_text
                {limit_clause}
            """
            rows = conn.execute(query, params).fetchall()
        return [row[0] for row in rows]

    def add_words(self, category: str, words: Iterable[str], source: str | None = None) -> int:
        return self._insert_words(category, words, source=source, update_existing=False)

    def merge_words(
        self, category: str, words: Iterable[str], source: str | None = None
    ) -> int:
        return self._insert_words(category, words, source=source, update_existing=True)

    def _insert_words(
        self,
        category: str,
        words: Iterable[str],
        *,
        source: str | None,
        update_existing: bool,
    ) -> int:
        prepared = self._prepare_words(words)
        if not prepared:
            return 0

        with self._connect() as conn:
            category_id = self._get_category_id(conn, category)
            conn.executemany(
                "INSERT OR IGNORE INTO words(normalized_text) VALUES (?)",
                [(normalized,) for normalized, _ in prepared],
            )

            word_ids = self._fetch_word_ids(conn, [normalized for normalized, _ in prepared])
            rows = [
                (category_id, word_ids[normalized], display, source)
                for normalized, display in prepared
            ]

            before = conn.total_changes
            conn.executemany(
                """
                INSERT OR IGNORE INTO category_words(
                    category_id, word_id, display_text, source
                ) VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            added = conn.total_changes - before

            if update_existing:
                conn.executemany(
                    """
                    UPDATE category_words
                    SET display_text = COALESCE(NULLIF(display_text, ''), ?),
                        source = COALESCE(source, ?)
                    WHERE category_id = ? AND word_id = ?
                    """,
                    [
                        (display, source, category_id, word_ids[normalized])
                        for normalized, display in prepared
                    ],
                )

        return added


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]
