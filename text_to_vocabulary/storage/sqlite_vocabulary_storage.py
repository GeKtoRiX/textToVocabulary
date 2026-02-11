import os
import re
import sqlite3
from typing import Iterable

from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES
from text_to_vocabulary.storage.vocabulary_storage import VocabularyStorage


CATEGORIES_SQL = """
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS words (
    id INTEGER PRIMARY KEY,
    lemma TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS category_words (
    id INTEGER PRIMARY KEY,
    category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    word_id INTEGER NOT NULL REFERENCES words(id) ON DELETE CASCADE,
    surface_form TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    frequency INTEGER NOT NULL DEFAULT 1,
    UNIQUE(category_id, word_id)
);

CREATE INDEX IF NOT EXISTS idx_category_words_category
    ON category_words(category_id);

CREATE INDEX IF NOT EXISTS idx_words_lemma
    ON words(lemma);

CREATE INDEX IF NOT EXISTS idx_category_words_surface
    ON category_words(surface_form);
"""

FTS_SCHEMA_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS words_fts USING fts5(
    lemma,
    surface_form,
    content='',
    tokenize='unicode61',
    prefix='2 3 4'
);
"""

FTS_TRIGGERS_SQL = """
CREATE TRIGGER IF NOT EXISTS category_words_ai
AFTER INSERT ON category_words
BEGIN
    INSERT INTO words_fts(rowid, lemma, surface_form)
    VALUES (
        new.id,
        (SELECT lemma FROM words WHERE id = new.word_id),
        new.surface_form
    );
END;

CREATE TRIGGER IF NOT EXISTS category_words_ad
AFTER DELETE ON category_words
BEGIN
    INSERT INTO words_fts(words_fts, rowid, lemma, surface_form)
    VALUES (
        'delete',
        old.id,
        (SELECT lemma FROM words WHERE id = old.word_id),
        old.surface_form
    );
END;

CREATE TRIGGER IF NOT EXISTS category_words_au
AFTER UPDATE ON category_words
BEGIN
    INSERT INTO words_fts(words_fts, rowid, lemma, surface_form)
    VALUES (
        'delete',
        old.id,
        (SELECT lemma FROM words WHERE id = old.word_id),
        old.surface_form
    );
    INSERT INTO words_fts(rowid, lemma, surface_form)
    VALUES (
        new.id,
        (SELECT lemma FROM words WHERE id = new.word_id),
        new.surface_form
    );
END;

CREATE TRIGGER IF NOT EXISTS words_au
AFTER UPDATE OF lemma ON words
BEGIN
    INSERT INTO words_fts(words_fts, rowid, lemma, surface_form)
    SELECT
        'delete',
        cw.id,
        old.lemma,
        cw.surface_form
    FROM category_words cw
    WHERE cw.word_id = old.id;
    INSERT INTO words_fts(rowid, lemma, surface_form)
    SELECT cw.id, new.lemma, cw.surface_form
    FROM category_words cw
    WHERE cw.word_id = new.id;
END;
"""

MIGRATION_SCHEMA_SQL = """
CREATE TABLE words_new (
    id INTEGER PRIMARY KEY,
    lemma TEXT NOT NULL UNIQUE
);

CREATE TABLE category_words_new (
    id INTEGER PRIMARY KEY,
    category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    word_id INTEGER NOT NULL REFERENCES words_new(id) ON DELETE CASCADE,
    surface_form TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    frequency INTEGER NOT NULL DEFAULT 1,
    UNIQUE(category_id, word_id)
);
"""


class SQLiteVocabularyStorage(VocabularyStorage):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._category_ids: dict[str, int] = {}
        self._fts_available = False
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _load_category_ids(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("SELECT id, name FROM categories").fetchall()
        self._category_ids = {row[1]: row[0] for row in rows}

    def _ensure_schema(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with self._connect() as conn:
            conn.executescript(CATEGORIES_SQL)
            if self._needs_migration(conn):
                self._migrate_schema(conn)
            conn.executescript(SCHEMA_SQL)
            conn.executemany(
                "INSERT OR IGNORE INTO categories(name) VALUES (?)",
                [(category,) for category in LEXICAL_CATEGORIES],
            )
            self._load_category_ids(conn)
            self._ensure_fts(conn)

    def _ensure_fts(self, conn: sqlite3.Connection) -> None:
        existing = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'words_fts'"
        ).fetchone()
        try:
            conn.executescript(FTS_SCHEMA_SQL)
            conn.executescript(FTS_TRIGGERS_SQL)
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "fts5" in message or "no such module" in message:
                self._fts_available = False
                return
            raise
        self._fts_available = True
        if not existing:
            self._rebuild_fts(conn)

    def _rebuild_fts(self, conn: sqlite3.Connection) -> None:
        conn.execute("INSERT INTO words_fts(words_fts) VALUES ('delete-all')")
        conn.execute(
            """
            INSERT INTO words_fts(rowid, lemma, surface_form)
            SELECT cw.id, w.lemma, cw.surface_form
            FROM category_words cw
            JOIN words w ON w.id = cw.word_id
            """
        )

    def _prepare_words(self, words: Iterable[str]) -> list[tuple[str, str]]:
        prepared = []
        seen = set()
        for word in words or []:
            if not isinstance(word, str):
                continue
            cleaned = word.strip()
            if not cleaned:
                continue
            lemma = cleaned.casefold()
            if lemma in seen:
                continue
            seen.add(lemma)
            prepared.append((lemma, cleaned))
        return prepared

    def _get_category_id(self, conn: sqlite3.Connection, category: str) -> int:
        cached = self._category_ids.get(category)
        if cached is not None:
            return cached
        row = conn.execute(
            "SELECT id FROM categories WHERE name = ?",
            (category,),
        ).fetchone()
        if row:
            self._category_ids[category] = row[0]
            return row[0]
        raise ValueError(f"Unknown category: {category}")

    def _fetch_word_ids(
        self, conn: sqlite3.Connection, lemma_values: list[str]
    ) -> dict[str, int]:
        if not lemma_values:
            return {}
        result = {}
        for chunk in _chunked(lemma_values, 900):
            placeholders = ",".join("?" for _ in chunk)
            query = f"SELECT id, lemma FROM words WHERE lemma IN ({placeholders})"
            for row in conn.execute(query, chunk):
                result[row[1]] = row[0]
        return result

    def is_empty(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM category_words LIMIT 1").fetchone()
        return row is None

    def get_categories(self) -> list[str]:
        if self._category_ids:
            return sorted(self._category_ids.keys())
        with self._connect() as conn:
            rows = conn.execute("SELECT name FROM categories ORDER BY name").fetchall()
        return [row[0] for row in rows]

    def get_words_by_category(self) -> dict[str, list[str]]:
        words_by_category = {category: [] for category in LEXICAL_CATEGORIES}
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.name, cw.surface_form, w.lemma
                FROM category_words cw
                JOIN categories c ON c.id = cw.category_id
                JOIN words w ON w.id = cw.word_id
                ORDER BY c.name, cw.surface_form COLLATE NOCASE, w.lemma
                """
            ).fetchall()
        for category, surface_form, _lemma in rows:
            words_by_category.setdefault(category, []).append(surface_form)
        return words_by_category

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
                    if self._fts_available:
                        fts_query = _build_fts_query(normalized)
                        if fts_query:
                            return self._get_words_fts(
                                conn,
                                category_id,
                                fts_query,
                                limit=limit,
                                offset=offset,
                            )
                    search_clause = " AND (w.lemma LIKE ? OR cw.surface_form LIKE ?)"
                    params.extend([f"%{normalized}%", f"%{normalized}%"])

            limit_clause = ""
            if limit is not None:
                limit_clause = " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            elif offset:
                limit_clause = " LIMIT -1 OFFSET ?"
                params.append(offset)

            query = f"""
                SELECT cw.surface_form
                FROM category_words cw
                JOIN words w ON w.id = cw.word_id
                WHERE cw.category_id = ?{search_clause}
                ORDER BY cw.surface_form COLLATE NOCASE, w.lemma
                {limit_clause}
            """
            rows = conn.execute(query, params).fetchall()
        return [row[0] for row in rows]

    def _get_words_fts(
        self,
        conn: sqlite3.Connection,
        category_id: int,
        fts_query: str,
        *,
        limit: int | None,
        offset: int,
    ) -> list[str]:
        params = [category_id, fts_query]
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            limit_clause = " LIMIT -1 OFFSET ?"
            params.append(offset)

        query = f"""
            SELECT cw.surface_form
            FROM words_fts
            JOIN category_words cw ON cw.id = words_fts.rowid
            JOIN words w ON w.id = cw.word_id
            WHERE cw.category_id = ? AND words_fts MATCH ?
            ORDER BY cw.surface_form COLLATE NOCASE, w.lemma
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

    def merge_categories(
        self, category_word_map: dict[str, Iterable[str]], source: str | None = None
    ) -> dict[str, int]:
        if not isinstance(category_word_map, dict):
            raise TypeError("category_word_map must be a dict of category -> words")
        counts = {category: 0 for category in category_word_map}
        if not category_word_map:
            return counts

        prepared_by_category = {}
        all_lemmas = set()
        for category, words in category_word_map.items():
            prepared = self._prepare_words(words)
            if not prepared:
                continue
            prepared_by_category[category] = prepared
            all_lemmas.update(lemma for lemma, _ in prepared)

        if not prepared_by_category:
            return counts

        with self._connect() as conn:
            for category in prepared_by_category:
                self._get_category_id(conn, category)

            conn.executemany(
                "INSERT OR IGNORE INTO words(lemma) VALUES (?)",
                [(lemma,) for lemma in all_lemmas],
            )

            word_ids = self._fetch_word_ids(conn, list(all_lemmas))

            for category, prepared in prepared_by_category.items():
                category_id = self._category_ids[category]
                rows = [
                    (category_id, word_ids[lemma], surface, source)
                    for lemma, surface in prepared
                ]

                insert_cursor = conn.executemany(
                    """
                    INSERT OR IGNORE INTO category_words(
                        category_id, word_id, surface_form, source
                    ) VALUES (?, ?, ?, ?)
                    """,
                    rows,
                )
                counts[category] = max(insert_cursor.rowcount, 0)

                conn.executemany(
                    """
                    UPDATE category_words
                    SET surface_form = COALESCE(NULLIF(surface_form, ''), ?),
                        source = COALESCE(source, ?)
                    WHERE category_id = ? AND word_id = ?
                      AND (surface_form IS NULL OR surface_form = '' OR source IS NULL)
                    """,
                    [
                        (surface, source, category_id, word_ids[lemma])
                        for lemma, surface in prepared
                    ],
                )

        return counts

    def _insert_words(
        self,
        category: str,
        words: Iterable[str],
        *,
        source: str | None,
        update_existing: bool,
    ) -> int:
        with self._connect() as conn:
            return self._insert_words_with_connection(
                conn,
                category,
                words,
                source=source,
                update_existing=update_existing,
            )

    def _insert_words_with_connection(
        self,
        conn: sqlite3.Connection,
        category: str,
        words: Iterable[str],
        *,
        source: str | None,
        update_existing: bool,
    ) -> int:
        prepared = self._prepare_words(words)
        if not prepared:
            return 0

        category_id = self._get_category_id(conn, category)
        conn.executemany(
            "INSERT OR IGNORE INTO words(lemma) VALUES (?)",
            [(lemma,) for lemma, _ in prepared],
        )

        word_ids = self._fetch_word_ids(conn, [lemma for lemma, _ in prepared])
        rows = [
            (category_id, word_ids[lemma], surface, source)
            for lemma, surface in prepared
        ]

        insert_cursor = conn.executemany(
            """
            INSERT OR IGNORE INTO category_words(
                category_id, word_id, surface_form, source
            ) VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        added = max(insert_cursor.rowcount, 0)

        if update_existing:
            conn.executemany(
                """
                UPDATE category_words
                SET surface_form = COALESCE(NULLIF(surface_form, ''), ?),
                    source = COALESCE(source, ?)
                WHERE category_id = ? AND word_id = ?
                  AND (surface_form IS NULL OR surface_form = '' OR source IS NULL)
                """,
                [
                    (surface, source, category_id, word_ids[lemma])
                    for lemma, surface in prepared
                ],
            )

        return added

    def _needs_migration(self, conn: sqlite3.Connection) -> bool:
        words_cols = _table_columns(conn, "words")
        category_cols = _table_columns(conn, "category_words")
        if not words_cols and not category_cols:
            return False
        return "normalized_text" in words_cols or "display_text" in category_cols

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        words_cols = _table_columns(conn, "words")
        category_cols = _table_columns(conn, "category_words")
        if not words_cols:
            return

        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            if not category_cols:
                self._migrate_words_only(conn)
                return

            conn.execute("DROP TABLE IF EXISTS words_new")
            conn.execute("DROP TABLE IF EXISTS category_words_new")
            conn.executescript(MIGRATION_SCHEMA_SQL)

            lemma_col = "normalized_text" if "normalized_text" in words_cols else "lemma"
            surface_col = (
                "display_text" if "display_text" in category_cols else "surface_form"
            )

            conn.execute(
                f"INSERT INTO words_new(id, lemma) SELECT id, {lemma_col} FROM words"
            )
            conn.execute(
                f"""
                INSERT OR IGNORE INTO category_words_new(
                    id, category_id, word_id, surface_form, created_at, source, frequency
                )
                SELECT
                    id,
                    category_id,
                    word_id,
                    {surface_col},
                    created_at,
                    source,
                    COALESCE(frequency, 1)
                FROM category_words
                """
            )

            conn.execute("ALTER TABLE category_words RENAME TO category_words_old")
            conn.execute("ALTER TABLE words RENAME TO words_old")
            conn.execute("ALTER TABLE words_new RENAME TO words")
            conn.execute("ALTER TABLE category_words_new RENAME TO category_words")
            conn.execute("DROP TABLE words_old")
            conn.execute("DROP TABLE category_words_old")
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

    def _migrate_words_only(self, conn: sqlite3.Connection) -> None:
        conn.execute("ALTER TABLE words RENAME TO words_old")
        conn.execute("DROP TABLE IF EXISTS words_new")
        conn.executescript(
            """
            CREATE TABLE words_new (
                id INTEGER PRIMARY KEY,
                lemma TEXT NOT NULL UNIQUE
            );
            """
        )
        conn.execute(
            "INSERT INTO words_new(id, lemma) SELECT id, normalized_text FROM words_old"
        )
        conn.execute("ALTER TABLE words_new RENAME TO words")
        conn.execute("DROP TABLE words_old")


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _count_category_words(conn: sqlite3.Connection, category_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM category_words WHERE category_id = ?",
        (category_id,),
    ).fetchone()
    return row[0] if row else 0


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


_FTS_TOKEN_RE = re.compile(r"[\w']+")
_FTS_MIN_TOKEN_LENGTH = 2


def _build_fts_query(search: str) -> str | None:
    tokens = _FTS_TOKEN_RE.findall(search)
    if not tokens:
        return None
    if any(len(token) < _FTS_MIN_TOKEN_LENGTH for token in tokens):
        return None
    return " AND ".join(f"{token}*" for token in tokens)
