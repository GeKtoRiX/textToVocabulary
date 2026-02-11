import os

from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES
from text_to_vocabulary.storage.ods_vocabulary_store import (
    write_rows_to_ods,
    write_words_to_ods,
)
from text_to_vocabulary.storage.sqlite_vocabulary_storage import SQLiteVocabularyStorage
from text_to_vocabulary.storage.vocabulary_storage import VocabularyStorage


def export_sqlite_to_ods(
    db_path: str,
    output_dir: str,
    *,
    mode: str = "per_category",
    consolidated_name: str = "vocabulary_all.ods",
) -> dict:
    storage = SQLiteVocabularyStorage(db_path)
    return export_storage_to_ods(
        storage,
        output_dir,
        mode=mode,
        consolidated_name=consolidated_name,
    )


def export_storage_to_ods(
    storage: VocabularyStorage,
    output_dir: str,
    *,
    mode: str = "per_category",
    consolidated_name: str = "vocabulary_all.ods",
) -> dict:
    os.makedirs(output_dir, exist_ok=True)

    if mode not in {"per_category", "consolidated"}:
        raise ValueError("mode must be 'per_category' or 'consolidated'")

    if mode == "per_category":
        return _export_per_category(storage, output_dir)

    return _export_consolidated(storage, output_dir, consolidated_name)


def _get_words_map(storage: VocabularyStorage) -> dict[str, list[str]]:
    getter = getattr(storage, "get_words_by_category", None)
    if callable(getter):
        return getter()
    return {category: storage.get_words(category) for category in LEXICAL_CATEGORIES}


def _export_per_category(storage: VocabularyStorage, output_dir: str) -> dict:
    saved_files = {}
    words_by_category = _get_words_map(storage)
    for category in LEXICAL_CATEGORIES:
        words = words_by_category.get(category, [])
        path = os.path.join(output_dir, f"{category}.ods")
        write_words_to_ods(path, words)
        saved_files[category] = path
    return {"mode": "per_category", "files": saved_files}


def _export_consolidated(
    storage: VocabularyStorage, output_dir: str, consolidated_name: str
) -> dict:
    rows = []
    words_by_category = _get_words_map(storage)
    for category in LEXICAL_CATEGORIES:
        words = words_by_category.get(category, [])
        rows.extend([(category, word) for word in words])

    path = os.path.join(output_dir, consolidated_name)
    write_rows_to_ods(path, rows, headers=["category", "word"])
    return {"mode": "consolidated", "files": {"consolidated": path}}
