import os
from typing import Iterable

from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES
from text_to_vocabulary.storage.ods_vocabulary_store import (
    read_words_from_ods,
    read_words_from_txt,
)
from text_to_vocabulary.storage.sqlite_vocabulary_storage import SQLiteVocabularyStorage
from text_to_vocabulary.storage.vocabulary_storage import VocabularyStorage


def import_ods_to_sqlite(input_dir: str, db_path: str, *, casefold: bool = True) -> dict:
    storage = SQLiteVocabularyStorage(db_path, casefold=casefold)
    return import_ods_to_storage(input_dir, storage)


def import_ods_to_storage(input_dir: str, storage: VocabularyStorage) -> dict:
    report = {
        "input_dir": input_dir,
        "categories": {},
        "skipped_files": [],
        "errors": [],
        "malformed": {"count": 0, "examples": []},
        "total_added": 0,
    }
    casefold = getattr(storage, "casefold", True)

    for category in LEXICAL_CATEGORIES:
        ods_path = os.path.join(input_dir, f"{category}.ods")
        txt_path = os.path.join(input_dir, f"{category}.txt")

        if os.path.exists(ods_path):
            words, error = _safe_read(ods_path, read_words_from_ods)
            source = "ods"
        elif os.path.exists(txt_path):
            words, error = _safe_read(txt_path, read_words_from_txt)
            source = "txt"
        else:
            continue

        if error:
            report["skipped_files"].append({"path": ods_path if source == "ods" else txt_path})
            report["errors"].append({"path": ods_path if source == "ods" else txt_path, "error": error})
            continue

        cleaned_words, malformed_examples = _normalize_import_words(words, casefold)
        report["malformed"]["count"] += len(malformed_examples)
        if malformed_examples:
            report["malformed"]["examples"].extend(
                _cap_examples(malformed_examples, 5 - len(report["malformed"]["examples"]))
            )

        added = storage.merge_words(category, cleaned_words, source="ods_import")
        report["total_added"] += added

        report["categories"][category] = {
            "source": source,
            "input_count": len(words),
            "added": added,
            "unique_imported": len(cleaned_words),
        }

    return report


def _safe_read(path: str, reader) -> tuple[list[str], str | None]:
    try:
        return reader(path), None
    except Exception as exc:
        return [], str(exc)


def _normalize_import_words(words: Iterable[str], casefold: bool) -> tuple[list[str], list[str]]:
    cleaned = []
    malformed = []
    seen = set()

    for word in words:
        if word is None:
            malformed.append("")
            continue
        if not isinstance(word, str):
            word = str(word)
        display = word.strip()
        if not display:
            malformed.append(word)
            continue
        normalized = display.casefold() if casefold else display
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(display)

    return cleaned, malformed


def _cap_examples(examples: list[str], remaining: int) -> list[str]:
    if remaining <= 0:
        return []
    return examples[:remaining]
