import os

from text_to_vocabulary.storage.ods_exporter import export_sqlite_to_ods
from text_to_vocabulary.storage.ods_importer import import_ods_to_sqlite
from text_to_vocabulary.storage.ods_vocabulary_store import (
    read_words_from_ods,
    write_words_to_ods,
)
from text_to_vocabulary.storage.sqlite_vocabulary_storage import SQLiteVocabularyStorage


def test_import_ods_to_sqlite(tmp_path):
    input_dir = tmp_path / "legacy"
    input_dir.mkdir()
    write_words_to_ods(str(input_dir / "noun.ods"), ["cat", "dog", "cat"])
    write_words_to_ods(str(input_dir / "verb.ods"), ["run"])

    db_path = tmp_path / "vocab.db"
    report = import_ods_to_sqlite(str(input_dir), str(db_path))

    storage = SQLiteVocabularyStorage(str(db_path))
    assert storage.get_words("noun") == ["cat", "dog"]
    assert storage.get_words("verb") == ["run"]
    assert report["categories"]["noun"]["added"] == 2


def test_export_sqlite_to_ods_per_category(tmp_path):
    db_path = tmp_path / "vocab.db"
    storage = SQLiteVocabularyStorage(str(db_path))
    storage.add_words("noun", ["cat", "dog"])
    storage.add_words("verb", ["run"])

    output_dir = tmp_path / "exports"
    result = export_sqlite_to_ods(str(db_path), str(output_dir), mode="per_category")

    noun_path = result["files"]["noun"]
    verb_path = result["files"]["verb"]

    assert os.path.exists(noun_path)
    assert os.path.exists(verb_path)
    assert read_words_from_ods(noun_path) == ["cat", "dog"]
    assert read_words_from_ods(verb_path) == ["run"]


def test_export_sqlite_to_ods_consolidated(tmp_path):
    db_path = tmp_path / "vocab.db"
    storage = SQLiteVocabularyStorage(str(db_path))
    storage.add_words("noun", ["cat"])

    output_dir = tmp_path / "exports"
    result = export_sqlite_to_ods(str(db_path), str(output_dir), mode="consolidated")

    consolidated_path = result["files"]["consolidated"]
    assert os.path.exists(consolidated_path)
