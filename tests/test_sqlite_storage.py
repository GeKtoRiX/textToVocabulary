from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES
from text_to_vocabulary.storage.sqlite_vocabulary_storage import SQLiteVocabularyStorage


def test_sqlite_storage_add_and_get(tmp_path):
    db_path = tmp_path / "vocab.db"
    storage = SQLiteVocabularyStorage(str(db_path))

    assert set(storage.get_categories()) == set(LEXICAL_CATEGORIES)

    added = storage.add_words("noun", ["Cat", "dog", "cat", " "])
    assert added == 2

    words = storage.get_words("noun")
    assert words == ["Cat", "dog"]

    search = storage.get_words("noun", search="ca")
    assert search == ["Cat"]


def test_sqlite_storage_merge_words(tmp_path):
    db_path = tmp_path / "vocab.db"
    storage = SQLiteVocabularyStorage(str(db_path))

    added = storage.merge_words("verb", ["Run"])
    assert added == 1

    added = storage.merge_words("verb", ["run"])
    assert added == 0
