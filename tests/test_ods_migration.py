import os
import zipfile
from xml.etree import ElementTree as ET

from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES
from text_to_vocabulary.storage.ods_exporter import (
    export_sqlite_to_ods,
    export_storage_to_single_file,
)
from text_to_vocabulary.storage.ods_importer import import_ods_to_sqlite
from text_to_vocabulary.storage.ods_vocabulary_store import (
    NS_TABLE,
    NS_TEXT,
    read_words_from_ods,
    write_words_to_ods,
)
from text_to_vocabulary.storage.sqlite_vocabulary_storage import SQLiteVocabularyStorage


def _read_rows_from_ods(path):
    with zipfile.ZipFile(path, "r") as archive:
        with archive.open("content.xml") as content_xml:
            tree = ET.parse(content_xml)

    row_tag = f"{{{NS_TABLE}}}table-row"
    cell_tag = f"{{{NS_TABLE}}}table-cell"
    text_tag = f"{{{NS_TEXT}}}p"

    rows = []
    for row_elem in tree.getroot().iter(row_tag):
        row_values = []
        for cell in row_elem.findall(cell_tag):
            text_nodes = cell.findall(text_tag)
            if text_nodes:
                value = "\n".join("".join(node.itertext()) for node in text_nodes)
            else:
                value = "".join(cell.itertext())
            row_values.append(value)
        rows.append(row_values)
    return rows


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


def test_export_sqlite_to_ods_single_columns(tmp_path):
    db_path = tmp_path / "vocab.db"
    storage = SQLiteVocabularyStorage(str(db_path))
    storage.add_words("noun", ["cat", "dog"])
    storage.add_words("verb", ["run"])

    output_path = tmp_path / "all.ods"
    result = export_storage_to_single_file(storage, str(output_path))

    single_path = result["files"]["single"]
    assert os.path.exists(single_path)

    rows = _read_rows_from_ods(single_path)
    assert rows[0] == LEXICAL_CATEGORIES
    assert len(rows) == 3

    noun_index = LEXICAL_CATEGORIES.index("noun")
    verb_index = LEXICAL_CATEGORIES.index("verb")
    assert rows[1][noun_index] == "cat"
    assert rows[1][verb_index] == "run"
    assert rows[2][noun_index] == "dog"
    assert rows[2][verb_index] == ""
