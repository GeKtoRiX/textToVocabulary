import os
import zipfile
from xml.etree import ElementTree as ET

from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES, dedupe_preserve_order
from text_to_vocabulary.storage.vocabulary_cache import VocabularyCache


ODS_MIMETYPE = "application/vnd.oasis.opendocument.spreadsheet"

NS_OFFICE = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
NS_TABLE = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
NS_TEXT = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
NS_MANIFEST = "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"

ET.register_namespace("office", NS_OFFICE)
ET.register_namespace("table", NS_TABLE)
ET.register_namespace("text", NS_TEXT)
ET.register_namespace("manifest", NS_MANIFEST)


def read_words_from_txt(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def read_words_from_ods(path):
    if not os.path.exists(path):
        return []

    with zipfile.ZipFile(path, "r") as archive:
        try:
            with archive.open("content.xml") as content_xml:
                return _read_words_from_ods_stream(content_xml)
        except KeyError:
            return []


def _read_words_from_ods_stream(content_xml):
    words = []
    row_tag = f"{{{NS_TABLE}}}table-row"
    cell_tag = f"{{{NS_TABLE}}}table-cell"
    text_tag = f"{{{NS_TEXT}}}p"

    for event, elem in ET.iterparse(content_xml, events=("end",)):
        if elem.tag != row_tag:
            continue
        cell = elem.find(cell_tag)
        if cell is None:
            elem.clear()
            continue
        text_nodes = cell.findall(text_tag)
        if text_nodes:
            value = "\n".join("".join(node.itertext()) for node in text_nodes)
        else:
            value = "".join(cell.itertext())
        value = value.strip()
        if value:
            words.append(value)
        elem.clear()

    return words


def build_content_xml(words):
    doc = ET.Element(
        f"{{{NS_OFFICE}}}document-content",
        {f"{{{NS_OFFICE}}}version": "1.2"},
    )
    body = ET.SubElement(doc, f"{{{NS_OFFICE}}}body")
    spreadsheet = ET.SubElement(body, f"{{{NS_OFFICE}}}spreadsheet")
    table = ET.SubElement(
        spreadsheet,
        f"{{{NS_TABLE}}}table",
        {f"{{{NS_TABLE}}}name": "Words"},
    )

    for word in words:
        row = ET.SubElement(table, f"{{{NS_TABLE}}}table-row")
        cell = ET.SubElement(
            row,
            f"{{{NS_TABLE}}}table-cell",
            {f"{{{NS_OFFICE}}}value-type": "string"},
        )
        entry = ET.SubElement(cell, f"{{{NS_TEXT}}}p")
        entry.text = word

    return ET.tostring(doc, encoding="utf-8", xml_declaration=True)


def build_content_xml_rows(rows):
    doc = ET.Element(
        f"{{{NS_OFFICE}}}document-content",
        {f"{{{NS_OFFICE}}}version": "1.2"},
    )
    body = ET.SubElement(doc, f"{{{NS_OFFICE}}}body")
    spreadsheet = ET.SubElement(body, f"{{{NS_OFFICE}}}spreadsheet")
    table = ET.SubElement(
        spreadsheet,
        f"{{{NS_TABLE}}}table",
        {f"{{{NS_TABLE}}}name": "Vocabulary"},
    )

    for row_values in rows:
        row = ET.SubElement(table, f"{{{NS_TABLE}}}table-row")
        for value in row_values:
            cell = ET.SubElement(
                row,
                f"{{{NS_TABLE}}}table-cell",
                {f"{{{NS_OFFICE}}}value-type": "string"},
            )
            entry = ET.SubElement(cell, f"{{{NS_TEXT}}}p")
            entry.text = "" if value is None else str(value)

    return ET.tostring(doc, encoding="utf-8", xml_declaration=True)


def build_styles_xml():
    doc = ET.Element(
        f"{{{NS_OFFICE}}}document-styles",
        {f"{{{NS_OFFICE}}}version": "1.2"},
    )
    ET.SubElement(doc, f"{{{NS_OFFICE}}}styles")
    return ET.tostring(doc, encoding="utf-8", xml_declaration=True)


def build_manifest_xml():
    manifest = ET.Element(
        f"{{{NS_MANIFEST}}}manifest",
        {f"{{{NS_MANIFEST}}}version": "1.2"},
    )
    ET.SubElement(
        manifest,
        f"{{{NS_MANIFEST}}}file-entry",
        {
            f"{{{NS_MANIFEST}}}media-type": ODS_MIMETYPE,
            f"{{{NS_MANIFEST}}}full-path": "/",
        },
    )
    ET.SubElement(
        manifest,
        f"{{{NS_MANIFEST}}}file-entry",
        {
            f"{{{NS_MANIFEST}}}media-type": "text/xml",
            f"{{{NS_MANIFEST}}}full-path": "content.xml",
        },
    )
    ET.SubElement(
        manifest,
        f"{{{NS_MANIFEST}}}file-entry",
        {
            f"{{{NS_MANIFEST}}}media-type": "text/xml",
            f"{{{NS_MANIFEST}}}full-path": "styles.xml",
        },
    )
    return ET.tostring(manifest, encoding="utf-8", xml_declaration=True)


def write_words_to_ods(path, words):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    content_xml = build_content_xml(words)
    styles_xml = build_styles_xml()
    manifest_xml = build_manifest_xml()

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        mimetype_info = zipfile.ZipInfo("mimetype")
        mimetype_info.compress_type = zipfile.ZIP_STORED
        archive.writestr(mimetype_info, ODS_MIMETYPE)
        archive.writestr("content.xml", content_xml)
        archive.writestr("styles.xml", styles_xml)
        archive.writestr("META-INF/manifest.xml", manifest_xml)


def write_rows_to_ods(path, rows, headers=None):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    all_rows = []
    if headers:
        all_rows.append(headers)
    all_rows.extend(rows)

    content_xml = build_content_xml_rows(all_rows)
    styles_xml = build_styles_xml()
    manifest_xml = build_manifest_xml()

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        mimetype_info = zipfile.ZipInfo("mimetype")
        mimetype_info.compress_type = zipfile.ZIP_STORED
        archive.writestr(mimetype_info, ODS_MIMETYPE)
        archive.writestr("content.xml", content_xml)
        archive.writestr("styles.xml", styles_xml)
        archive.writestr("META-INF/manifest.xml", manifest_xml)


def _load_words(path, cache, loader):
    if cache is None:
        return loader(path)
    return cache.get_words(path, loader)


def _load_existing_words(path, cache):
    ods_exists = os.path.exists(path)
    if ods_exists:
        return "ods", _load_words(path, cache, read_words_from_ods), ods_exists

    txt_path = os.path.splitext(path)[0] + ".txt"
    if os.path.exists(txt_path):
        return "txt", _load_words(txt_path, cache, read_words_from_txt), ods_exists

    return None, [], ods_exists


def append_missing_words(path, words, cache=None):
    extension = os.path.splitext(path)[1].lower()
    if extension != ".ods":
        return append_missing_words_txt(path, words, cache=cache)

    source, existing_words, ods_exists = _load_existing_words(path, cache)

    existing_set = {word.strip() for word in existing_words if word.strip()}
    cleaned = [word.strip() for word in words if word is not None]
    new_words = [word for word in cleaned if word and word not in existing_set]

    if not new_words and not (existing_words and source == "txt" and not ods_exists):
        return 0

    updated_words = existing_words + new_words
    write_words_to_ods(path, updated_words)
    if cache is not None:
        cache.update_words(path, updated_words)

    return len(new_words)


def append_missing_words_txt(path, words, cache=None):
    existing_words = []
    if os.path.exists(path):
        existing_words = _load_words(path, cache, read_words_from_txt)

    existing_set = {word.strip() for word in existing_words if word.strip()}
    cleaned = [word.strip() for word in words if word is not None]
    new_words = [word for word in cleaned if word and word not in existing_set]
    if not new_words:
        return 0

    with open(path, "a", encoding="utf-8") as handle:
        for word in new_words:
            handle.write(f"{word}\n")

    if cache is not None:
        cache.update_words(path, existing_words + new_words)

    return len(new_words)


def write_vocabulary_exports(output_dir, data, cache=None):
    os.makedirs(output_dir, exist_ok=True)

    mapping = {key: f"{key}.ods" for key in LEXICAL_CATEGORIES}

    saved_files = {}
    added_counts = {}

    for key, filename in mapping.items():
        path = os.path.join(output_dir, filename)
        saved_files[key] = path
        words = dedupe_preserve_order(data.get(key, []))
        if not words:
            txt_path = os.path.splitext(path)[0] + ".txt"
            if not os.path.exists(path) and os.path.exists(txt_path):
                added_counts[key] = append_missing_words(path, words, cache=cache)
            else:
                added_counts[key] = 0
            continue
        added_counts[key] = append_missing_words(path, words, cache=cache)

    return saved_files, added_counts
