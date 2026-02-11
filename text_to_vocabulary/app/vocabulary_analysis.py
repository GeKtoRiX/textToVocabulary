from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES, format_markdown_table
from text_to_vocabulary.integrations.llm_client import request_vocabulary_analysis
from text_to_vocabulary.storage.ods_exporter import export_storage_to_ods
from text_to_vocabulary.storage.ods_importer import import_ods_to_storage
from text_to_vocabulary.storage.vocabulary_storage import VocabularyStorage


def analyze_and_store(
    text,
    *,
    endpoint,
    model,
    output_dir,
    storage,
    temperature=0.2,
    system_prompt=None,
    auto_import_ods=True,
):
    analysis = request_vocabulary_analysis(
        endpoint,
        model,
        text,
        temperature=temperature,
        system_prompt=system_prompt,
    )
    if not isinstance(storage, VocabularyStorage):
        raise TypeError("storage must implement VocabularyStorage")

    migration_report = None
    if auto_import_ods and storage.is_empty():
        migration_report = import_ods_to_storage(output_dir, storage)

    added_counts = {}
    for category in LEXICAL_CATEGORIES:
        words = analysis.get(category, [])
        added_counts[category] = storage.merge_words(category, words, source="llm")

    table = analysis.get("table") or format_markdown_table(analysis)
    return analysis, added_counts, table, migration_report


def export_vocabulary(
    *,
    storage,
    output_dir,
    export_mode="per_category",
    consolidated_export_name="vocabulary_all.ods",
):
    if not isinstance(storage, VocabularyStorage):
        raise TypeError("storage must implement VocabularyStorage")
    if storage.is_empty():
        raise ValueError("No vocabulary data to export.")

    result = export_storage_to_ods(
        storage,
        output_dir,
        mode=export_mode,
        consolidated_name=consolidated_export_name,
    )
    if result["mode"] == "per_category":
        message = f"Exported ODS files to {output_dir}"
    else:
        message = f"Exported consolidated file to {result['files']['consolidated']}"
    return result, message
