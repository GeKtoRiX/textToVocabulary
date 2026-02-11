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
    context_limit=None,
    max_output_tokens=None,
    token_safety_margin=None,
    auto_import_ods=True,
    cache=None,
):
    analysis = request_vocabulary_analysis(
        endpoint,
        model,
        text,
        temperature=temperature,
        system_prompt=system_prompt,
        context_limit=context_limit,
        max_output_tokens=max_output_tokens,
        token_safety_margin=token_safety_margin,
        cache=cache,
    )
    if not isinstance(storage, VocabularyStorage):
        raise TypeError("storage must implement VocabularyStorage")

    migration_report = None
    if auto_import_ods and storage.is_empty():
        migration_report = import_ods_to_storage(output_dir, storage)

    category_words = {category: analysis.get(category, []) for category in LEXICAL_CATEGORIES}
    bulk_merge = getattr(storage, "merge_categories", None)
    if callable(bulk_merge):
        added_counts = bulk_merge(category_words, source="llm")
    else:
        added_counts = {}
        for category, words in category_words.items():
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
