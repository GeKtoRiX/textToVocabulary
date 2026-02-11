import json
import os


SETTINGS_FILENAME = "settings.json"
DEFAULT_OUTPUT_DIRNAME = "exports"
DEFAULT_CONTEXT_LIMIT = 16384
DEFAULT_MAX_OUTPUT_TOKENS = 16000
DEFAULT_TOKEN_SAFETY_MARGIN = 200
DEFAULT_LLM_CACHE_ENABLED = True
DEFAULT_LLM_CACHE_MAX_ENTRIES = 500

DEFAULT_SYSTEM_PROMPT = """You are a linguistic analysis assistant.

Analyze the input text written in English and extract all unique lexical units (single words and multi-word units), then classify them by part of speech according to their function in the given context.

------------------------
Linguistic Processing Rules
------------------------

1. Language
   - Assume the input text is in English.

2. Tokenization
   - Expand contractions (e.g., "don't" -> "do", "not").
   - Treat hyphenated compounds as a single lexical unit if they function as one item (e.g., "state-of-the-art").
   - Ignore punctuation marks unless they form part of a lexical unit.

3. Normalization
   - Convert all single-word lexical units to their base dictionary form (lemma).
   - Convert all common nouns to singular form.
   - Do NOT singularize pluralia tantum nouns (use standard English dictionary conventions, e.g., "scissors", "pants", "glasses").
   - Convert pronouns to their base nominative form while preserving grammatical number and person (e.g., "them" -> "they", "him" -> "he").
   - Do NOT lemmatize or alter proper nouns; preserve their original capitalization.

4. Contextual Classification
   - Classify each lexical unit according to its actual syntactic and semantic function in the given text.
   - A lexical unit MAY appear in multiple categories if it performs different functions in the text.

5. Multi-word Units
   - Detect and preserve phrasal verbs and idioms as single lexical units.
   - Idioms take priority over phrasal verbs.
   - If a word is part of a detected multi-word unit, do NOT list it separately as a single-word entry.

6. Articles and Determiners
   - Classify "a", "an", and "the" exclusively as "article".
   - Exclude articles from the "determiner" category.

7. Uniqueness
   - Each lexical unit must appear only once per category.
   - All arrays must contain unique entries.

8. Sorting
   - Sort all arrays alphabetically in ascending order.

------------------------
Output Requirements
------------------------

Return ONLY a valid JSON object and nothing else.

Allowed keys:
noun, verb, adjective, adverb, pronoun, proper_noun, article, determiner,
preposition, conjunction, numeral, particle, interjection,
phrasal_verb, idiom, other

Rules:
- Each key must have an array of strings as its value.
- Use an empty array [] if no items are found for a category.
Do not include any additional keys."""

DEFAULT_SETTINGS = {
    "endpoint": "http://127.0.0.1:1234/v1/chat/completions",
    "model": "mistralai/ministral-3-14b-reasoning",
    "temperature": 0.2,
    "context_limit": DEFAULT_CONTEXT_LIMIT,
    "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
    "token_safety_margin": DEFAULT_TOKEN_SAFETY_MARGIN,
    "system_prompt": DEFAULT_SYSTEM_PROMPT,
    "db_path": "vocabulary.db",
    "normalize_casefold": True,
    "export_on_process": False,
    "export_mode": "per_category",
    "consolidated_export_name": "vocabulary_all.ods",
    "auto_import_ods": True,
    "llm_cache_enabled": DEFAULT_LLM_CACHE_ENABLED,
    "llm_cache_max_entries": DEFAULT_LLM_CACHE_MAX_ENTRIES,
}


def _coerce_str(value, default):
    if isinstance(value, str) and value.strip():
        return value
    return default


def _coerce_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value, default, *, minimum=None):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def _coerce_bool(value, default):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return default


def get_default_settings_path(cwd=None):
    base = cwd or os.getcwd()
    return os.path.join(base, SETTINGS_FILENAME)


def load_settings(path=None):
    path = path or get_default_settings_path()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing settings.json at '{path}'. Create a settings.json file with "
            "keys: endpoint, model, temperature, context_limit, max_output_tokens, "
            "token_safety_margin, system_prompt, db_path."
        )

    settings = dict(DEFAULT_SETTINGS)
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        settings["endpoint"] = _coerce_str(data.get("endpoint"), settings["endpoint"])
        settings["model"] = _coerce_str(data.get("model"), settings["model"])
        settings["system_prompt"] = _coerce_str(
            data.get("system_prompt"), settings["system_prompt"]
        )
        settings["temperature"] = _coerce_float(
            data.get("temperature"), settings["temperature"]
        )
        settings["context_limit"] = _coerce_int(
            data.get("context_limit"), settings["context_limit"], minimum=1
        )
        settings["max_output_tokens"] = _coerce_int(
            data.get("max_output_tokens"), settings["max_output_tokens"], minimum=1
        )
        settings["token_safety_margin"] = _coerce_int(
            data.get("token_safety_margin"),
            settings["token_safety_margin"],
            minimum=0,
        )
        settings["db_path"] = _coerce_str(data.get("db_path"), settings["db_path"])
        settings["normalize_casefold"] = _coerce_bool(
            data.get("normalize_casefold"), settings["normalize_casefold"]
        )
        settings["export_on_process"] = _coerce_bool(
            data.get("export_on_process"), settings["export_on_process"]
        )
        settings["export_mode"] = _coerce_str(
            data.get("export_mode"), settings["export_mode"]
        )
        settings["consolidated_export_name"] = _coerce_str(
            data.get("consolidated_export_name"),
            settings["consolidated_export_name"],
        )
        settings["auto_import_ods"] = _coerce_bool(
            data.get("auto_import_ods"), settings["auto_import_ods"]
        )
        settings["llm_cache_enabled"] = _coerce_bool(
            data.get("llm_cache_enabled"), settings["llm_cache_enabled"]
        )
        settings["llm_cache_max_entries"] = _coerce_int(
            data.get("llm_cache_max_entries"),
            settings["llm_cache_max_entries"],
            minimum=0,
        )

    return settings


def apply_settings_defaults(settings):
    merged = dict(DEFAULT_SETTINGS)
    if isinstance(settings, dict):
        for key, value in settings.items():
            if value is not None:
                merged[key] = value
    return merged


def resolve_default_output_dir(cwd=None):
    base = cwd or os.getcwd()
    return os.path.join(base, DEFAULT_OUTPUT_DIRNAME)
