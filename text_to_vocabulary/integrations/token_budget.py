import math

from text_to_vocabulary.config import (
    DEFAULT_CONTEXT_LIMIT,
    DEFAULT_MAX_OUTPUT_TOKENS,
    DEFAULT_TOKEN_SAFETY_MARGIN,
)

MIN_OUTPUT_TOKENS = 256
_MESSAGE_OVERHEAD_TOKENS = 4
_TOKENIZER_CACHE = {}


def _coerce_int(value, default, *, minimum=None):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def _stringify_content(content):
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and "text" in item:
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
        return " ".join(part for part in parts if part)
    return str(content)


def _flatten_message(message):
    if not isinstance(message, dict):
        return str(message)
    role = message.get("role", "")
    content = _stringify_content(message.get("content", ""))
    extras = []
    for key, value in message.items():
        if key in {"role", "content"} or value is None:
            continue
        extras.append(f"{key}:{value}")
    parts = []
    if role:
        parts.append(f"role:{role}")
    if content:
        parts.append(content)
    if extras:
        parts.append(" ".join(extras))
    return "\n".join(parts)


def _get_tokenizer(model):
    try:
        import tiktoken
    except Exception:
        return None
    cache_key = model or "__default__"
    if cache_key in _TOKENIZER_CACHE:
        return _TOKENIZER_CACHE[cache_key]
    try:
        if model:
            encoding = tiktoken.encoding_for_model(model)
        else:
            encoding = tiktoken.get_encoding("cl100k_base")
    except Exception:
        try:
            encoding = tiktoken.get_encoding("cl100k_base")
        except Exception:
            return None
    _TOKENIZER_CACHE[cache_key] = encoding
    return encoding


def estimate_input_tokens(messages, *, model=None):
    if messages is None:
        return 0
    if not isinstance(messages, list):
        raise TypeError("messages must be a list of chat messages.")
    if not messages:
        return 0
    tokenizer = _get_tokenizer(model)
    if tokenizer is not None:
        text = "\n\n".join(_flatten_message(message) for message in messages)
        if not text:
            return 0
        return len(tokenizer.encode(text)) + (len(messages) * _MESSAGE_OVERHEAD_TOKENS)

    char_count = 0
    word_count = 0
    for message in messages:
        text = _flatten_message(message)
        char_count += len(text)
        word_count += len(text.split())
    token_by_chars = math.ceil(char_count / 4) if char_count else 0
    token_by_words = math.ceil(word_count * 1.3) if word_count else 0
    return max(token_by_chars, token_by_words) + (len(messages) * _MESSAGE_OVERHEAD_TOKENS)


def calculate_max_tokens(messages, settings):
    settings = settings or {}
    context_limit = _coerce_int(
        settings.get("context_limit"), DEFAULT_CONTEXT_LIMIT, minimum=1
    )
    max_output_tokens = _coerce_int(
        settings.get("max_output_tokens"), DEFAULT_MAX_OUTPUT_TOKENS, minimum=1
    )
    safety_margin = _coerce_int(
        settings.get("token_safety_margin"), DEFAULT_TOKEN_SAFETY_MARGIN, minimum=0
    )

    input_tokens = estimate_input_tokens(messages, model=settings.get("model"))
    available_output_tokens = context_limit - input_tokens - safety_margin

    if available_output_tokens < MIN_OUTPUT_TOKENS:
        available = max(available_output_tokens, 0)
        raise ValueError(
            "Input is too large for the configured context window. "
            f"Estimated input tokens: {input_tokens}, "
            f"context limit: {context_limit}, safety margin: {safety_margin}, "
            f"available output tokens: {available} (min {MIN_OUTPUT_TOKENS}). "
            "Shorten the input or increase context_limit in settings.json."
        )

    return min(max_output_tokens, available_output_tokens)
