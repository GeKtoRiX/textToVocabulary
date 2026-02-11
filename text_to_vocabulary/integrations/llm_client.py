import json

from text_to_vocabulary.config import DEFAULT_SYSTEM_PROMPT
from text_to_vocabulary.domain.vocabulary import (
    LEXICAL_CATEGORIES,
    dedupe_preserve_order,
    format_markdown_table,
)
from text_to_vocabulary.integrations.http_client import HttpClient
from text_to_vocabulary.integrations.llm_cache import build_cache_key
from text_to_vocabulary.integrations.token_budget import calculate_max_tokens

CACHE_VERSION = 1
_HTTP_CLIENT = HttpClient()


def extract_json(content):
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        index = content.find("{")
        while index != -1:
            try:
                return decoder.raw_decode(content[index:])[0]
            except json.JSONDecodeError:
                index = content.find("{", index + 1)
        raise ValueError("No JSON object found in model response.")


def normalize_endpoint(endpoint):
    cleaned = endpoint.rstrip("/")
    if cleaned.endswith("/v1"):
        return f"{cleaned}/chat/completions"
    if cleaned.endswith("/chat/completions"):
        return cleaned
    if cleaned.endswith("/v1/chat/completions"):
        return cleaned
    return f"{cleaned}/v1/chat/completions"


def _post_json(url, payload, *, timeout):
    return _HTTP_CLIENT.post_json(url, payload, timeout=timeout)


def request_vocabulary_analysis(
    endpoint,
    model,
    text,
    *,
    temperature=0.2,
    timeout=90,
    system_prompt=None,
    context_limit=None,
    max_output_tokens=None,
    token_safety_margin=None,
    cache=None,
):
    if not endpoint:
        raise ValueError("Endpoint is empty.")

    endpoint = normalize_endpoint(endpoint)
    model = model.strip() or "local-model"
    if system_prompt is None:
        system_prompt = DEFAULT_SYSTEM_PROMPT
    elif not isinstance(system_prompt, str):
        system_prompt = str(system_prompt)
    try:
        temperature = float(temperature)
    except (TypeError, ValueError):
        temperature = 0.2

    cache_key = None
    if cache is not None:
        signature = {
            "version": CACHE_VERSION,
            "endpoint": endpoint,
            "model": model,
            "temperature": temperature,
            "system_prompt": system_prompt,
            "text": text,
            "context_limit": context_limit,
            "max_output_tokens": max_output_tokens,
            "token_safety_margin": token_safety_margin,
        }
        cache_key = build_cache_key(signature)
        try:
            cached = cache.get(cache_key)
        except Exception:
            cached = None
        if isinstance(cached, dict):
            return cached

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"TEXT:\n{text}"},
        ],
        "temperature": temperature,
    }
    budget_settings = {
        "context_limit": context_limit,
        "max_output_tokens": max_output_tokens,
        "token_safety_margin": token_safety_margin,
        "model": model,
    }
    payload["max_tokens"] = calculate_max_tokens(payload["messages"], budget_settings)
    raw = _post_json(endpoint, payload, timeout=timeout)
    data = json.loads(raw)
    content = data["choices"][0]["message"]["content"]
    parsed = extract_json(content)
    if not isinstance(parsed, dict):
        raise ValueError("Model response JSON must be an object.")

    result = {
        key: dedupe_preserve_order(parsed.get(key, [])) for key in LEXICAL_CATEGORIES
    }
    result["table"] = format_markdown_table(result)

    if cache is not None and cache_key is not None:
        try:
            cache.set(cache_key, result)
        except Exception:
            pass

    return result
