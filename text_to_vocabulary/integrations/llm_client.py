import json
import urllib.error
import urllib.request

from text_to_vocabulary.config import DEFAULT_SYSTEM_PROMPT
from text_to_vocabulary.domain.vocabulary import (
    LEXICAL_CATEGORIES,
    dedupe_preserve_order,
    format_markdown_table,
)


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


def request_vocabulary_analysis(
    endpoint,
    model,
    text,
    *,
    temperature=0.2,
    timeout=90,
    system_prompt=None,
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

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"TEXT:\n{text}"},
        ],
        "temperature": temperature,
    }

    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Request failed ({exc.code}): {detail or exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Request failed: {exc.reason}") from exc

    data = json.loads(raw)
    content = data["choices"][0]["message"]["content"]
    parsed = extract_json(content)
    if not isinstance(parsed, dict):
        raise ValueError("Model response JSON must be an object.")

    result = {
        key: dedupe_preserve_order(parsed.get(key, [])) for key in LEXICAL_CATEGORIES
    }
    result["table"] = format_markdown_table(result)

    return result
