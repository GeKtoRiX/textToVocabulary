import json

from text_to_vocabulary import config
from text_to_vocabulary.domain import vocabulary as domain
from text_to_vocabulary.integrations import llm_client
from text_to_vocabulary.storage import ods_vocabulary_store as store


def test_dedupe_preserve_order_strips():
    items = [" cat ", "dog", "cat", " ", "dog", "take off"]
    assert domain.dedupe_preserve_order(items) == ["cat", "dog", "take off"]


def test_dedupe_preserve_order_empty():
    assert domain.dedupe_preserve_order([]) == []


def test_extract_json_direct():
    data = llm_client.extract_json('{"noun": ["cat"]}')
    assert data["noun"] == ["cat"]


def test_extract_json_embedded():
    content = 'prefix {"noun": ["cat"]} suffix'
    data = llm_client.extract_json(content)
    assert data["noun"] == ["cat"]


def test_extract_json_missing():
    try:
        llm_client.extract_json("no json here")
    except ValueError as exc:
        assert "No JSON object" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_format_table_rows():
    table = domain.format_markdown_table({"noun": ["cat"], "verb": ["run"]})
    lines = table.strip().splitlines()
    assert lines[0] == "| Type | Words |"
    assert lines[1] == "| --- | --- |"
    assert len(lines) == len(domain.LEXICAL_CATEGORIES) + 2
    assert "| noun | cat |" in lines
    assert "| verb | run |" in lines


def test_format_table_uses_dash_for_empty():
    table = domain.format_markdown_table({})
    lines = table.strip().splitlines()
    assert "| noun | - |" in lines
    assert "| verb | - |" in lines


def test_ods_roundtrip(tmp_path):
    path = tmp_path / "words.ods"
    words = ["cat", "take off"]

    store.write_words_to_ods(str(path), words)
    assert store.read_words_from_ods(str(path)) == words


def test_normalize_endpoint():
    assert (
        llm_client.normalize_endpoint("http://127.0.0.1:1234")
        == "http://127.0.0.1:1234/v1/chat/completions"
    )
    assert (
        llm_client.normalize_endpoint("http://127.0.0.1:1234/v1")
        == "http://127.0.0.1:1234/v1/chat/completions"
    )
    assert (
        llm_client.normalize_endpoint("http://127.0.0.1:1234/v1/chat/completions")
        == "http://127.0.0.1:1234/v1/chat/completions"
    )
    assert (
        llm_client.normalize_endpoint("http://127.0.0.1:1234/")
        == "http://127.0.0.1:1234/v1/chat/completions"
    )


def test_request_analysis_requires_endpoint():
    try:
        llm_client.request_vocabulary_analysis("", "model-x", "text")
    except ValueError as exc:
        assert "Endpoint is empty" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_request_analysis_parses_response(monkeypatch):
    captured = {}

    def fake_post_json(url, payload, timeout=90):
        captured["url"] = url
        captured["payload"] = payload
        response_payload = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "noun": ["cat", "cat", " dog "],
                                "phrasal_verb": ["take off", "take off"],
                                "table": "",
                            }
                        )
                    }
                }
            ]
        }
        return json.dumps(response_payload)

    monkeypatch.setattr(llm_client, "_post_json", fake_post_json)

    result = llm_client.request_vocabulary_analysis(
        "http://127.0.0.1:1234",
        "model-x",
        "Hello world",
        temperature=0.3,
        system_prompt="PROMPT",
    )
    payload = captured["payload"]

    assert captured["url"].endswith("/v1/chat/completions")
    assert payload["model"] == "model-x"
    assert payload["temperature"] == 0.3
    assert payload["messages"][0]["content"] == "PROMPT"
    assert result["noun"] == ["cat", "cat", " dog "]
    assert result["phrasal_verb"] == ["take off", "take off"]
    assert result["table"] == domain.format_markdown_table(result)
    for key in domain.LEXICAL_CATEGORIES:
        assert key in result


def test_request_analysis_default_model_and_messages(monkeypatch):
    captured = {}

    def fake_post_json(url, payload, timeout=90):
        captured["url"] = url
        captured["payload"] = payload
        response_payload = {"choices": [{"message": {"content": '{"noun": []}'}}]}
        return json.dumps(response_payload)

    monkeypatch.setattr(llm_client, "_post_json", fake_post_json)

    result = llm_client.request_vocabulary_analysis("http://127.0.0.1:1234", "   ", "Hello")
    payload = captured["payload"]

    assert payload["model"] == "local-model"
    assert payload["temperature"] == 0.2
    assert payload["messages"][0]["content"] == config.DEFAULT_SYSTEM_PROMPT
    assert payload["messages"][0]["role"] == "system"
    assert payload["messages"][1]["role"] == "user"
    assert payload["messages"][1]["content"].startswith("TEXT:\n")
    assert result["noun"] == []
    assert result["table"] == domain.format_markdown_table(result)


def test_write_vocabulary_exports_appends_missing(tmp_path):
    output_dir = tmp_path / "data"
    data = {"noun": ["cat", "dog", "cat"], "verb": ["run"]}

    saved_files, added_counts = store.write_vocabulary_exports(str(output_dir), data)
    assert added_counts["noun"] == 2
    assert added_counts["verb"] == 1

    noun_path = saved_files["noun"]
    verb_path = saved_files["verb"]
    assert output_dir.joinpath("noun.ods").exists()
    assert output_dir.joinpath("verb.ods").exists()

    assert store.read_words_from_ods(noun_path) == ["cat", "dog"]

    saved_files, added_counts = store.write_vocabulary_exports(str(output_dir), data)
    assert added_counts["noun"] == 0
    assert added_counts["verb"] == 0

    data_update = {"noun": ["cat", "bird"]}
    saved_files, added_counts = store.write_vocabulary_exports(str(output_dir), data_update)
    assert added_counts["noun"] == 1

    assert store.read_words_from_ods(noun_path) == ["cat", "dog", "bird"]


def test_append_missing_words_handles_existing(tmp_path):
    path = tmp_path / "noun.ods"
    store.write_words_to_ods(str(path), ["cat", "dog"])

    count = store.append_missing_words(str(path), ["dog", "bird", "cat", " "])
    assert count == 1
    assert store.read_words_from_ods(str(path)) == ["cat", "dog", "bird"]


def test_append_missing_words_migrates_from_txt(tmp_path):
    txt_path = tmp_path / "noun.txt"
    txt_path.write_text("cat\ndog\n", encoding="utf-8")
    ods_path = tmp_path / "noun.ods"

    count = store.append_missing_words(str(ods_path), ["dog", "bird"])
    assert count == 1
    assert ods_path.exists()
    assert store.read_words_from_ods(str(ods_path)) == ["cat", "dog", "bird"]
