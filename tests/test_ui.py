import os
import tkinter as tk

import pytest

from text_to_vocabulary.domain import vocabulary as domain
from text_to_vocabulary.ui import tk_main_window as ui


def _stub_settings(tmp_path):
    return {
        "endpoint": "http://127.0.0.1:1234/v1/chat/completions",
        "model": "mistralai/ministral-3-14b-reasoning",
        "temperature": 0.2,
        "system_prompt": "PROMPT",
        "db_path": str(tmp_path / "vocabulary.db"),
        "export_mode": "per_category",
        "consolidated_export_name": "vocabulary_all.ods",
        "auto_import_ods": False,
    }


def _create_app(monkeypatch, tmp_path):
    monkeypatch.setattr(ui, "load_settings", lambda: _stub_settings(tmp_path))
    try:
        return ui.VocabularyWindow()
    except tk.TclError as exc:
        pytest.skip(f"Tkinter unavailable: {exc}")


def test_app_defaults(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    assert app.endpoint_var.get() == "http://127.0.0.1:1234/v1/chat/completions"
    assert app.model_var.get() == "mistralai/ministral-3-14b-reasoning"
    assert os.path.basename(app.output_dir_var.get()) == "exports"

    app.destroy()


def test_run_request_success(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    data = {key: [] for key in domain.LEXICAL_CATEGORIES}
    data["table"] = "TABLE"

    added_counts = {key: 0 for key in domain.LEXICAL_CATEGORIES}

    def fake_analyze_and_store(
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
        return data, added_counts, data["table"], None

    monkeypatch.setattr(ui, "analyze_and_store", fake_analyze_and_store)

    app._run_request("hello", str(tmp_path))
    app.update()

    output = app.output_text.get("1.0", "end").strip()
    assert output.startswith("TABLE")
    assert "Stored in database." in output
    assert "Use Export to generate ODS files." in output
    assert app.status_var.get() == "Done."

    app.destroy()


def test_run_request_error(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    def fake_analyze_and_store(
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
        raise RuntimeError("boom")

    monkeypatch.setattr(ui, "analyze_and_store", fake_analyze_and_store)

    app._run_request("hello", str(tmp_path))
    app.update()

    output = app.output_text.get("1.0", "end").strip()
    assert output.startswith("Error:")
    assert "boom" in output
    assert app.status_var.get() == "Failed."

    app.destroy()


def test_on_process_empty_input(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    called = {}

    def fake_warning(title, message):
        called["title"] = title
        called["message"] = message

    monkeypatch.setattr(ui.messagebox, "showwarning", fake_warning)

    app.input_text.delete("1.0", "end")
    app._on_process()

    assert called["title"] == "Missing input"
    assert app.status_var.get() == "Ready."
    assert app.process_button.instate(["!disabled"])

    app.destroy()


def test_on_process_empty_output_dir(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    called = {}

    def fake_warning(title, message):
        called["title"] = title
        called["message"] = message

    monkeypatch.setattr(ui.messagebox, "showwarning", fake_warning)

    app.input_text.delete("1.0", "end")
    app.input_text.insert("1.0", "Hello")
    app.output_dir_var.set("")
    app._on_process()

    assert called["title"] == "Missing output dir"
    assert app.status_var.get() == "Ready."
    assert app.process_button.instate(["!disabled"])

    app.destroy()


def test_export_button_disabled_when_empty(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    assert app.export_button.instate(["disabled"])

    app.destroy()


def test_export_button_enabled_when_data(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    app.storage.add_words("noun", ["cat"])
    app._update_export_state()

    assert app.export_button.instate(["!disabled"])

    app.destroy()


def test_export_action_success(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)
    app.withdraw()

    app.storage.add_words("noun", ["cat"])
    app._update_export_state()

    called = {}

    def fake_export_multiple(*args, **kwargs):
        return {"mode": "per_category", "files": {}}, "Exported to exports"

    def fake_showinfo(title, message):
        called["title"] = title
        called["message"] = message

    monkeypatch.setattr(ui, "export_multiple_files", fake_export_multiple)
    monkeypatch.setattr(ui.messagebox, "showinfo", fake_showinfo)

    app._run_export("multiple", str(tmp_path))
    app.update()

    assert called["title"] == "Export complete"
    assert "Exported" in called["message"]
    assert app.status_var.get() == "Exported."

    app.destroy()
