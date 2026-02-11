# Text to Vocabulary

Desktop Tkinter tool that sends input text to an LLM and exports categorized vocabulary lists.

## Requirements
- Python 3.10+

## Run
```bash
python -m text_to_vocabulary
```

Windows users can also run `run.bat` (it uses the same module entrypoint).

## Configuration
`settings.json` in the project root is required. It must include:
- `endpoint`
- `model`
- `temperature`
- `system_prompt`

Minimal example:
```json
{
  "endpoint": "http://127.0.0.1:1234/v1/chat/completions",
  "model": "mistralai/ministral-3-14b-reasoning",
  "temperature": 0.2,
  "system_prompt": "...",
  "db_path": "vocabulary.db",
  "normalize_casefold": true,
  "export_on_process": false,
  "export_mode": "per_category",
  "consolidated_export_name": "vocabulary_all.ods",
  "auto_import_ods": true
}
```

Default exports are written to `exports/` unless you choose another folder in the UI.

## Storage
- SQLite (`db_path`) is the source of truth.
- ODS files are produced only via the Export button in the UI.
- Legacy ODS files in the output directory are imported into SQLite on first run when
  `auto_import_ods` is enabled.

`export_on_process` remains in settings for backward compatibility but is ignored.

## Tests
```bash
pytest -q
```
