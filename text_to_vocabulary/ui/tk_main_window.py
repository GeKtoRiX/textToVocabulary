import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from text_to_vocabulary.app.vocabulary_analysis import analyze_and_store, export_vocabulary
from text_to_vocabulary.config import load_settings, resolve_default_output_dir
from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES, format_markdown_table
from text_to_vocabulary.storage.sqlite_vocabulary_storage import SQLiteVocabularyStorage


class VocabularyWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Text to Vocabulary")
        self.geometry("980x720")
        self.minsize(900, 680)

        try:
            settings = load_settings()
        except FileNotFoundError as exc:
            messagebox.showerror("Missing settings.json", str(exc))
            self.destroy()
            raise SystemExit(1) from exc

        self.status_var = tk.StringVar(value="Ready.")
        self.endpoint_var = tk.StringVar(value=settings["endpoint"])
        self.model_var = tk.StringVar(value=settings["model"])
        self.temperature = settings["temperature"]
        self.system_prompt = settings["system_prompt"]
        self.output_dir_var = tk.StringVar(value=resolve_default_output_dir())
        self.export_mode = settings["export_mode"]
        self.consolidated_export_name = settings["consolidated_export_name"]
        self.auto_import_ods = settings["auto_import_ods"]
        self.export_in_progress = False

        db_path = settings["db_path"]
        if not os.path.isabs(db_path):
            db_path = os.path.join(os.getcwd(), db_path)
        try:
            self.storage = SQLiteVocabularyStorage(
                db_path, casefold=settings["normalize_casefold"]
            )
        except Exception as exc:
            messagebox.showerror("SQLite setup failed", str(exc))
            self.destroy()
            raise SystemExit(1) from exc

        self._build_ui()
        self._update_export_state()

    def _build_ui(self):
        padding = {"padx": 10, "pady": 8}

        config_frame = ttk.LabelFrame(self, text="LM Studio Config")
        config_frame.grid(row=0, column=0, sticky="ew", **padding)
        config_frame.columnconfigure(1, weight=1)
        config_frame.columnconfigure(3, weight=1)

        ttk.Label(config_frame, text="Endpoint").grid(row=0, column=0, sticky="w")
        endpoint_entry = ttk.Entry(config_frame, textvariable=self.endpoint_var)
        endpoint_entry.grid(row=0, column=1, sticky="ew", padx=(8, 20))

        ttk.Label(config_frame, text="Model").grid(row=0, column=2, sticky="w")
        model_entry = ttk.Entry(config_frame, textvariable=self.model_var)
        model_entry.grid(row=0, column=3, sticky="ew")

        ttk.Label(config_frame, text="Output dir").grid(row=1, column=0, sticky="w")
        output_entry = ttk.Entry(config_frame, textvariable=self.output_dir_var)
        output_entry.grid(row=1, column=1, sticky="ew", padx=(8, 20))
        browse_button = ttk.Button(config_frame, text="Browse", command=self._browse_output_dir)
        browse_button.grid(row=1, column=2, sticky="w")

        main_frame = ttk.Frame(self)
        main_frame.grid(row=1, column=0, sticky="nsew", **padding)
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)

        ttk.Label(main_frame, text="Input text").grid(row=0, column=0, sticky="w")
        ttk.Label(main_frame, text="Model output").grid(row=0, column=1, sticky="w")

        self.input_text = tk.Text(main_frame, wrap="word")
        self.input_text.grid(row=1, column=0, sticky="nsew", padx=(0, 8))

        self.output_text = tk.Text(main_frame, wrap="word", state="disabled")
        self.output_text.grid(row=1, column=1, sticky="nsew")

        action_frame = ttk.Frame(self)
        action_frame.grid(row=2, column=0, sticky="ew", **padding)
        action_frame.columnconfigure(2, weight=1)

        self.process_button = ttk.Button(action_frame, text="Process text", command=self._on_process)
        self.process_button.grid(row=0, column=0, sticky="w")
        self.export_button = ttk.Button(
            action_frame, text="Export", command=self._on_export, state="disabled"
        )
        self.export_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Label(action_frame, textvariable=self.status_var).grid(row=0, column=2, sticky="e")

        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)

    def _browse_output_dir(self):
        chosen = filedialog.askdirectory(initialdir=self.output_dir_var.get())
        if chosen:
            self.output_dir_var.set(chosen)

    def _set_output_text(self, text):
        self.output_text.configure(state="normal")
        self.output_text.delete("1.0", "end")
        self.output_text.insert("1.0", text)
        self.output_text.configure(state="disabled")

    def _on_process(self):
        text = self.input_text.get("1.0", "end").strip()
        if not text:
            messagebox.showwarning("Missing input", "Please paste text to analyze.")
            return

        output_dir = self.output_dir_var.get().strip()
        if not output_dir:
            messagebox.showwarning("Missing output dir", "Please choose an output directory.")
            return

        self.process_button.configure(state="disabled")
        self.export_button.configure(state="disabled")
        self.status_var.set("Sending request to LM Studio...")

        thread = threading.Thread(target=self._run_request, args=(text, output_dir), daemon=True)
        thread.start()

    def _run_request(self, text, output_dir):
        try:
            data, added_counts, table, _migration = analyze_and_store(
                text,
                endpoint=self.endpoint_var.get().strip(),
                model=self.model_var.get(),
                output_dir=output_dir,
                storage=self.storage,
                temperature=self.temperature,
                system_prompt=self.system_prompt,
                auto_import_ods=self.auto_import_ods,
            )
            summary = [table or format_markdown_table(data), "", "Stored in database."]
            if any(added_counts.values()):
                summary.append("Added counts:")
                for key in LEXICAL_CATEGORIES:
                    if added_counts[key]:
                        summary.append(f"- {key}: +{added_counts[key]}")
            summary.append("Use Export to generate ODS files.")
            self.after(0, lambda: self._on_success("\n".join(summary)))
        except Exception as exc:
            message = str(exc)
            self.after(0, lambda: self._on_error(message))

    def _on_success(self, message):
        self._set_output_text(message)
        self.status_var.set("Done.")
        self.process_button.configure(state="normal")
        self._update_export_state()

    def _on_error(self, message):
        self._set_output_text(f"Error:\n{message}")
        self.status_var.set("Failed.")
        self.process_button.configure(state="normal")
        self._update_export_state()

    def _update_export_state(self):
        has_db = os.path.exists(self.storage.db_path)
        has_data = False
        if has_db:
            try:
                has_data = not self.storage.is_empty()
            except Exception:
                has_data = False
        if self.export_in_progress or not (has_db and has_data):
            self.export_button.configure(state="disabled")
        else:
            self.export_button.configure(state="normal")

    def _on_export(self):
        if self.export_in_progress:
            return
        if self.storage.is_empty():
            messagebox.showwarning("Nothing to export", "No vocabulary data to export yet.")
            return
        output_dir = self.output_dir_var.get().strip()
        if not output_dir:
            messagebox.showwarning("Missing output dir", "Please choose an output directory.")
            return

        self.export_in_progress = True
        self.export_button.configure(state="disabled")
        self.status_var.set("Exporting ODS...")

        thread = threading.Thread(target=self._run_export, args=(output_dir,), daemon=True)
        thread.start()

    def _run_export(self, output_dir):
        try:
            _result, message = export_vocabulary(
                storage=self.storage,
                output_dir=output_dir,
                export_mode=self.export_mode,
                consolidated_export_name=self.consolidated_export_name,
            )
            self.after(0, lambda: self._on_export_success(message))
        except Exception as exc:
            self.after(0, lambda: self._on_export_error(str(exc)))

    def _on_export_success(self, message):
        self.export_in_progress = False
        self.status_var.set("Exported.")
        self._update_export_state()
        messagebox.showinfo("Export complete", message)

    def _on_export_error(self, message):
        self.export_in_progress = False
        self.status_var.set("Export failed.")
        self._update_export_state()
        messagebox.showerror("Export failed", message)
