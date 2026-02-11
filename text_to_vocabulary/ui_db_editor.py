import os
import sqlite3
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from text_to_vocabulary.db_manager import DatabaseManager, quote_identifier


class RowEditorDialog(tk.Toplevel):
    def __init__(self, master, table_info, title, values=None):
        super().__init__(master)
        self.title(title)
        self.resizable(True, True)
        self.result = None
        self._entries = {}

        self.transient(master)
        self.grab_set()

        form_frame = ttk.Frame(self)
        form_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        form_frame.columnconfigure(1, weight=1)

        for row_index, column in enumerate(table_info):
            col_name = column["name"]
            col_type = column["type"] or ""
            label_text = f"{col_name} ({col_type})" if col_type else col_name
            if column.get("notnull"):
                label_text = f"{label_text} *"
            ttk.Label(form_frame, text=label_text).grid(
                row=row_index, column=0, sticky="w", pady=4
            )
            entry = ttk.Entry(form_frame)
            entry.grid(row=row_index, column=1, sticky="ew", pady=4)
            if values and col_name in values and values[col_name] is not None:
                entry.insert(0, str(values[col_name]))
            self._entries[col_name] = entry

        button_frame = ttk.Frame(self)
        button_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 10))
        button_frame.columnconfigure(0, weight=1)

        save_button = ttk.Button(button_frame, text="Save", command=self._on_save)
        save_button.grid(row=0, column=1, sticky="e", padx=(0, 6))
        cancel_button = ttk.Button(button_frame, text="Cancel", command=self._on_cancel)
        cancel_button.grid(row=0, column=2, sticky="e")

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

    def _on_save(self):
        values = {}
        for name, entry in self._entries.items():
            text = entry.get().strip()
            values[name] = None if text == "" else text
        self.result = values
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()


class DatabaseEditorWindow(tk.Toplevel):
    def __init__(self, master=None, db_path=None):
        super().__init__(master)
        self.title("SQLite Editor")
        self.geometry("1100x740")
        self.minsize(900, 600)

        self.db = None
        self.current_table = None
        self.current_columns = []
        self.table_info = []
        self.row_identifier = None
        self.page_size = 200
        self.current_offset = 0
        self.total_rows = 0
        self.row_id_map = {}
        self.row_data_map = {}

        self.status_var = tk.StringVar(value="No database loaded.")
        self.sql_status_var = tk.StringVar(value="")
        self.page_info_var = tk.StringVar(value="")

        self._build_ui()

        if db_path:
            self.open_database(db_path)

    def _build_ui(self):
        menubar = tk.Menu(self)
        file_menu = tk.Menu(menubar, tearoff=False)
        file_menu.add_command(label="Open .db...", command=self._open_db_dialog)
        file_menu.add_separator()
        file_menu.add_command(label="Close", command=self.destroy)
        menubar.add_cascade(label="File", menu=file_menu)
        self.config(menu=menubar)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        main_pane = ttk.PanedWindow(self, orient="horizontal")
        main_pane.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        left_frame = ttk.Frame(main_pane)
        right_frame = ttk.Frame(main_pane)
        main_pane.add(left_frame, weight=1)
        main_pane.add(right_frame, weight=4)

        ttk.Label(left_frame, text="Tables").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.table_list = tk.Listbox(left_frame, exportselection=False)
        self.table_list.grid(row=1, column=0, sticky="nsew", padx=6, pady=(0, 6))
        table_scroll = ttk.Scrollbar(left_frame, orient="vertical", command=self.table_list.yview)
        table_scroll.grid(row=1, column=1, sticky="ns", pady=(0, 6))
        self.table_list.configure(yscrollcommand=table_scroll.set)
        self.table_list.bind("<<ListboxSelect>>", self._on_table_select)
        left_frame.rowconfigure(1, weight=1)
        left_frame.columnconfigure(0, weight=1)

        right_frame.rowconfigure(0, weight=1)
        right_frame.columnconfigure(0, weight=1)

        notebook = ttk.Notebook(right_frame)
        notebook.grid(row=0, column=0, sticky="nsew")

        data_tab = ttk.Frame(notebook)
        sql_tab = ttk.Frame(notebook)
        notebook.add(data_tab, text="Data")
        notebook.add(sql_tab, text="SQL Console")

        self._build_data_tab(data_tab)
        self._build_sql_tab(sql_tab)

        status = ttk.Label(self, textvariable=self.status_var, anchor="w")
        status.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))

    def _build_data_tab(self, parent):
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", padx=6, pady=(6, 2))

        button_frame = ttk.Frame(toolbar)
        button_frame.pack(side="left")

        self.add_button = ttk.Button(button_frame, text="Add", command=self._on_add)
        self.add_button.pack(side="left")
        self.edit_button = ttk.Button(button_frame, text="Edit", command=self._on_edit)
        self.edit_button.pack(side="left", padx=(6, 0))
        self.delete_button = ttk.Button(button_frame, text="Delete", command=self._on_delete)
        self.delete_button.pack(side="left", padx=(6, 0))
        self.refresh_button = ttk.Button(button_frame, text="Refresh", command=self._refresh_table)
        self.refresh_button.pack(side="left", padx=(6, 0))

        page_frame = ttk.Frame(toolbar)
        page_frame.pack(side="right")

        self.prev_button = ttk.Button(page_frame, text="Prev", command=self._on_prev_page)
        self.prev_button.pack(side="left")
        self.next_button = ttk.Button(page_frame, text="Next", command=self._on_next_page)
        self.next_button.pack(side="left", padx=(6, 0))
        ttk.Label(page_frame, textvariable=self.page_info_var).pack(side="left", padx=(8, 0))

        table_frame = ttk.Frame(parent)
        table_frame.pack(fill="both", expand=True, padx=6, pady=(2, 6))

        self.table_tree = ttk.Treeview(table_frame, show="headings")
        table_scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.table_tree.yview)
        table_scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.table_tree.xview)
        self.table_tree.configure(yscrollcommand=table_scroll_y.set, xscrollcommand=table_scroll_x.set)

        self.table_tree.grid(row=0, column=0, sticky="nsew")
        table_scroll_y.grid(row=0, column=1, sticky="ns")
        table_scroll_x.grid(row=1, column=0, sticky="ew")

        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

    def _build_sql_tab(self, parent):
        ttk.Label(parent, text="SQL Query").pack(anchor="w", padx=6, pady=(6, 2))
        self.sql_text = tk.Text(parent, height=6, wrap="none")
        self.sql_text.pack(fill="x", padx=6)

        run_frame = ttk.Frame(parent)
        run_frame.pack(fill="x", padx=6, pady=(4, 6))
        run_button = ttk.Button(run_frame, text="Run", command=self._on_run_sql)
        run_button.pack(side="left")
        ttk.Label(run_frame, textvariable=self.sql_status_var).pack(side="left", padx=(8, 0))

        result_frame = ttk.Frame(parent)
        result_frame.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        self.sql_tree = ttk.Treeview(result_frame, show="headings")
        sql_scroll_y = ttk.Scrollbar(result_frame, orient="vertical", command=self.sql_tree.yview)
        sql_scroll_x = ttk.Scrollbar(result_frame, orient="horizontal", command=self.sql_tree.xview)
        self.sql_tree.configure(yscrollcommand=sql_scroll_y.set, xscrollcommand=sql_scroll_x.set)

        self.sql_tree.grid(row=0, column=0, sticky="nsew")
        sql_scroll_y.grid(row=0, column=1, sticky="ns")
        sql_scroll_x.grid(row=1, column=0, sticky="ew")
        result_frame.rowconfigure(0, weight=1)
        result_frame.columnconfigure(0, weight=1)

    def _open_db_dialog(self):
        initial_dir = os.path.dirname(self.db.db_path) if self.db else os.getcwd()
        path = filedialog.askopenfilename(
            title="Open SQLite database",
            initialdir=initial_dir,
            filetypes=[
                ("SQLite Database", "*.db;*.sqlite;*.sqlite3"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.open_database(path)

    def open_database(self, path: str):
        try:
            if self.db is None:
                self.db = DatabaseManager(path)
            else:
                self.db.open(path)
        except (OSError, sqlite3.Error) as exc:
            self._handle_db_error(exc, title="Open database failed")
            return

        self.current_table = None
        self.current_offset = 0
        self._clear_table_view()
        self._load_table_list()
        self._update_status()

    def _load_table_list(self):
        self.table_list.delete(0, "end")
        tables = []
        if self.db:
            tables = self.db.list_tables()
            for name in tables:
                self.table_list.insert("end", name)
        if not tables:
            return
        if self.current_table in tables:
            index = tables.index(self.current_table)
        else:
            index = 0
        self.table_list.selection_set(index)
        self.table_list.activate(index)
        self._load_table(tables[index])

    def _on_table_select(self, _event):
        selection = self.table_list.curselection()
        if not selection:
            return
        table = self.table_list.get(selection[0])
        self._load_table(table)

    def _load_table(self, table: str):
        self.current_table = table
        self.current_offset = 0
        self._refresh_table()

    def _refresh_table(self):
        if not self.db or not self.current_table:
            return
        try:
            self.table_info = self.db.get_table_info(self.current_table)
            self.current_columns = [col["name"] for col in self.table_info]
            self.row_identifier = self.db.get_row_identifier(self.current_table)
            self.total_rows = self.db.count_rows(self.current_table)
            if self.total_rows and self.current_offset >= self.total_rows:
                last_page = (self.total_rows - 1) // self.page_size
                self.current_offset = last_page * self.page_size

            include_rowid = self.row_identifier is not None and self.row_identifier.kind == "rowid"
            order_by = None
            if self.row_identifier:
                order_by = "rowid" if self.row_identifier.kind == "rowid" else quote_identifier(
                    self.row_identifier.column
                )
            columns, rows = self.db.fetch_rows(
                self.current_table,
                self.page_size,
                self.current_offset,
                include_rowid=include_rowid,
                order_by=order_by,
            )
        except sqlite3.Error as exc:
            self._handle_db_error(exc, title="Load table failed")
            return

        self._populate_table(columns, rows)
        self._update_page_controls()
        self._update_action_state()
        self._update_status()

    def _populate_table(self, columns, rows):
        self.table_tree.delete(*self.table_tree.get_children())
        self.table_tree["columns"] = columns
        for col in columns:
            self.table_tree.heading(col, text=col)
            self.table_tree.column(col, width=140, anchor="w", stretch=True)

        self.row_id_map = {}
        self.row_data_map = {}
        for index, row in enumerate(rows):
            item_id = f"row_{self.current_offset + index}"
            identifier_value = None
            if self.row_identifier:
                if self.row_identifier.kind == "rowid":
                    identifier_value = row["__rowid__"]
                else:
                    identifier_value = row[self.row_identifier.column]
            self.row_id_map[item_id] = identifier_value
            self.row_data_map[item_id] = row
            values = [self._format_value(row[col]) for col in columns]
            self.table_tree.insert("", "end", iid=item_id, values=values)

    def _clear_table_view(self):
        self.table_tree.delete(*self.table_tree.get_children())
        self.table_tree["columns"] = []
        self.row_id_map = {}
        self.row_data_map = {}
        self.current_columns = []
        self.table_info = []
        self.row_identifier = None
        self.total_rows = 0
        self.page_info_var.set("")
        self._update_action_state()

    def _update_action_state(self):
        has_table = bool(self.current_table)
        self.add_button.configure(state="normal" if has_table else "disabled")
        can_edit = has_table and self.row_identifier is not None
        self.edit_button.configure(state="normal" if can_edit else "disabled")
        self.delete_button.configure(state="normal" if can_edit else "disabled")
        self.refresh_button.configure(state="normal" if has_table else "disabled")

    def _update_page_controls(self):
        if not self.current_table:
            self.page_info_var.set("")
            self.prev_button.configure(state="disabled")
            self.next_button.configure(state="disabled")
            return

        if self.total_rows == 0:
            self.page_info_var.set("0 rows")
            self.prev_button.configure(state="disabled")
            self.next_button.configure(state="disabled")
            return

        start = self.current_offset + 1
        end = min(self.current_offset + self.page_size, self.total_rows)
        page = self.current_offset // self.page_size + 1
        total_pages = (self.total_rows - 1) // self.page_size + 1
        self.page_info_var.set(f"Rows {start}-{end} of {self.total_rows} (Page {page}/{total_pages})")

        self.prev_button.configure(state="normal" if self.current_offset > 0 else "disabled")
        self.next_button.configure(
            state="normal" if self.current_offset + self.page_size < self.total_rows else "disabled"
        )

    def _update_status(self):
        if not self.db:
            self.status_var.set("No database loaded.")
            return
        parts = [f"DB: {self.db.db_path}"]
        if self.current_table:
            parts.append(f"Table: {self.current_table}")
        if self.current_table and self.row_identifier is None:
            parts.append("Edit/Delete disabled: no primary key or rowid")
        elif self.current_table and self.row_identifier and self.row_identifier.kind == "rowid":
            parts.append("Edit/Delete uses rowid")
        self.status_var.set(" | ".join(parts))

    def _on_prev_page(self):
        if self.current_offset == 0:
            return
        self.current_offset = max(0, self.current_offset - self.page_size)
        self._refresh_table()

    def _on_next_page(self):
        if self.current_offset + self.page_size >= self.total_rows:
            return
        self.current_offset += self.page_size
        self._refresh_table()

    def _on_add(self):
        if not self.current_table:
            return
        dialog = RowEditorDialog(self, self.table_info, f"Add Row: {self.current_table}")
        self.wait_window(dialog)
        if dialog.result is None:
            return
        try:
            self.db.insert_row(self.current_table, dialog.result)
        except sqlite3.Error as exc:
            self._handle_db_error(exc, title="Insert failed")
            return
        self._refresh_table()

    def _on_edit(self):
        if not self.current_table:
            return
        if self.row_identifier is None:
            messagebox.showwarning(
                "Edit disabled",
                "Editing is disabled because this table has no primary key and rowid is unavailable.",
            )
            return
        selected = self._get_selected_item()
        if not selected:
            return
        row = self.row_data_map[selected]
        values = {col: row[col] for col in self.current_columns}
        dialog = RowEditorDialog(self, self.table_info, f"Edit Row: {self.current_table}", values=values)
        self.wait_window(dialog)
        if dialog.result is None:
            return
        try:
            identifier_value = self.row_id_map[selected]
            self.db.update_row(self.current_table, dialog.result, self.row_identifier, identifier_value)
        except sqlite3.Error as exc:
            self._handle_db_error(exc, title="Update failed")
            return
        self._refresh_table()

    def _on_delete(self):
        if not self.current_table:
            return
        if self.row_identifier is None:
            messagebox.showwarning(
                "Delete disabled",
                "Deleting is disabled because this table has no primary key and rowid is unavailable.",
            )
            return
        selected = self._get_selected_item()
        if not selected:
            return
        if not messagebox.askyesno(
            "Confirm delete",
            "Delete the selected row? This cannot be undone.",
        ):
            return
        try:
            identifier_value = self.row_id_map[selected]
            self.db.delete_row(self.current_table, self.row_identifier, identifier_value)
        except sqlite3.Error as exc:
            self._handle_db_error(exc, title="Delete failed")
            return
        self._refresh_table()

    def _on_run_sql(self):
        if not self.db:
            messagebox.showwarning("No database", "Please open a database first.")
            return
        sql = self.sql_text.get("1.0", "end").strip()
        if not sql:
            return
        try:
            result = self.db.execute_sql(sql)
        except sqlite3.Error as exc:
            self._handle_db_error(exc, title="SQL error")
            return

        if result["kind"] == "select":
            self._populate_sql_results(result["columns"], result["rows"])
            self.sql_status_var.set(f"{result['rowcount']} row(s)")
        else:
            self._populate_sql_results([], [])
            self.sql_status_var.set(f"Affected rows: {result['rowcount']}")
            self._load_table_list()
            if self.current_table:
                self._refresh_table()

    def _populate_sql_results(self, columns, rows):
        self.sql_tree.delete(*self.sql_tree.get_children())
        self.sql_tree["columns"] = columns
        for col in columns:
            self.sql_tree.heading(col, text=col)
            self.sql_tree.column(col, width=140, anchor="w", stretch=True)
        for index, row in enumerate(rows):
            item_id = f"sql_{index}"
            values = [self._format_value(row[i]) for i in range(len(columns))]
            self.sql_tree.insert("", "end", iid=item_id, values=values)

    def _get_selected_item(self):
        selection = self.table_tree.selection()
        if not selection:
            messagebox.showwarning("No selection", "Please select a row first.")
            return None
        return selection[0]

    @staticmethod
    def _format_value(value):
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.hex()
        return str(value)

    def _handle_db_error(self, exc: Exception, title: str):
        message = str(exc)
        if isinstance(exc, sqlite3.OperationalError) and "locked" in message.lower():
            message = "Database is locked. Close other applications using it and try again."
        messagebox.showerror(title, message)
