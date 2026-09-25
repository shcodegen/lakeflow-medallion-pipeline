"""Static checks for the Databricks source-format notebooks (runs in CI).

* Every Python cell must parse (magic cells like `%pip` / `%sql` are skipped).
* No leftover course-environment identifiers.
"""
import ast
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
FORBIDDEN = re.compile(r"dbacademy|Vocareum|labuser|da\.catalog_name|Databricks, Inc\.|db-academy", re.I)


def python_cells(text):
    for i, cell in enumerate(text.split("# COMMAND ----------")):
        lines = [l for l in cell.splitlines() if not l.startswith("# MAGIC") and l != "# Databricks notebook source"]
        body = "\n".join(lines)
        if body.strip() and not body.lstrip().startswith("%"):
            yield i, body


errors = []
for nb in sorted((ROOT / "notebooks").rglob("*")):
    if nb.suffix not in {".py", ".sql"}:
        continue
    text = nb.read_text()
    first = text.splitlines()[0] if text else ""
    if first not in ("# Databricks notebook source", "-- Databricks notebook source"):
        errors.append(f"{nb.relative_to(ROOT)}: missing Databricks notebook header")
    if m := FORBIDDEN.search(text):
        errors.append(f"{nb.relative_to(ROOT)}: forbidden identifier {m.group(0)!r}")
    if nb.suffix == ".py":
        for i, body in python_cells(text):
            try:
                ast.parse(body)
            except SyntaxError as e:
                errors.append(f"{nb.relative_to(ROOT)} cell {i}: {e.msg} (line {e.lineno})")

print("\n".join(errors) or "notebooks OK")
sys.exit(1 if errors else 0)
