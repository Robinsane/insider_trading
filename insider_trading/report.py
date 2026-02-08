from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def render_table(rows: list[dict[str, Any]], columns: list[str], max_width: int = 22) -> str:
    def truncate(value: Any) -> str:
        text = "" if value is None else str(value)
        if len(text) > max_width:
            return text[: max_width - 1] + "..."
        return text

    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(truncate(row.get(col))))

    def fmt_row(row: dict[str, Any]) -> str:
        return " | ".join(truncate(row.get(col)).ljust(widths[col]) for col in columns)

    header = " | ".join(col.ljust(widths[col]) for col in columns)
    sep = "-+-".join("-" * widths[col] for col in columns)
    lines = [header, sep]
    for row in rows:
        lines.append(fmt_row(row))
    return "\n".join(lines)


def _format_value(value: Any) -> Any:
    if isinstance(value, float):
        return f"{value}".replace(".", ",")
    return value


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({col: _format_value(row.get(col)) for col in columns})
