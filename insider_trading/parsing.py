from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator


def read_tsv(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="	")
        for row in reader:
            yield row
