from __future__ import annotations

import csv
from io import StringIO
from typing import Optional

import requests


def build_url(symbol: str) -> str:
    return f"https://stooq.com/q/d/l/?s={symbol}&i=d"


def fetch_last_close(symbol: str, timeout: int = 30) -> Optional[float]:
    url = build_url(symbol)
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException:
        return None
    text = resp.text.strip()
    if not text:
        return None
    reader = csv.DictReader(StringIO(text))
    rows = list(reader)
    if not rows:
        return None
    last = rows[-1]
    close = last.get("Close") or last.get("close")
    if close is None:
        return None
    try:
        return float(close)
    except ValueError:
        return None
