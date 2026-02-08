from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Optional

import requests


def _load_usage(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _save_usage(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def increment_fmp_usage(usage_path: Path) -> int:
    today = date.today().isoformat()
    data = _load_usage(usage_path)
    if data.get("date") != today:
        data = {"date": today, "count": 0}
    data["count"] = int(data.get("count", 0)) + 1
    _save_usage(usage_path, data)
    return int(data["count"])


def get_fmp_usage_count(usage_path: Path) -> int:
    today = date.today().isoformat()
    data = _load_usage(usage_path)
    if data.get("date") != today:
        return 0
    try:
        return int(data.get("count", 0))
    except (TypeError, ValueError):
        return 0


def fetch_market_cap_fmp(
    symbol: str,
    api_key: str | None,
    usage_path: Path | None = None,
    timeout: int = 30,
) -> Optional[float]:
    if not api_key:
        return None
    if usage_path is not None:
        increment_fmp_usage(usage_path)
    url = f"https://financialmodelingprep.com/api/v3/market-capitalization/{symbol}?apikey={api_key}"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    if not isinstance(data, list) or not data:
        return None
    market_cap = data[0].get("marketCap")
    if market_cap is None:
        return None
    try:
        return float(market_cap)
    except (TypeError, ValueError):
        return None
