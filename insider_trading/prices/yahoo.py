from __future__ import annotations

from time import sleep
from typing import Optional

import requests


def _get_json(url: str, timeout: int = 30, sleep_seconds: float = 0.0) -> Optional[dict]:
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException:
        return None
    if sleep_seconds:
        sleep(sleep_seconds)
    try:
        return resp.json()
    except ValueError:
        return None


def fetch_last_price(symbol: str, timeout: int = 30, sleep_seconds: float = 0.0) -> Optional[float]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
    data = _get_json(url, timeout=timeout, sleep_seconds=sleep_seconds)
    if not data:
        return None
    result = (data.get("chart") or {}).get("result") or []
    if not result:
        return None
    meta = result[0].get("meta") or {}
    price = meta.get("regularMarketPrice")
    if price is not None:
        try:
            return float(price)
        except (TypeError, ValueError):
            return None
    indicators = result[0].get("indicators") or {}
    quotes = indicators.get("quote") or []
    if not quotes:
        return None
    closes = quotes[0].get("close") or []
    for value in reversed(closes):
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def fetch_market_cap(symbol: str, timeout: int = 30, sleep_seconds: float = 0.0) -> Optional[float]:
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=price"
    data = _get_json(url, timeout=timeout, sleep_seconds=sleep_seconds)
    if not data:
        return None
    result = (data.get("quoteSummary") or {}).get("result") or []
    if not result:
        return None
    price = result[0].get("price") or {}
    market_cap = price.get("marketCap") or {}
    raw = market_cap.get("raw")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
