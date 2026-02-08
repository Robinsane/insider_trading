from __future__ import annotations

from typing import Any

from .config import Config


def is_open_market_purchase(row: dict[str, Any], cfg: Config) -> bool:
    return (
        row.get("TRANS_CODE") == cfg.required_trans_code
        and row.get("TRANS_ACQUIRED_DISP_CD") == cfg.required_acq_disp_code
    )


def is_material_trade(row: dict[str, Any], cfg: Config) -> bool:
    trade_value = row.get("trade_value")
    if trade_value is None:
        return False
    return trade_value >= cfg.min_trade_value_usd


def passes_position_increase(row: dict[str, Any], cfg: Config) -> bool:
    inc = row.get("position_increase_pct")
    if inc is None:
        return False
    return inc >= cfg.min_position_increase_pct


def passes_security_title_filter(row: dict[str, Any], cfg: Config) -> bool:
    title = (row.get("SECURITY_TITLE") or "").lower()
    for keyword in cfg.exclude_security_title_keywords:
        if keyword.lower() in title:
            return False
    return True


def passes_market_cap(row: dict[str, Any], cfg: Config) -> bool:
    market_cap = row.get("market_cap_usd")
    if market_cap is None:
        return not cfg.require_market_cap
    return market_cap <= cfg.max_market_cap_usd


def basic_filters(row: dict[str, Any], cfg: Config) -> bool:
    return (
        is_open_market_purchase(row, cfg)
        and passes_security_title_filter(row, cfg)
        and is_material_trade(row, cfg)
        and passes_position_increase(row, cfg)
        and passes_market_cap(row, cfg)
    )
