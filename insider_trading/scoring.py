from __future__ import annotations

from typing import Any

from .config import Config


def industry_boost(row: dict[str, Any], cfg: Config) -> float:
    desc = (row.get("sic_description") or "").lower()
    for keyword in cfg.industry_keywords:
        if keyword.lower() in desc:
            return 1.0
    return 0.0


def cannibal_boost(row: dict[str, Any], cfg: Config) -> float:
    reduction = row.get("share_count_reduction_pct")
    if reduction is None:
        return 0.0
    return 1.0 if reduction >= cfg.cannibal_min_reduction_pct else 0.0


def score_row(row: dict[str, Any], cfg: Config) -> float:
    score = 0.0
    trade_value = row.get("trade_value") or 0.0
    position_inc = row.get("position_increase_pct") or 0.0

    score += min(trade_value / cfg.min_trade_value_usd, 3.0) * cfg.weights.trade_value
    score += (
        min(position_inc / cfg.min_position_increase_pct, 3.0) * cfg.weights.position_increase
    )

    market_cap = row.get("market_cap_usd")
    if market_cap is not None and market_cap <= cfg.max_market_cap_usd:
        score += 1.0 * cfg.weights.market_cap

    score += industry_boost(row, cfg) * cfg.weights.industry
    score += cannibal_boost(row, cfg) * cfg.weights.cannibal

    return round(score, 4)
