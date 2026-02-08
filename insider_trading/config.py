from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib  # py311+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib


@dataclass
class Weights:
    trade_value: float = 1.0
    position_increase: float = 1.0
    market_cap: float = 1.0
    industry: float = 0.5
    cannibal: float = 0.5


@dataclass
class Config:
    user_agent: str = "InsiderTradingTracker your.email@example.com"
    data_dir: Path = Path("data")
    output_dir: Path = Path("outputs")

    min_trade_value_usd: float = 1_000_000
    min_position_increase_pct: float = 10.0
    max_market_cap_usd: float = 500_000_000
    require_market_cap: bool = False

    required_trans_code: str = "P"
    required_acq_disp_code: str = "A"

    exclude_security_title_keywords: list[str] = field(
        default_factory=lambda: ["option", "derivative", "restricted stock unit", "rsu"]
    )

    industry_keywords: list[str] = field(
        default_factory=lambda: [
            "biotech",
            "pharma",
            "therapeutics",
            "gold",
            "mining",
            "exploration",
        ]
    )

    cannibal_min_reduction_pct: float = 2.0

    sec_sleep_seconds: float = 0.12
    stooq_symbol_suffix: str = ".us"

    weights: Weights = field(default_factory=Weights)


def _merge_dict(target: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_dict(target[key], value)
        else:
            target[key] = value
    return target


def load_config(path: Path | None) -> Config:
    base = Config()
    if not path:
        return base
    data = tomllib.loads(Path(path).read_text(encoding="utf-8"))

    # map nested config
    user_agent = data.get("user_agent", base.user_agent)

    min_req = data.get("min_requirements", {})
    weights = data.get("weights", {})
    industry = data.get("industry", {})
    cannibal = data.get("cannibal", {})
    fetch = data.get("fetch", {})

    cfg = Config(
        user_agent=user_agent,
        data_dir=Path(data.get("data_dir", base.data_dir)),
        output_dir=Path(data.get("output_dir", base.output_dir)),
        min_trade_value_usd=float(min_req.get("min_trade_value_usd", base.min_trade_value_usd)),
        min_position_increase_pct=float(
            min_req.get("min_position_increase_pct", base.min_position_increase_pct)
        ),
        max_market_cap_usd=float(min_req.get("max_market_cap_usd", base.max_market_cap_usd)),
        require_market_cap=bool(min_req.get("require_market_cap", base.require_market_cap)),
        weights=Weights(
            trade_value=float(weights.get("trade_value", base.weights.trade_value)),
            position_increase=float(weights.get("position_increase", base.weights.position_increase)),
            market_cap=float(weights.get("market_cap", base.weights.market_cap)),
            industry=float(weights.get("industry", base.weights.industry)),
            cannibal=float(weights.get("cannibal", base.weights.cannibal)),
        ),
        industry_keywords=list(industry.get("keywords", base.industry_keywords)),
        cannibal_min_reduction_pct=float(
            cannibal.get("min_reduction_pct", base.cannibal_min_reduction_pct)
        ),
        sec_sleep_seconds=float(fetch.get("sec_sleep_seconds", base.sec_sleep_seconds)),
    )

    return cfg
