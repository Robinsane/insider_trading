from insider_trading.config import Config
from insider_trading.scoring import score_row


def test_score_positive():
    cfg = Config()
    row = {
        "trade_value": cfg.min_trade_value_usd * 2,
        "position_increase_pct": cfg.min_position_increase_pct * 2,
        "market_cap_usd": cfg.max_market_cap_usd - 1,
        "sic_description": "Biotech",
        "share_count_reduction_pct": cfg.cannibal_min_reduction_pct + 1,
    }
    score = score_row(row, cfg)
    assert score > 0
