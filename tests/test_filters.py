from insider_trading.config import Config
from insider_trading.filters import basic_filters


def test_basic_filters_pass():
    cfg = Config()
    row = {
        "TRANS_CODE": "P",
        "TRANS_ACQUIRED_DISP_CD": "A",
        "SECURITY_TITLE": "Common Stock",
        "trade_value": cfg.min_trade_value_usd + 1,
        "position_increase_pct": cfg.min_position_increase_pct + 1,
        "market_cap_usd": cfg.max_market_cap_usd - 1,
    }
    assert basic_filters(row, cfg) is True


def test_basic_filters_fail():
    cfg = Config()
    row = {
        "TRANS_CODE": "S",
        "TRANS_ACQUIRED_DISP_CD": "D",
        "SECURITY_TITLE": "Option",
        "trade_value": cfg.min_trade_value_usd - 1,
        "position_increase_pct": cfg.min_position_increase_pct - 1,
    }
    assert basic_filters(row, cfg) is False
