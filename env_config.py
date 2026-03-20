from __future__ import annotations
import os
from typing import Dict, Any
from dotenv import load_dotenv


def str_to_bool(value: str) -> bool:
    return value.lower() in ('true', '1', 'yes', 'on')


def load_env_config() -> Dict[str, Any]:
    load_dotenv()

    def get_env(key: str, default: Any = None) -> str:
        value = os.getenv(key)
        if value is None:
            if default is not None:
                return str(default)
            raise ValueError(f"Missing: {key}")
        return value

    symbols_str = get_env("HIBACHI_SYMBOLS", get_env("HIBACHI_SYMBOL", "BTC/USDT-P"))
    symbols = [s.strip() for s in symbols_str.split(",") if s.strip()]

    return {
        "api": {
            "apiUrl": get_env("HIBACHI_API_ENDPOINT",
                              "https://api.hibachi.xyz"),
            "dataApiUrl": get_env("HIBACHI_DATA_API_ENDPOINT",
                                  "https://data-api.hibachi.xyz"),
            "apiKey": get_env("HIBACHI_API_KEY"),
            "accountId": get_env("HIBACHI_ACCOUNT_ID"),
            "privateKey": get_env("HIBACHI_PRIVATE_KEY"),
            "publicKey": get_env("HIBACHI_PUBLIC_KEY", ""),
            "useWebsocket": str_to_bool(get_env("USE_WEBSOCKET", "false"))
        },
        "bot": {
            "symbol": symbols[0],
            "symbols": symbols,
            "baseOrderPct": float(get_env("BASE_ORDER_PCT", "1.0")),
            "invBudgetPct": float(get_env("INV_BUDGET_PCT", "30.0")),
            "slipGuardATR": float(get_env("SLIP_GUARD_ATR", "3.0")),
            "minVol": int(get_env("MIN_VOL", "0")),
            "longBiasOnly": str_to_bool(get_env("LONG_BIAS_ONLY", "false")),
            "atrLen": int(get_env("ATR_LEN", "14")),
            "atrTimeframe": get_env("ATR_TIMEFRAME", "5m"),
            "kATR": float(get_env("K_ATR", "0.75")),
            "minFullBps": float(get_env("MIN_FULL_BPS", "50.0")),
            "maxFullBps": float(get_env("MAX_FULL_BPS", "400.0")),
            "skewDamp": float(get_env("SKEW_DAMP", "0.30")),
            "sizeAmp": float(get_env("SIZE_AMP", "1.5")),
            "useBullBias": str_to_bool(get_env("USE_BULL_BIAS", "false")),
            "bullBiasBps": float(get_env("BULL_BIAS_BPS", "25.0")),
            "requoteBps": float(get_env("REQUOTE_BPS", "10.0")),
            "postOnly": str_to_bool(get_env("POST_ONLY", "true")),
            "timeInForce": get_env("TIME_IN_FORCE", "GTC"),
            "minNotional": float(get_env("MIN_NOTIONAL", "10.0")),
            "leverage": int(get_env("LEVERAGE", "1")),
            "creationDeadline": int(get_env("CREATION_DEADLINE", "0"))
        },
        "logging": {
            "level": get_env("LOG_LEVEL", "INFO"),
            "dir": get_env("LOG_DIR", "logs")
        }
    }


def validate_config(config: Dict[str, Any]) -> bool:
    bot = config["bot"]

    assert 0.1 <= bot["baseOrderPct"] <= 100.0, "baseOrderPct must be 0.1-100"
    assert 10.0 <= bot["invBudgetPct"] <= 100.0, "invBudgetPct must be 10-100"
    assert bot["atrLen"] >= 1, "atrLen must be >= 1"
    assert bot["kATR"] >= 0.1, "kATR must be >= 0.1"
    assert bot["minFullBps"] >= 5.0, "minFullBps must be >= 5"
    assert bot["maxFullBps"] > bot["minFullBps"], "maxFullBps must be > minFullBps"
    assert 0.0 <= bot["skewDamp"] <= 1.0, "skewDamp must be 0-1"
    assert bot["sizeAmp"] >= 0.5, "sizeAmp must be >= 0.5"
    assert 1.0 <= bot["requoteBps"] <= 100.0, "requoteBps must be 1-100"
    assert 1 <= bot["leverage"] <= 100, "leverage must be 1-100"
    assert bot["minNotional"] >= 1.0, "minNotional must be >= 1"

    for sym in bot.get("symbols", [bot["symbol"]]):
        assert "/" in sym, f"symbol '{sym}' must be in format 'BASE/QUOTE-P' (e.g. BTC/USDT-P)"

    valid_timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d']
    assert bot["atrTimeframe"] in valid_timeframes, \
        f"atrTimeframe must be one of: {', '.join(valid_timeframes)}"

    if bot["useBullBias"]:
        assert bot["bullBiasBps"] < bot["minFullBps"], \
            "bullBiasBps should be < minFullBps to avoid excessive skew"

    return True