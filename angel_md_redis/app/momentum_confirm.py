from __future__ import annotations

from .logging_setup import setup_logger

log = setup_logger("momentum_confirm")


def momentum_confirm(st_bias: str, ema_state: str) -> str:
    """
    Combine Supertrend bias with EMA9/EMA26 alignment.

    Bullish: Supertrend CALL + EMA9 > EMA26  -> BUY CALL
    Bearish: Supertrend PUT  + EMA9 < EMA26  -> BUY PUT
    Else: NEUTRAL
    """
    bias = (st_bias or "").strip().upper()
    state = (ema_state or "").strip().lower()

    if bias == "CALL" and state == "bullish":
        out = "BUY CALL"
    elif bias == "PUT" and state == "bearish":
        out = "BUY PUT"
    else:
        out = "NEUTRAL"

    log.debug("LOGIC_IN st=%s ema=%s -> %s", bias, state, out)
    return out
