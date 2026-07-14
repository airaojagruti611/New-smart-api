from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .logging_setup import setup_logger
from .scripmaster import build_atm_option_tokens

log = setup_logger("strike_select")


@dataclass(frozen=True)
class StrikeSelectResult:
    status: str  # "OK" / "SKIP"
    signal: str
    side: str  # "CE" / "PE" / ""
    mode: str  # "atm" / "otm1" / ... / "oi_target"
    spot: float
    atm: float
    strike: float
    step: float
    expiry: str
    token: str
    tradingsymbol: str
    exchange: str
    reason: str


def _skip(
    signal: str,
    reason: str,
    spot: float = 0.0,
) -> StrikeSelectResult:
    return StrikeSelectResult(
        status="SKIP",
        signal=signal,
        side="",
        mode="",
        spot=spot,
        atm=0.0,
        strike=0.0,
        step=0.0,
        expiry="",
        token="",
        tradingsymbol="",
        exchange="",
        reason=reason,
    )


def strike_select(
    signal: str,
    strength: str,
    underlying: str,
    spot: float,
    df: pd.DataFrame,
    offset_base: int = 0,
    offset_strong: int = 1,
    oi_target_strike: Optional[float] = None,
) -> StrikeSelectResult:
    """
    Map entry signal -> ATM, slight-OTM, or OI-target option contract.

    BUY CALL: CE; base -> ATM, strong -> +offset_strong steps (OTM)
    BUY PUT:  PE; base -> ATM, strong -> -offset_strong steps (OTM)

    When oi_target_strike is provided (OI resistance level for a CALL
    breakout, or OI support level for a PUT breakdown — passed through from
    entry_trigger's OI positioning check), it OVERRIDES the offset-based
    target: "select strike near resistance breakout" per the integration
    brief's worked example. The strikes_around window fetched from
    ScripMaster must be wide enough to actually contain that strike, or
    the nearest available contract is chosen instead (best-effort, not a
    skip) — a target a full ScripMaster fetch away is a config issue
    (STRIKES_AROUND too narrow), not a signal to discard the trade.
    """
    sig = (signal or "").strip().upper()
    strength_l = (strength or "").strip().lower()
    sym = (underlying or "").strip().upper()

    log.debug(
        "LOGIC_IN signal=%s strength=%s underlying=%s spot=%s offset_base=%s offset_strong=%s oi_target=%s",
        sig, strength_l, sym, spot, offset_base, offset_strong, oi_target_strike,
    )

    if not sym:
        result = _skip(sig or "NEUTRAL", "missing_symbol", spot)
        log.debug("LOGIC_OUT %s", result)
        return result
    if spot is None or spot <= 0:
        result = _skip(sig or "NEUTRAL", "invalid_spot", float(spot or 0.0))
        log.debug("LOGIC_OUT %s", result)
        return result

    if sig == "BUY CALL":
        cp = "CE"
        direction = 1
    elif sig == "BUY PUT":
        cp = "PE"
        direction = -1
    else:
        result = _skip(sig or "NEUTRAL", "not_buy_signal", float(spot))
        log.debug("LOGIC_OUT %s", result)
        return result

    offset = offset_strong if strength_l == "strong" else offset_base
    try:
        offset = int(offset)
    except Exception:
        offset = 0
    offset = abs(offset)

    # Widen the fetch window when an OI target implies more strikes-out
    # than the offset alone would request, so build_atm_option_tokens
    # actually has that contract available to choose from.
    around = max(offset, 0)
    if oi_target_strike and spot > 0:
        approx_steps = int(abs(oi_target_strike - spot) / max(spot * 0.005, 0.01))
        around = max(around, min(approx_steps, 10))

    contracts, expiry = build_atm_option_tokens(df, sym, float(spot), around)
    if not contracts or not expiry:
        result = _skip(sig, "no_option_chain", float(spot))
        log.debug("LOGIC_OUT %s", result)
        return result

    side_rows = [c for c in contracts if str(c.get("cp") or "").upper() == cp]
    if not side_rows:
        result = _skip(sig, f"no_{cp}_contracts", float(spot))
        log.debug("LOGIC_OUT %s", result)
        return result

    # ATM = strike closest to spot among this side.
    atm_row = min(side_rows, key=lambda c: abs(float(c["strike"]) - float(spot)))
    atm = float(atm_row["strike"])

    strikes = sorted({float(c["strike"]) for c in side_rows})
    step = 0.0
    if len(strikes) >= 2:
        diffs = [round(strikes[i + 1] - strikes[i], 6) for i in range(len(strikes) - 1)]
        diffs = [d for d in diffs if d > 0]
        step = min(diffs) if diffs else 0.0

    use_oi_target = oi_target_strike is not None and oi_target_strike > 0
    if use_oi_target:
        target = float(oi_target_strike)
        mode = "oi_target"
    else:
        target = atm + (direction * offset * step) if step > 0 else atm
        mode = "atm" if offset == 0 else f"otm{offset}"

    chosen = min(side_rows, key=lambda c: abs(float(c["strike"]) - target))

    result = StrikeSelectResult(
        status="OK",
        signal=sig,
        side=cp,
        mode=mode,
        spot=float(spot),
        atm=atm,
        strike=float(chosen["strike"]),
        step=float(step),
        expiry=str(expiry),
        token=str(chosen.get("token") or ""),
        tradingsymbol=str(chosen.get("tradingsymbol") or ""),
        exchange=str(chosen.get("exchange") or "NFO"),
        reason="ok",
    )
    log.debug("LOGIC_OUT %s", result)
    return result
