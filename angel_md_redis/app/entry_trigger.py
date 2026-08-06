from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .logging_setup import setup_logger

log = setup_logger("entry_trigger")

_BULLISH_VOLUME = frozenset({"Bullish Volume", "Strong Bullish Volume"})
_BEARISH_VOLUME = frozenset({"Bearish Volume", "Strong Bearish Volume"})


@dataclass(frozen=True)
class EntryTriggerResult:
    signal: str  # "BUY CALL" / "BUY PUT" / "NEUTRAL"
    strength: str  # "strong" / "base" / ""
    level: str  # "P" / "R1" / "S1" / ...
    reason: str = ""
    oi_target_strike: Optional[float] = None  # OI resistance (CALL) / support (PUT), if available


def entry_trigger(
    st_bias: str,
    ema_state: str,
    level_signal: str,
    level: str,
    strength: str = "",
    htf_bias: str = "",
    volume_signal: str = "",
    oi_positioning: str = "",
    oi_resistance: Optional[float] = None,
    oi_support: Optional[float] = None,
    greeks_phase_ce: str = "",
    greeks_phase_pe: str = "",
) -> EntryTriggerResult:
    """
    Final entry: HTF D/W/M + Supertrend bias + EMA9/26 + pivot level break
    + volume + OI positioning + Greeks phase (Market Breadth -> OI Flow ->
    Volume Imbalance -> Greeks Analyzer -> Strike Selection chain).

    CALL: HTF CALL + ST CALL + EMA bullish + break P (base) or R1 (strong)
          + Bullish / Strong Bullish Volume + BULLISH_POSITIONING (OI)
          + ATM CE Greeks phase == MARKUP
    PUT:  HTF PUT  + ST PUT  + EMA bearish + break P (base) or S1 (strong)
          + Bearish / Strong Bearish Volume + BEARISH_POSITIONING (OI)
          + ATM PE Greeks phase == MARKUP

    When OI positioning aligns, the fired result carries oi_target_strike
    (the OI resistance level for a CALL breakout, or OI support level for a
    PUT breakdown) so run_strike_select.py can prefer that strike over the
    default ATM/offset logic — "select strike near resistance breakout"
    per the integration brief's worked example.
    """
    htf = (htf_bias or "").strip().upper()
    bias = (st_bias or "").strip().upper()
    state = (ema_state or "").strip().lower()
    lvl_sig = (level_signal or "").strip().upper()
    lvl = (level or "").strip().upper()
    strength_in = (strength or "").strip().lower()
    vol = (volume_signal or "").strip()
    oi_pos = (oi_positioning or "").strip().upper()
    gp_ce = (greeks_phase_ce or "").strip().upper()
    gp_pe = (greeks_phase_pe or "").strip().upper()

    call_ok = htf == "CALL" and bias == "CALL" and state == "bullish"
    put_ok = htf == "PUT" and bias == "PUT" and state == "bearish"
    vol_call_ok = vol in _BULLISH_VOLUME
    vol_put_ok = vol in _BEARISH_VOLUME
    oi_call_ok = oi_pos == "BULLISH_POSITIONING"
    oi_put_ok = oi_pos == "BEARISH_POSITIONING"
    greeks_call_ok = gp_ce == "MARKUP"
    greeks_put_ok = gp_pe == "MARKUP"

    log.debug(
        "LOGIC_IN htf=%s st=%s ema=%s lvl_sig=%s lvl=%s strength=%s vol=%s oi_pos=%s "
        "gp_ce=%s gp_pe=%s call_ok=%s put_ok=%s vol_call_ok=%s vol_put_ok=%s "
        "oi_call_ok=%s oi_put_ok=%s greeks_call_ok=%s greeks_put_ok=%s",
        htf, bias, state, lvl_sig, lvl, strength_in, vol, oi_pos,
        gp_ce, gp_pe, call_ok, put_ok, vol_call_ok, vol_put_ok,
        oi_call_ok, oi_put_ok, greeks_call_ok, greeks_put_ok,
    )

    if call_ok and vol_call_ok and oi_call_ok and greeks_call_ok and lvl_sig == "BUY CALL" and lvl in ("P", "R1"):
        out_strength = strength_in or ("strong" if lvl == "R1" else "base")
        if vol.startswith("Strong"):
            out_strength = "strong"
        result = EntryTriggerResult("BUY CALL", out_strength, lvl, "aligned_call", oi_target_strike=oi_resistance)
        log.debug("LOGIC_OUT %s", result)
        return result

    if put_ok and vol_put_ok and oi_put_ok and greeks_put_ok and lvl_sig == "BUY PUT" and lvl in ("P", "S1"):
        out_strength = strength_in or ("strong" if lvl == "S1" else "base")
        if vol.startswith("Strong"):
            out_strength = "strong"
        result = EntryTriggerResult("BUY PUT", out_strength, lvl, "aligned_put", oi_target_strike=oi_support)
        log.debug("LOGIC_OUT %s", result)
        return result

    reasons = []
    if not call_ok and not put_ok:
        reasons.append(f"filters_fail(htf={htf},st={bias},ema={state})")
    elif call_ok and not vol_call_ok:
        reasons.append(f"volume_fail(vol={vol})")
    elif put_ok and not vol_put_ok:
        reasons.append(f"volume_fail(vol={vol})")
    elif call_ok and not oi_call_ok:
        reasons.append(f"oi_positioning_fail(oi_pos={oi_pos})")
    elif put_ok and not oi_put_ok:
        reasons.append(f"oi_positioning_fail(oi_pos={oi_pos})")
    elif call_ok and not greeks_call_ok:
        reasons.append(f"greeks_phase_fail(phase_ce={gp_ce})")
    elif put_ok and not greeks_put_ok:
        reasons.append(f"greeks_phase_fail(phase_pe={gp_pe})")
    elif call_ok and (lvl_sig != "BUY CALL" or lvl not in ("P", "R1")):
        reasons.append(f"call_level_mismatch(sig={lvl_sig},lvl={lvl})")
    elif put_ok and (lvl_sig != "BUY PUT" or lvl not in ("P", "S1")):
        reasons.append(f"put_level_mismatch(sig={lvl_sig},lvl={lvl})")
    else:
        reasons.append("neutral")

    result = EntryTriggerResult("NEUTRAL", "", lvl if lvl else "", "|".join(reasons))
    log.debug("LOGIC_OUT %s", result)
    return result
