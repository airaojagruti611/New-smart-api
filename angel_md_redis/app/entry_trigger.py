from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .logging_setup import setup_logger

log = setup_logger("entry_trigger")

_BULLISH_VOLUME = frozenset({"Bullish Volume", "Strong Bullish Volume"})
_BEARISH_VOLUME = frozenset({"Bearish Volume", "Strong Bearish Volume"})
_CALL_LEVELS = frozenset({"P", "R1", "R2"})
_PUT_LEVELS = frozenset({"P", "S1", "S2"})


@dataclass(frozen=True)
class EntryTriggerResult:
    signal: str  # "BUY CALL" / "BUY PUT" / "NEUTRAL"
    strength: str  # "strong" / "base" / ""
    level: str  # "R1" / "R2" / "P" / "S1" / "S2" / ""
    reason: str = ""
    oi_target_strike: Optional[float] = None
    module1_signal: str = "NEUTRAL"
    module1_reason: str = ""


def _module1_decision(
    bias: str,
    state: str,
    lvl_sig: str,
    lvl: str,
    strength_in: str,
) -> tuple[str, str, str]:
    """
    Spec Module 1 only: Supertrend + EMA9/26 + pivot break.
    HTF / volume / OI / Greeks are applied later as confirmation gates.
    """
    strong_call = lvl in ("R1", "R2") or strength_in == "strong"
    strong_put = lvl in ("S1", "S2") or strength_in == "strong"

    if bias == "CALL" and state == "bullish" and lvl_sig == "BUY CALL" and lvl in _CALL_LEVELS:
        out_strength = strength_in or ("strong" if strong_call else "base")
        return "BUY CALL", out_strength, f"st_ema_level({lvl})"
    if bias == "PUT" and state == "bearish" and lvl_sig == "BUY PUT" and lvl in _PUT_LEVELS:
        out_strength = strength_in or ("strong" if strong_put else "base")
        return "BUY PUT", out_strength, f"st_ema_level({lvl})"

    parts = []
    if bias not in ("CALL", "PUT"):
        parts.append(f"st={bias or 'na'}")
    if state not in ("bullish", "bearish"):
        parts.append(f"ema={state or 'na'}")
    if bias == "CALL" and state != "bullish":
        parts.append(f"ema_not_bullish({state or 'na'})")
    if bias == "PUT" and state != "bearish":
        parts.append(f"ema_not_bearish({state or 'na'})")
    if lvl_sig not in ("BUY CALL", "BUY PUT"):
        parts.append(f"no_pivot_break({lvl_sig or 'na'})")
    elif bias == "CALL" and (lvl_sig != "BUY CALL" or lvl not in _CALL_LEVELS):
        parts.append(f"call_level_mismatch({lvl_sig},{lvl})")
    elif bias == "PUT" and (lvl_sig != "BUY PUT" or lvl not in _PUT_LEVELS):
        parts.append(f"put_level_mismatch({lvl_sig},{lvl})")
    if not parts:
        parts.append("unaligned")
    return "NEUTRAL", "", "|".join(parts)


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
    Module 1 (ST + EMA + pivot) is computed independently of confirmation
    gates (HTF, volume, OI, Greeks MARKUP). Final BUY requires both.
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

    m1_signal, m1_strength, m1_reason = _module1_decision(
        bias, state, lvl_sig, lvl, strength_in
    )

    vol_call_ok = vol in _BULLISH_VOLUME
    vol_put_ok = vol in _BEARISH_VOLUME
    oi_call_ok = oi_pos == "BULLISH_POSITIONING"
    oi_put_ok = oi_pos == "BEARISH_POSITIONING"
    greeks_call_ok = gp_ce == "MARKUP"
    greeks_put_ok = gp_pe == "MARKUP"
    htf_call_ok = htf == "CALL"
    htf_put_ok = htf == "PUT"

    log.info(
        "GATES module1=%s(%s) htf=%s st=%s ema=%s vol=%s oi=%s gp_ce=%s gp_pe=%s lvl=%s/%s",
        m1_signal,
        m1_reason,
        htf or "na",
        bias or "na",
        state or "na",
        vol or "na",
        oi_pos or "na",
        gp_ce or "na",
        gp_pe or "na",
        lvl_sig or "na",
        lvl or "na",
    )

    if m1_signal == "BUY CALL":
        fails = []
        if not htf_call_ok:
            fails.append(f"htf_fail({htf or 'na'})")
        if not vol_call_ok:
            fails.append(f"volume_fail({vol or 'na'})")
        if not oi_call_ok:
            fails.append(f"oi_positioning_fail({oi_pos or 'na'})")
        if not greeks_call_ok:
            fails.append(f"greeks_phase_fail(phase_ce={gp_ce or 'na'})")
        if not fails:
            out_strength = m1_strength
            if vol.startswith("Strong"):
                out_strength = "strong"
            result = EntryTriggerResult(
                "BUY CALL",
                out_strength,
                lvl,
                "aligned_call",
                oi_target_strike=oi_resistance,
                module1_signal=m1_signal,
                module1_reason=m1_reason,
            )
            log.info("LOGIC_OUT %s", result)
            return result
        result = EntryTriggerResult(
            "NEUTRAL",
            "",
            lvl,
            "|".join(fails),
            module1_signal=m1_signal,
            module1_reason=m1_reason,
        )
        log.info("LOGIC_OUT %s", result)
        return result

    if m1_signal == "BUY PUT":
        fails = []
        if not htf_put_ok:
            fails.append(f"htf_fail({htf or 'na'})")
        if not vol_put_ok:
            fails.append(f"volume_fail({vol or 'na'})")
        if not oi_put_ok:
            fails.append(f"oi_positioning_fail({oi_pos or 'na'})")
        if not greeks_put_ok:
            fails.append(f"greeks_phase_fail(phase_pe={gp_pe or 'na'})")
        if not fails:
            out_strength = m1_strength
            if vol.startswith("Strong"):
                out_strength = "strong"
            result = EntryTriggerResult(
                "BUY PUT",
                out_strength,
                lvl,
                "aligned_put",
                oi_target_strike=oi_support,
                module1_signal=m1_signal,
                module1_reason=m1_reason,
            )
            log.info("LOGIC_OUT %s", result)
            return result
        result = EntryTriggerResult(
            "NEUTRAL",
            "",
            lvl,
            "|".join(fails),
            module1_signal=m1_signal,
            module1_reason=m1_reason,
        )
        log.info("LOGIC_OUT %s", result)
        return result

    result = EntryTriggerResult(
        "NEUTRAL",
        "",
        lvl if lvl else "",
        m1_reason,
        module1_signal=m1_signal,
        module1_reason=m1_reason,
    )
    log.info("LOGIC_OUT %s", result)
    return result
