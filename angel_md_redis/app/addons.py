"""
Add-on signals (pure logic, no I/O).

One function per add-on; each returns a dict. Missing/insufficient inputs never
raise: the relevant fields are None and a "reason" string explains why.
Candles are app.candle_types.Candle (ts_ms = bar close, epoch ms), oldest-first.
"""

from __future__ import annotations

import datetime as dt
import statistics
from typing import Dict, List, Optional, Sequence

from .candle_types import Candle

_IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def ist_dt(ts_ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=_IST)


def ist_date(ts_ms: int) -> dt.date:
    return ist_dt(ts_ms).date()


def minute_of_day(ts_ms: int) -> int:
    d = ist_dt(ts_ms)
    return d.hour * 60 + d.minute


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if b is None or b == 0 or a is None:
        return None
    return (a / b - 1.0) * 100.0


def _unavailable(reason: str, **fields) -> dict:
    out = {"available": False, "reason": reason, "signal": None}
    out.update(fields)
    return out


def buy_sell_split(c: Candle) -> tuple:
    """Split candle volume by close position in range (same rule as VolumeAnalyzer)."""
    rng = c.h - c.l
    if c.v <= 0:
        return 0.0, 0.0
    if rng <= 0:
        return c.v / 2.0, c.v / 2.0
    buy = c.v * (c.c - c.l) / rng
    return buy, c.v - buy


# 1 ──────────────────────────────────────────────────────────────────────────
def circuit_proximity(ltp: Optional[float], upper: Optional[float], lower: Optional[float],
                      near_pct: float = 1.0) -> dict:
    """Distance to upper/lower circuit (% of ltp) and near/hit flags."""
    if ltp is None or ltp <= 0:
        return _unavailable("no_ltp")
    if not upper and not lower:
        return _unavailable("no_circuit_limits")
    du = (upper - ltp) / ltp * 100.0 if upper else None
    dl = (ltp - lower) / ltp * 100.0 if lower else None
    hit_u = bool(upper) and ltp >= upper
    hit_l = bool(lower) and ltp <= lower
    near_u = bool(du is not None and not hit_u and du <= near_pct)
    near_l = bool(dl is not None and not hit_l and dl <= near_pct)
    signal = ("UPPER_CIRCUIT_HIT" if hit_u else "LOWER_CIRCUIT_HIT" if hit_l else
              "NEAR_UPPER_CIRCUIT" if near_u else "NEAR_LOWER_CIRCUIT" if near_l else "NONE")
    return {"available": True, "reason": None, "signal": signal, "ltp": ltp,
            "upper": upper or None, "lower": lower or None,
            "dist_upper_pct": du, "dist_lower_pct": dl,
            "near_upper": near_u, "near_lower": near_l, "hit_upper": hit_u, "hit_lower": hit_l}


# 2 ──────────────────────────────────────────────────────────────────────────
def daily_volume_gain(today_volume: Optional[float], prev_daily_volumes: Sequence[float],
                      window: int = 20, min_days: int = 5) -> dict:
    """Today's (cumulative) volume % gain vs SMA of the previous `window` daily volumes."""
    if today_volume is None:
        return _unavailable("no_today_volume")
    hist = [v for v in prev_daily_volumes if v and v > 0][-window:]
    if len(hist) < min_days:
        return _unavailable("insufficient_daily_history", days_used=len(hist))
    sma = sum(hist) / len(hist)
    gain = _pct(today_volume, sma)
    return {"available": True, "reason": None, "signal": "VOLUME_UP" if gain > 0 else "VOLUME_DOWN",
            "today_volume": today_volume, "sma_volume": sma, "gain_pct": gain, "days_used": len(hist)}


# 3 ──────────────────────────────────────────────────────────────────────────
def box_breakout(candles: Sequence[Candle], window_min: int = 60, min_bars: int = 30,
                 buffer_pct: float = 0.0) -> dict:
    """Last candle vs the high/low box of the preceding `window_min` 1m bars."""
    if len(candles) < 2:
        return _unavailable("no_candles")
    last = candles[-1]
    start_ms = last.ts_ms - window_min * 60_000
    box = [c for c in candles[:-1] if start_ms < c.ts_ms < last.ts_ms]
    if len(box) < min_bars:
        return _unavailable("insufficient_box_bars", bars=len(box))
    hi, lo = max(c.h for c in box), min(c.l for c in box)
    up_lvl, dn_lvl = hi * (1 + buffer_pct / 100.0), lo * (1 - buffer_pct / 100.0)
    signal = "BOX_BREAKOUT" if last.c > up_lvl else "BOX_BREAKDOWN" if last.c < dn_lvl else "INSIDE_BOX"
    return {"available": True, "reason": None, "signal": signal, "box_high": hi, "box_low": lo,
            "box_range_pct": _pct(hi, lo), "close": last.c, "bars": len(box)}


# 4 ──────────────────────────────────────────────────────────────────────────
def rolling_sums(vols: Sequence[float], n: int) -> List[float]:
    """Non-overlapping n-bar volume sums, aligned to the newest bar."""
    out: List[float] = []
    i = len(vols)
    while i - n >= 0:
        out.append(sum(vols[i - n:i]))
        i -= n
    return out[::-1]


def volume_spike(current: Optional[float], history: Sequence[float], z_threshold: float = 3.0,
                 mult_threshold: float = 3.0, min_hist: int = 10) -> dict:
    """Current volume vs mean/stdev of history (multiple and z-score)."""
    if current is None:
        return _unavailable("no_current_volume")
    hist = [v for v in history if v is not None]
    if len(hist) < min_hist:
        return _unavailable("insufficient_history", samples=len(hist))
    mean = sum(hist) / len(hist)
    sd = statistics.pstdev(hist)
    if mean <= 0:
        return _unavailable("zero_mean_volume", samples=len(hist))
    mult = current / mean
    z = (current - mean) / sd if sd > 0 else None
    spike = mult >= mult_threshold or (z is not None and z >= z_threshold)
    return {"available": True, "reason": None, "signal": "VOLUME_SPIKE" if spike else "NORMAL",
            "current": current, "mean": mean, "stdev": sd, "multiple": mult, "zscore": z,
            "samples": len(hist)}


# 5 / 11 ─────────────────────────────────────────────────────────────────────
def sector_heatmap(stats: Dict[str, dict], sectors: Dict[str, str], threshold_pct: float = 10.0) -> dict:
    """
    stats: {sym: {"buy_vol": f, "sell_vol": f, "chg_pct": f|None}}.
    Per-sector buying/selling pressure from signed volume.
    """
    acc: Dict[str, dict] = {}
    unmapped = 0
    for sym, s in stats.items():
        sec = sectors.get(sym.upper())
        if not sec:
            unmapped += 1
            continue
        a = acc.setdefault(sec, {"buy_vol": 0.0, "sell_vol": 0.0, "symbols": 0})
        a["buy_vol"] += float(s.get("buy_vol") or 0)
        a["sell_vol"] += float(s.get("sell_vol") or 0)
        a["symbols"] += 1
    out = {}
    for sec, a in acc.items():
        tot = a["buy_vol"] + a["sell_vol"]
        net = (a["buy_vol"] - a["sell_vol"]) / tot * 100.0 if tot > 0 else None
        label = (None if net is None else "BUYING" if net >= threshold_pct else
                 "SELLING" if net <= -threshold_pct else "NEUTRAL")
        out[sec] = {**a, "net_pct": net, "label": label}
    return {"available": bool(out), "reason": None if out else "no_sector_data",
            "sectors": out, "unmapped_symbols": unmapped}


def sector_change(stats: Dict[str, dict], sectors: Dict[str, str]) -> dict:
    """Average % price change per sector (stats[sym]["chg_pct"])."""
    groups: Dict[str, List[float]] = {}
    for sym, s in stats.items():
        sec, chg = sectors.get(sym.upper()), s.get("chg_pct")
        if sec and chg is not None:
            groups.setdefault(sec, []).append(float(chg))
    out = {}
    for sec, vals in groups.items():
        out[sec] = {"avg_chg_pct": sum(vals) / len(vals), "median_chg_pct": statistics.median(vals),
                    "symbols": len(vals), "advancers": sum(1 for v in vals if v > 0),
                    "decliners": sum(1 for v in vals if v < 0)}
    ranked = sorted(out, key=lambda k: out[k]["avg_chg_pct"], reverse=True)
    return {"available": bool(out), "reason": None if out else "no_sector_data", "sectors": out,
            "ranked": ranked, "top": ranked[0] if ranked else None, "bottom": ranked[-1] if ranked else None}


# 6 ──────────────────────────────────────────────────────────────────────────
def premarket_analysis(prev_close: Optional[float], preopen_price: Optional[float],
                       preopen_volume: Optional[float] = None, open_price: Optional[float] = None,
                       gap_threshold_pct: float = 1.0) -> dict:
    """Pre-open (09:00-09:15 IST) gap vs previous close; open_price used if pre-open price missing."""
    if not prev_close or prev_close <= 0:
        return _unavailable("no_prev_close")
    px, src = (preopen_price, "preopen") if preopen_price else (open_price, "open")
    if not px:
        return _unavailable("no_preopen_data")
    gap = _pct(px, prev_close)
    signal = "GAP_UP" if gap >= gap_threshold_pct else "GAP_DOWN" if gap <= -gap_threshold_pct else "FLAT_OPEN"
    return {"available": True, "reason": None if preopen_volume is not None else "preopen_volume_unavailable",
            "signal": signal, "gap_pct": gap, "price": px, "price_source": src,
            "prev_close": prev_close, "preopen_volume": preopen_volume}


# 7 ──────────────────────────────────────────────────────────────────────────
def heavy_flow(candles: Sequence[Candle], n: int = 5, ratio_threshold: float = 0.65,
               tbq: Optional[float] = None, tsq: Optional[float] = None) -> dict:
    """Signed volume over last n minutes (+ optional total buy/sell qty imbalance)."""
    if len(candles) < n:
        return _unavailable("insufficient_candles", bars=len(candles))
    buy = sell = 0.0
    for c in candles[-n:]:
        b, s = buy_sell_split(c)
        buy += b
        sell += s
    tot = buy + sell
    if tot <= 0:
        return _unavailable("no_volume")
    ratio = buy / tot
    imb = (tbq - tsq) / (tbq + tsq) if tbq is not None and tsq is not None and (tbq + tsq) > 0 else None
    signal = ("HEAVY_BUYING" if ratio >= ratio_threshold else
              "HEAVY_SELLING" if ratio <= 1 - ratio_threshold else "BALANCED")
    return {"available": True, "reason": None, "signal": signal, "buy_volume": buy, "sell_volume": sell,
            "buy_ratio": ratio, "net_volume": buy - sell, "qty_imbalance": imb, "minutes": n}


# 8 ──────────────────────────────────────────────────────────────────────────
def fast_move(candles: Sequence[Candle], n: int = 5, threshold_pct: float = 1.0) -> dict:
    """% price move over last n minutes (close vs close n bars ago)."""
    if len(candles) < n + 1:
        return _unavailable("insufficient_candles", bars=len(candles))
    ref = candles[-n - 1].c
    move = _pct(candles[-1].c, ref)
    if move is None:
        return _unavailable("bad_reference_price")
    signal = "FAST_UP" if move >= threshold_pct else "FAST_DOWN" if move <= -threshold_pct else "NORMAL"
    return {"available": True, "reason": None, "signal": signal, "move_pct": move,
            "from_price": ref, "to_price": candles[-1].c, "minutes": n}


# 9 ──────────────────────────────────────────────────────────────────────────
def volume_breaker(candles: Sequence[Candle], lookback: int = 20, vol_mult: float = 2.0) -> dict:
    """Close breaks the prior lookback high/low on volume >= vol_mult x average."""
    if len(candles) < lookback + 1:
        return _unavailable("insufficient_candles", bars=len(candles))
    last, prior = candles[-1], candles[-lookback - 1:-1]
    hi, lo = max(c.h for c in prior), min(c.l for c in prior)
    avg = sum(c.v for c in prior) / len(prior)
    if avg <= 0:
        return _unavailable("zero_avg_volume")
    mult = last.v / avg
    big = mult >= vol_mult
    signal = ("VOLUME_BREAKER_UP" if last.c > hi and big else
              "VOLUME_BREAKER_DOWN" if last.c < lo and big else "NONE")
    return {"available": True, "reason": None, "signal": signal, "recent_high": hi, "recent_low": lo,
            "volume_multiple": mult, "close": last.c, "lookback": lookback}


# 10 ─────────────────────────────────────────────────────────────────────────
def intraday_volume_increase(today_cum: Optional[float], same_time_history: Sequence[float],
                             prev_day_total: Optional[float] = None, min_days: int = 3) -> dict:
    """Today's cumulative volume vs same-time-of-day average; fallback previous day's volume."""
    if today_cum is None:
        return _unavailable("no_today_volume")
    hist = [v for v in same_time_history if v and v > 0]
    if len(hist) >= min_days:
        ref, kind = sum(hist) / len(hist), "same_time_avg"
    elif prev_day_total and prev_day_total > 0:
        ref, kind = prev_day_total, "prev_day_total"
    else:
        return _unavailable("no_reference_volume")
    inc = _pct(today_cum, ref)
    return {"available": True, "reason": None, "signal": "VOLUME_UP" if inc > 0 else "VOLUME_DOWN",
            "today_cum_volume": today_cum, "reference_volume": ref, "reference": kind,
            "increase_pct": inc, "days_used": len(hist)}
