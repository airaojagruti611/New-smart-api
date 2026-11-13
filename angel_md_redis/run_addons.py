"""
run_addons.py
─────────────
Evaluates the 11 add-on signals (app/addons.py) per symbol on each closed 1m
candle from md:candles:1m. Daily context comes from md:candles:1d, tick fields
(prev close, cumulative volume, tbq/tsq, optional circuit limits) from the tail
of md:ticks:eq.

Output:
  Stream : md:addons:signal            (one entry per symbol per closed 1m candle)
  Keys   : md:addons:latest:{SYMBOL}   (full add-on dict, TTL)
           md:addons:sector_heatmap    (market-wide, add-on 5)
           md:addons:sector_change     (market-wide, add-on 11)
"""

from __future__ import annotations

import json
import os
import time
from collections import deque
from typing import Deque, Dict, List, Optional

import redis

from app import addons as A
from app.candle_io import parse_candle_fields, read_last_candles_multi
from app.candle_types import Candle
from app.config import load_symbols

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IN_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
IN_1D = os.getenv("STREAM_CANDLES_1D", "md:candles:1d")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OUT_STREAM = os.getenv("STREAM_ADDONS_SIGNAL", "md:addons:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_ADDONS", "20000"))
GROUP = os.getenv("ADDONS_GROUP", "addons")
CONSUMER = os.getenv("ADDONS_CONSUMER", "addons-1")
LATEST_PREFIX = os.getenv("ADDONS_LATEST_PREFIX", "md:addons:latest:")
HEATMAP_KEY = os.getenv("ADDONS_SECTOR_HEATMAP_KEY", "md:addons:sector_heatmap")
SECTOR_CHANGE_KEY = os.getenv("ADDONS_SECTOR_CHANGE_KEY", "md:addons:sector_change")
LATEST_TTL_SEC = int(os.getenv("ADDONS_LATEST_TTL_SEC", "3600"))
SECTORS_FILE = os.getenv("SECTORS_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "sectors.json"))
SEED_BARS = int(os.getenv("ADDONS_SEED_BARS", "400"))
TICK_SCAN = int(os.getenv("ADDONS_TICK_SCAN", "4000"))
HOUSEKEEP_SEC = float(os.getenv("ADDONS_HOUSEKEEP_SEC", "30"))
_SYMS_ENV = os.getenv("ADDONS_SYMBOLS", "").strip()

_QUIET = (None, "NONE", "NORMAL", "INSIDE_BOX", "BALANCED", "FLAT_OPEN")


def _f(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def load_cfg() -> dict:
    return {
        "circuit_near_pct": _f("ADDONS_CIRCUIT_NEAR_PCT", 1.0),
        "dvol_window": int(_f("ADDONS_DVOL_WINDOW", 20)),
        "dvol_min_days": int(_f("ADDONS_DVOL_MIN_DAYS", 5)),
        "box_window_min": int(_f("ADDONS_BOX_WINDOW_MIN", 60)),
        "box_min_bars": int(_f("ADDONS_BOX_MIN_BARS", 30)),
        "box_buffer_pct": _f("ADDONS_BOX_BUFFER_PCT", 0.0),
        "spike_z": _f("ADDONS_SPIKE_Z", 3.0),
        "spike_mult": _f("ADDONS_SPIKE_MULT", 3.0),
        "spike_min_hist": int(_f("ADDONS_SPIKE_MIN_HIST", 10)),
        "spike_hist_bars": int(_f("ADDONS_SPIKE_HIST_BARS", 60)),
        "heat_threshold_pct": _f("ADDONS_HEATMAP_THRESHOLD_PCT", 10.0),
        "heat_window_min": int(_f("ADDONS_HEATMAP_WINDOW_MIN", 15)),
        "gap_threshold_pct": _f("ADDONS_GAP_THRESHOLD_PCT", 1.0),
        "flow_minutes": int(_f("ADDONS_FLOW_MINUTES", 5)),
        "flow_ratio": _f("ADDONS_FLOW_RATIO", 0.65),
        "move_minutes": int(_f("ADDONS_MOVE_MINUTES", 5)),
        "move_pct": _f("ADDONS_MOVE_PCT", 1.0),
        "breaker_lookback": int(_f("ADDONS_BREAKER_LOOKBACK", 20)),
        "breaker_vol_mult": _f("ADDONS_BREAKER_VOL_MULT", 2.0),
        "intraday_min_days": int(_f("ADDONS_INTRADAY_MIN_DAYS", 3)),
    }


def load_sectors(path: str = SECTORS_FILE) -> Dict[str, str]:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {}
    return {str(k).upper(): str(v) for k, v in data.items() if not str(k).startswith("_")}


def _num(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def parse_tick(fields: dict) -> dict:
    """Subset of an md:ticks:eq entry; circuit fields are optional (ws_producer does not emit them today)."""
    return {
        "ltp": _num(fields.get("ltp")), "prev_close": _num(fields.get("c")), "open": _num(fields.get("o")),
        "cum_vol": _num(fields.get("vol")), "tbq": _num(fields.get("tbq")), "tsq": _num(fields.get("tsq")),
        "upper": _num(fields.get("upper_circuit_limit") or fields.get("upper_circuit")),
        "lower": _num(fields.get("lower_circuit_limit") or fields.get("lower_circuit")),
    }


class AddonsEngine:
    def __init__(self, r, symbols, sectors: Dict[str, str], cfg: Optional[dict] = None):
        self.r = r
        self.symbols = {s.upper() for s in symbols}
        self.sectors = sectors
        self.cfg = cfg or load_cfg()
        self.bars: Dict[str, Deque[Candle]] = {}
        self.daily: Dict[str, List[Candle]] = {}
        self.ticks: Dict[str, dict] = {}
        self.preopen: Dict[str, dict] = {}
        self.last_ts: Dict[str, int] = {}

    # ── data loading ──
    def seed(self) -> None:
        for sym, bars in read_last_candles_multi(self.r, IN_1M, sorted(self.symbols), SEED_BARS).items():
            self.bars[sym] = deque(bars, maxlen=SEED_BARS)
            if bars:
                self.last_ts[sym] = bars[-1].ts_ms
        self.refresh_daily()
        self.refresh_ticks()

    def refresh_daily(self) -> None:
        self.daily = read_last_candles_multi(self.r, IN_1D, sorted(self.symbols), self.cfg["dvol_window"] + 2)

    def refresh_ticks(self, now_ms: Optional[int] = None) -> None:
        rows = self.r.xrevrange(EQ_STREAM, max="+", min="-", count=TICK_SCAN) or []
        seen = set()
        for _id, fields in rows:  # newest first
            sym = str(fields.get("symbol") or "").upper()
            if sym not in self.symbols or sym in seen:
                continue
            seen.add(sym)
            self.ticks[sym] = parse_tick(fields)
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        md = A.minute_of_day(now)
        if 9 * 60 <= md < 9 * 60 + 15:  # pre-open window: remember last pre-open tick
            for sym in seen:
                t = self.ticks[sym]
                self.preopen[sym] = {"price": t["ltp"], "volume": t["cum_vol"]}

    # ── per-candle evaluation ──
    def evaluate(self, sym: str, candle: Candle) -> dict:
        cfg, bars = self.cfg, list(self.bars[sym])
        tick = self.ticks.get(sym, {})
        day = A.ist_date(candle.ts_ms)
        mod = A.minute_of_day(candle.ts_ms)
        today = [c for c in bars if A.ist_date(c.ts_ms) == day]
        today_cum = sum(c.v for c in today) if today else None
        vols = [c.v for c in bars[:-1]]
        prev_days = [c for c in self.daily.get(sym, []) if A.ist_date(c.ts_ms) < day]

        # cumulative volume up to this time-of-day on earlier days that cover this time
        cum_by_day: Dict = {}
        covers: set = set()
        for c in bars:
            d = A.ist_date(c.ts_ms)
            if d == day:
                continue
            m = A.minute_of_day(c.ts_ms)
            if m <= mod:
                cum_by_day[d] = cum_by_day.get(d, 0.0) + c.v
            if m >= mod:
                covers.add(d)
        same_time = [v for d, v in cum_by_day.items() if d in covers]

        pre = self.preopen.get(sym, {})
        cum_now = tick.get("cum_vol") if tick.get("cum_vol") is not None else today_cum
        return {
            "circuit": A.circuit_proximity(tick.get("ltp") or candle.c, tick.get("upper"), tick.get("lower"),
                                           cfg["circuit_near_pct"]),
            "daily_volume_gain": A.daily_volume_gain(cum_now, [c.v for c in prev_days],
                                                     cfg["dvol_window"], cfg["dvol_min_days"]),
            "box": A.box_breakout(bars, cfg["box_window_min"], cfg["box_min_bars"], cfg["box_buffer_pct"]),
            "volume_spike_1m": A.volume_spike(candle.v, vols[-cfg["spike_hist_bars"]:], cfg["spike_z"],
                                              cfg["spike_mult"], cfg["spike_min_hist"]),
            "volume_spike_5m": A.volume_spike(
                sum(c.v for c in bars[-5:]) if len(bars) >= 5 else None,
                A.rolling_sums(vols, 5)[-cfg["spike_hist_bars"]:], cfg["spike_z"], cfg["spike_mult"],
                cfg["spike_min_hist"]),
            "premarket": A.premarket_analysis(
                tick.get("prev_close") or (prev_days[-1].c if prev_days else None),
                pre.get("price"), pre.get("volume"), today[0].o if today else None, cfg["gap_threshold_pct"]),
            "heavy_flow": A.heavy_flow(bars, cfg["flow_minutes"], cfg["flow_ratio"],
                                       tick.get("tbq"), tick.get("tsq")),
            "fast_move": A.fast_move(bars, cfg["move_minutes"], cfg["move_pct"]),
            "volume_breaker": A.volume_breaker(bars, cfg["breaker_lookback"], cfg["breaker_vol_mult"]),
            "intraday_volume": A.intraday_volume_increase(
                today_cum, same_time, prev_days[-1].v if prev_days else None, cfg["intraday_min_days"]),
        }

    def day_change_pct(self, sym: str) -> Optional[float]:
        t, bars = self.ticks.get(sym, {}), self.bars.get(sym)
        px = t.get("ltp") or (bars[-1].c if bars else None)
        pc = t.get("prev_close")
        if not pc and bars:
            d = [c for c in self.daily.get(sym, []) if A.ist_date(c.ts_ms) < A.ist_date(bars[-1].ts_ms)]
            pc = d[-1].c if d else None
        return A._pct(px, pc) if px and pc else None

    def market_stats(self) -> Dict[str, dict]:
        win = self.cfg["heat_window_min"]
        stats = {}
        for sym, bars in self.bars.items():
            if not bars:
                continue
            buy = sell = 0.0
            for c in list(bars)[-win:]:
                b, s = A.buy_sell_split(c)
                buy += b
                sell += s
            stats[sym] = {"buy_vol": buy, "sell_vol": sell, "chg_pct": self.day_change_pct(sym)}
        return stats

    def publish_market(self, ts_ms: int) -> None:
        stats = self.market_stats()
        heat = A.sector_heatmap(stats, self.sectors, self.cfg["heat_threshold_pct"])
        chg = A.sector_change(stats, self.sectors)
        for key, doc in ((HEATMAP_KEY, heat), (SECTOR_CHANGE_KEY, chg)):
            self.r.set(key, json.dumps({**doc, "ts_ms": ts_ms}), ex=LATEST_TTL_SEC)

    def on_candle(self, sym: str, candle: Candle) -> Optional[dict]:
        sym = sym.upper()
        if self.symbols and sym not in self.symbols:
            return None
        prev = self.last_ts.get(sym)
        if prev is not None and candle.ts_ms <= prev:
            return None
        self.bars.setdefault(sym, deque(maxlen=SEED_BARS)).append(candle)
        self.last_ts[sym] = candle.ts_ms
        try:
            doc = self.evaluate(sym, candle)
        except Exception as e:  # never crash the worker on one symbol
            doc = {"error": f"{type(e).__name__}: {e}"}
        doc["ts_ms"] = candle.ts_ms
        doc["symbol"] = sym
        flags = [v["signal"] for v in doc.values()
                 if isinstance(v, dict) and v.get("signal") not in _QUIET]
        self.r.set(f"{LATEST_PREFIX}{sym}", json.dumps(doc), ex=LATEST_TTL_SEC)
        self.r.xadd(OUT_STREAM, {"ts_ms": str(candle.ts_ms), "symbol": sym, "tf": "1m",
                                 "flags": ",".join(flags), "addons": json.dumps(doc)},
                    maxlen=OUT_MAXLEN, approximate=True)
        return doc


def ensure_group(r, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def main() -> None:
    symbols = ({s.strip().upper() for s in _SYMS_ENV.split(",") if s.strip()} if _SYMS_ENV
               else set(load_symbols()))
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_1M, GROUP)
    eng = AddonsEngine(r, symbols, load_sectors())
    eng.seed()
    print(f"[ADDONS] reading {IN_1M}, writing {OUT_STREAM} + {LATEST_PREFIX}*, symbols={len(symbols)}")
    last_hk = time.time()
    while True:
        resp = r.xreadgroup(GROUP, CONSUMER, {IN_1M: ">"}, count=2000, block=2000)
        now = time.time()
        if now - last_hk >= HOUSEKEEP_SEC:
            eng.refresh_ticks()
            eng.refresh_daily()
            eng.publish_market(int(now * 1000))
            last_hk = now
        if not resp:
            continue
        max_ts = 0
        for _s, msgs in resp:
            ids = []
            for msg_id, fields in msgs:
                ids.append(msg_id)
                parsed = parse_candle_fields(fields)
                if parsed is None:
                    continue
                if eng.on_candle(parsed[0], parsed[1]) is not None:
                    max_ts = max(max_ts, parsed[1].ts_ms)
            if ids:
                r.xack(IN_1M, GROUP, *ids)
        if max_ts:
            eng.publish_market(max_ts)


if __name__ == "__main__":
    main()
