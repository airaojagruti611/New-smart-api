import json
import re
import time
import datetime as dt
from pathlib import Path
from typing import Optional, Dict, Any, List

import requests
import pandas as pd

SCRIPMASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

CACHE_PATH = Path("OpenAPIScripMaster.json")
CACHE_MAX_AGE_HOURS = 24

def parse_expiry(x) -> Optional[dt.date]:
    if not x:
        return None
    s = str(x).strip().upper()
    for fmt in ("%d%b%Y", "%d%b%y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except:
            pass
    return None

def to_float(x) -> Optional[float]:
    try:
        return float(x)
    except:
        return None

def load_scripmaster(cache_path: Path = CACHE_PATH) -> pd.DataFrame:
    use_cache = False
    if cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600.0
        if age_hours <= CACHE_MAX_AGE_HOURS:
            use_cache = True

    if not use_cache:
        r = requests.get(SCRIPMASTER_URL, timeout=120)
        r.raise_for_status()
        cache_path.write_bytes(r.content)

    data = json.loads(cache_path.read_text(encoding="utf-8"))
    return prepare_scripmaster(pd.DataFrame(data))


def option_type_of(symbol: Any, instrumenttype: Any = "") -> Optional[str]:
    """CE/PE from the tradingsymbol's 2-char suffix (PETRONET27OCT26320CE -> CE).

    Substring matching is wrong: PETRONET/PERSISTENT/HINDPETRO contain "PE".
    Non-option instruments return None.
    """
    it = str(instrumenttype or "").upper()
    if it and not it.startswith("OPT"):
        return None
    s = str(symbol or "").strip().upper()
    if s.endswith("CE"):
        return "CE"
    if s.endswith("PE"):
        return "PE"
    return None


def prepare_scripmaster(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw ScripMaster rows (columns, expiry/strike parsing, opt_type)."""
    df = df.copy()
    df["exch_seg"] = df["exch_seg"].astype(str).str.upper()

    # token
    if "token" in df.columns:
        df["token"] = df["token"].astype(str)
    elif "symboltoken" in df.columns:
        df["token"] = df["symboltoken"].astype(str)
    else:
        raise RuntimeError("ScripMaster missing token column.")

    # symbol/tradingsymbol
    if "symbol" not in df.columns:
        if "tradingsymbol" in df.columns:
            df["symbol"] = df["tradingsymbol"].astype(str)
        else:
            raise RuntimeError("ScripMaster missing symbol/tradingsymbol.")
    df["symbol"] = df["symbol"].astype(str)

    if "instrumenttype" not in df.columns:
        df["instrumenttype"] = ""
    df["instrumenttype"] = df["instrumenttype"].astype(str).str.upper()

    if "name" not in df.columns:
        df["name"] = ""
    df["name"] = df["name"].astype(str)

    df["expiry_date"] = df.get("expiry", "").apply(parse_expiry)
    df["strike_f"] = df.get("strike", None).apply(to_float)

    df["opt_type"] = [
        option_type_of(sym, it) for sym, it in zip(df["symbol"].tolist(), df["instrumenttype"].tolist())
    ]

    return df

def resolve_eq_tokens(df: pd.DataFrame, symbols: List[str]) -> Dict[str, Dict[str, str]]:
    """
    NSE equity tokens from ScripMaster: SYMBOL-EQ
    """
    d = df[df["exch_seg"] == "NSE"].copy()
    d["sym_u"] = d["symbol"].str.upper()

    out: Dict[str, Dict[str, str]] = {}
    for s in symbols:
        target = f"{s}-EQ"
        hit = d[d["sym_u"] == target]
        if not hit.empty:
            r = hit.iloc[0]
            out[s] = {"tradingsymbol": r["symbol"], "token": str(r["token"]), "exchange": "NSE"}
        else:
            # Fallback: same base symbol in another series (e.g. HFCL-BE for
            # trade-to-trade stocks). Exact base match only: a prefix match
            # would map PNB to PNBHOUSING.
            hit2 = d[d["sym_u"].str.split("-").str[0] == s]
            hit2 = hit2[hit2["sym_u"].str.endswith(("-EQ", "-BE", "-BZ"))]
            if not hit2.empty:
                r = hit2.iloc[0]
                out[s] = {"tradingsymbol": r["symbol"], "token": str(r["token"]), "exchange": "NSE"}
    return out

def pick_nearest_expiry(d: pd.DataFrame) -> Optional[dt.date]:
    today = dt.date.today()
    expiries = sorted({e for e in d["expiry_date"].dropna().tolist() if e >= today})
    return expiries[0] if expiries else None

def best_step(vals: list[float]) -> float:
    s = sorted(set([x for x in vals if x is not None]))
    if len(s) < 2:
        return 1.0
    diffs = [round(s[i+1]-s[i], 6) for i in range(len(s)-1)]
    diffs = [d for d in diffs if d > 0]
    return min(diffs) if diffs else 1.0

def round_to_step(x: float, step: float) -> float:
    return round(x / step) * step

def detect_strike_scale(median_strike: float, spot: float) -> float:
    """
    Better than the old threshold:
    - if strikes look ~100x bigger than spot (paise), scale=100
    """
    if spot and median_strike and median_strike > spot * 10:
        return 100.0
    # some masters use 1000 scaling (rare) – you can extend later
    return 1.0

def filter_underlying(d: pd.DataFrame, underlying: str) -> pd.DataFrame:
    """Rows whose underlying is exactly `underlying`.

    Uses the ScripMaster `name` column (LT != LTF/LTM, NIFTY != NIFTYNXT50,
    PNB != PNBHOUSING). Rows with an empty name fall back to a strict
    tradingsymbol pattern: UNDERLYING + DDMMMYY + strike + CE/PE.
    """
    u = str(underlying or "").strip().upper()
    names = d["name"].astype(str).str.strip().str.upper() if "name" in d.columns else pd.Series("", index=d.index)
    by_name = names == u
    blank = names == ""
    sym_u = d["symbol"].astype(str).str.upper()
    pat = r"^" + re.escape(u) + r"\d{2}[A-Z]{3}\d{2}\d+(?:\.\d+)?(?:CE|PE)$"
    by_sym = blank & sym_u.str.match(pat)
    return d[by_name | by_sym]


def _option_rows(df: pd.DataFrame, underlying: str) -> pd.DataFrame:
    """
    filter_underlying() over the NFO option rows, cached on the frame: the
    producer plans every symbol (twice with the wide tier), and re-filtering
    the full ScripMaster each time blocks the websocket callback for seconds.
    """
    cache = df.attrs.get("_opt_rows")
    if cache is None:
        nfo = df[(df["exch_seg"] == "NFO") & (df["instrumenttype"].str.contains("OPT", na=False))].copy()
        nfo["sym_u"] = nfo["symbol"].str.upper()
        cache = {"_all": nfo}
        df.attrs["_opt_rows"] = cache
    u = str(underlying or "").strip().upper()
    if u not in cache:
        cache[u] = filter_underlying(cache["_all"], u)
    return cache[u]


def build_atm_option_tokens(
    df: pd.DataFrame,
    underlying: str,
    spot: float,
    strikes_around: int
) -> tuple[list[dict], Optional[str]]:
    """
    Returns:
      - list of {token, tradingsymbol, underlying, expiry, strike, cp, exchange}
      - expiry_str like '2026-01-27' for greeks poller
    """
    d = _option_rows(df, underlying)
    if d.empty:
        return [], None
    d = d.copy()

    expiry = pick_nearest_expiry(d)
    if not expiry:
        return [], None

    d = d[d["expiry_date"] == expiry].copy()
    strikes_raw = [x for x in d["strike_f"].dropna().tolist()]
    if not strikes_raw:
        return [], None

    med = float(pd.Series(strikes_raw).median())
    scale = detect_strike_scale(med, spot)
    d["strike_rupees"] = d["strike_f"] / scale

    strikes = [x for x in d["strike_rupees"].dropna().tolist()]
    step = best_step(strikes)
    atm = round_to_step(spot, step)

    desired = [atm + i * step for i in range(-strikes_around, strikes_around + 1)]

    out: list[dict] = []
    ce_all = d[d["opt_type"] == "CE"].copy()
    pe_all = d[d["opt_type"] == "PE"].copy()
    if ce_all.empty or pe_all.empty:
        return [], expiry.isoformat()

    for st in desired:
        ce_all["diff"] = (ce_all["strike_rupees"] - st).abs()
        pe_all["diff"] = (pe_all["strike_rupees"] - st).abs()

        ce = ce_all.sort_values("diff").iloc[0].to_dict()
        pe = pe_all.sort_values("diff").iloc[0].to_dict()

        for row in (ce, pe):
            out.append({
                "token": str(row["token"]),
                "tradingsymbol": str(row["symbol"]),
                "underlying": underlying,
                "expiry": expiry.isoformat(),
                "strike": float(row.get("strike_rupees") or st),
                "cp": row.get("opt_type"),
                "exchange": "NFO"
            })

    return out, expiry.isoformat()


def option_specs(df: pd.DataFrame, underlyings: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Per-contract order specs for NFO options of `underlyings` (DECISION.md §7 E6/E8):
      tick       price tick in rupees (ScripMaster tick_size is in paise: 5 -> 0.05, 1 -> 0.01)
      lot_size   units per lot
      freeze_qty NSE quantity-freeze limit in UNITS per order (None if absent)
      expiry     date
      kind       OPTSTK / OPTIDX
    """
    names = {u.upper() for u in underlyings}
    d = df[(df["exch_seg"] == "NFO") & df["instrumenttype"].isin(["OPTSTK", "OPTIDX"])]
    d = d[d["name"].str.upper().isin(names)]
    out: Dict[str, Dict[str, Any]] = {}
    for row in d.to_dict("records"):
        tick_paise = to_float(row.get("tick_size"))
        out[str(row["symbol"]).upper()] = {
            "tick": round(tick_paise / 100.0, 4) if tick_paise else 0.05,
            "lot_size": to_float(row.get("lotsize")),
            "freeze_qty": to_float(row.get("freeze_qty")),
            "expiry": row.get("expiry_date"),
            "kind": row.get("instrumenttype"),
            "token": str(row.get("token") or ""),
        }
    return out
