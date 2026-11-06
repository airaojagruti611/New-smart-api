import json
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

def _iter_master_rows(cache_path: Path):
    """Decode the JSON array incrementally, without retaining the full JSON tree."""
    decoder = json.JSONDecoder()
    with cache_path.open(encoding="utf-8-sig") as source:
        buffer, eof = "", False

        def more():
            nonlocal buffer, eof
            chunk = source.read(65536)
            buffer += chunk
            eof = not chunk

        more()
        buffer = buffer.lstrip()
        if not buffer.startswith("["):
            raise ValueError("ScripMaster must be a JSON array")
        buffer = buffer[1:]
        first = True
        while True:
            buffer = buffer.lstrip()
            while not buffer and not eof:
                more()
                buffer = buffer.lstrip()
            if buffer.startswith("]"):
                if (buffer[1:] + source.read()).strip():
                    raise ValueError("Unexpected content after ScripMaster array")
                return
            if not first:
                if not buffer.startswith(","):
                    raise ValueError("Missing ScripMaster array delimiter")
                buffer = buffer[1:].lstrip()
            while True:
                try:
                    row, end = decoder.raw_decode(buffer)
                    break
                except json.JSONDecodeError:
                    if eof:
                        raise
                    more()
                    buffer = buffer.lstrip()
            if not isinstance(row, dict):
                raise ValueError("ScripMaster entries must be objects")
            buffer = buffer[end:]
            first = False
            yield row


def load_scripmaster(cache_path: Path = CACHE_PATH, *, symbols=None, equities_only: bool = False) -> pd.DataFrame:
    use_cache = False
    if cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600.0
        if age_hours <= CACHE_MAX_AGE_HOURS:
            use_cache = True

    if not use_cache:
        r = requests.get(SCRIPMASTER_URL, timeout=120)
        r.raise_for_status()
        cache_path.write_bytes(r.content)

    wanted = None if symbols is None else {str(s).upper() for s in symbols}
    def relevant(row):
        if wanted is None and not equities_only:
            return True
        exchange = str(row.get("exch_seg", "")).upper()
        trading = str(row.get("symbol", row.get("tradingsymbol", ""))).upper()
        equity = exchange == "NSE" and (wanted is None or trading.rsplit("-", 1)[0] in wanted)
        option = (not equities_only and exchange == "NFO" and
                  "OPT" in str(row.get("instrumenttype", "")).upper() and
                  (wanted is None or str(row.get("name", "")).upper() in wanted))
        return equity or option
    data = [row for row in _iter_master_rows(cache_path) if relevant(row)]
    df = pd.DataFrame(data)
    if df.empty:
        df = pd.DataFrame(columns=["exch_seg", "token", "symbol", "instrumenttype", "name", "expiry", "strike"])

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

    expiry = df.get("expiry", pd.Series("", index=df.index))
    # The same few expiry strings occur thousands of times.
    df["expiry_date"] = expiry.map({value: parse_expiry(value) for value in expiry.unique()})
    df["strike_f"] = df.get("strike", pd.Series(None, index=df.index, dtype=object)).apply(to_float)

    sym_u = df["symbol"].str.upper()
    df["opt_type"] = None
    df.loc[sym_u.str.contains("CE"), "opt_type"] = "CE"
    df.loc[sym_u.str.contains("PE"), "opt_type"] = "PE"

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


def _options_by_underlying(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """NFO option rows grouped by exact underlying name, built once per ScripMaster frame."""
    # pandas copies attrs when slicing/copying frames. Keeping a dict of full
    # frames in attrs recursively duplicated the option cache on routine slices.
    cached = df.__dict__.get("_smartapi_option_groups")
    if cached is not None:
        return cached
    d = df[(df["exch_seg"] == "NFO") & (df["instrumenttype"].str.contains("OPT", na=False))]
    groups = {str(name).upper(): g for name, g in d.groupby(d["name"].astype(str).str.upper())}
    df.__dict__["_smartapi_option_groups"] = groups
    return groups

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
    # Match on the exact underlying name. A tradingsymbol prefix match mixed in
    # other stocks' options (LT -> LTF/LTM, PNB -> PNBHOUSING).
    d = _options_by_underlying(df).get(underlying.upper())
    if d is None or d.empty:
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
