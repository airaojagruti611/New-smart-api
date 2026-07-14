import os
from typing import Any, Dict, List, Tuple

import redis


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_EQ = os.getenv("STREAM_EQ", "md:ticks:eq")
STREAM_OPT = os.getenv("STREAM_OPT", "md:ticks:opt")

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def f(x: Any) -> float | None:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def read_last_by_key(stream: str, key_field: str, limit: int = 20000) -> Dict[str, Dict[str, Any]]:
    """
    Reads last N entries from a Redis stream and returns latest row per key_field.
    Good enough for a quick scanner; increase limit if your stream is very active.
    """
    out: Dict[str, Dict[str, Any]] = {}
    rows = r.xrevrange(stream, max="+", min="-", count=limit)
    for _id, fields in rows:
        k = (fields.get(key_field) or "").strip()
        if not k:
            continue
        if k not in out:
            out[k] = fields
    return out


def step1_symbols(pct: float = 0.02) -> Dict[str, Dict[str, float]]:
    """
    Step 1 (Underlying filter):
      current day high >= prev_close*(1+pct) OR current day low <= prev_close*(1-pct)
    Uses fields from md:ticks:eq:
      c = prev close, h = day high, l = day low
    """
    eq_latest = read_last_by_key(STREAM_EQ, "symbol")
    passed: Dict[str, Dict[str, float]] = {}
    for sym, row in eq_latest.items():
        c = f(row.get("c"))
        h = f(row.get("h"))
        l = f(row.get("l"))
        if c is None or c <= 0 or h is None or l is None:
            continue

        pct_high = (h - c) / c
        pct_low = (l - c) / c

        if pct_high >= pct or pct_low <= -pct:
            passed[sym] = {"c": c, "h": h, "l": l, "pct_high": pct_high, "pct_low": pct_low}
    return passed


def step2_options(
    step1_syms: set[str],
    move: float = 0.30,
    mode: str = "open_to_high",
) -> List[Tuple[str, str, float, float, float]]:
    """
    Step 2 (Options filter): contracts with >= move upward expansion.

    mode:
      - open_to_high: (h - o) / o
      - low_to_high:  (h - l) / l

    Returns list of (underlying, tradingsymbol, move_pct, base_price, high_price)
    """
    opt_latest = read_last_by_key(STREAM_OPT, "tradingsymbol")
    hits: List[Tuple[str, str, float, float, float]] = []

    for tsym, row in opt_latest.items():
        und = (row.get("underlying") or "").strip().upper()
        if und not in step1_syms:
            continue

        h = f(row.get("h"))
        o = f(row.get("o"))
        l = f(row.get("l"))
        if h is None:
            continue

        if mode == "open_to_high":
            base = o
        elif mode == "low_to_high":
            base = l
        else:
            raise ValueError("mode must be 'open_to_high' or 'low_to_high'")

        if base is None or base <= 0:
            continue

        move_pct = (h - base) / base
        if move_pct >= move:
            hits.append((und, tsym, move_pct, base, h))

    hits.sort(key=lambda x: x[2], reverse=True)
    return hits


def main() -> None:
    step1 = step1_symbols(pct=0.02)
    print("\nSTEP 1: Underlyings with today High >= +2% OR Low <= -2% vs prev close")
    print(f"Found: {len(step1)}")

    ranked = sorted(
        step1.items(),
        key=lambda kv: max(kv[1]["pct_high"], -kv[1]["pct_low"]),
        reverse=True,
    )

    for sym, d in ranked[:50]:
        print(
            f"- {sym:12s}  prevC={d['c']:.2f}  "
            f"H={d['h']:.2f} ({d['pct_high']*100:+.2f}%)  "
            f"L={d['l']:.2f} ({d['pct_low']*100:+.2f}%)"
        )

    hits = step2_options(set(step1.keys()), move=0.30, mode="open_to_high")
    print("\nSTEP 2: Options with (High-Open)/Open >= +30% for Step-1 underlyings")
    print(f"Found: {len(hits)}")

    for und, tsym, mp, base, h in hits[:100]:
        print(f"- {und:12s}  {tsym:28s}  move={mp*100:6.2f}%  open={base:.2f}  high={h:.2f}")


if __name__ == "__main__":
    main()

