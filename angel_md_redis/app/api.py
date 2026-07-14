import json
import os
import time
import asyncio
from typing import Any, Optional

import redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

REDIS_URL         = os.getenv("REDIS_URL",          "redis://localhost:6379/0")
REGIME_LATEST_KEY = os.getenv("REGIME_LATEST_KEY",  "md:regime:latest")
VOLUME_LATEST_KEY = os.getenv("VOLUME_LATEST_KEY",  "md:volume:latest")

app = FastAPI(title="Market Data API")
_redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)

SCAN_PCT = 0.02
SCAN_MOVE = 0.30
SCAN_MODE = "open_to_high"


def _add_age(payload: dict) -> dict:
    ts_ms_raw = payload.get("ts_ms")
    try:
        ts_ms = int(ts_ms_raw)
        payload["age_sec"] = round((time.time() * 1000 - ts_ms) / 1000, 1)
    except Exception:
        payload["age_sec"] = None
    return payload


@app.get("/market-data/1m")
def get_market_data_1m():
    """
    Returns the latest 1-minute market regime snapshot written by run_market_regime.py.
    """
    raw = _redis.get(REGIME_LATEST_KEY)
    if not raw:
        raise HTTPException(status_code=404, detail="No market data available yet")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="Corrupted market data payload") from exc

    return _add_age(payload)


@app.get("/market-data/volume")
def get_market_data_volume():
    """
    Returns the latest 1-minute volume analysis snapshot written by run_volume_analyzer.py.

    Response shape:
      {
        "ts_ms": "...",
        "age_sec": 12.3,
        "symbols": {
          "NIFTY": {
            "signal": "Bullish Volume",
            "buy_pct": "68.00", "sell_pct": "32.00",
            "volume": "30000", "volume_surge": "2.30",
            "high": "...", "low": "...", "close": "..."
          },
          ...
        }
      }
    """
    raw = _redis.get(VOLUME_LATEST_KEY)
    if not raw:
        raise HTTPException(status_code=404, detail="No volume data available yet")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="Corrupted volume data payload") from exc

    ts_ms_str = data.pop("ts_ms", None)
    payload = {
        "ts_ms":   ts_ms_str,
        "symbols": data,
    }
    try:
        payload["age_sec"] = round((time.time() * 1000 - int(ts_ms_str)) / 1000, 1)
    except Exception:
        payload["age_sec"] = None

    return payload


def _safe_json_load(raw: Optional[str]) -> Optional[dict[str, Any]]:
    if not raw:
        return None
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else None
    except Exception:
        return None


@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket):
    """
    WebSocket that continuously streams latest regime + volume snapshots.

    Source keys (written by background jobs):
      - md:regime:latest  (run_market_regime.py)
      - md:volume:latest  (run_volume_analyzer.py)
    """
    await websocket.accept()
    last_regime_ts: Optional[str] = None
    last_volume_ts: Optional[str] = None

    # Keep it simple: poll redis keys at fixed interval.
    poll_sec = 1.0

    try:
        while True:
            regime = _safe_json_load(_redis.get(REGIME_LATEST_KEY))
            volume = _safe_json_load(_redis.get(VOLUME_LATEST_KEY))

            regime_ts = (str(regime.get("ts_ms")) if regime and regime.get("ts_ms") is not None else None)
            volume_ts = (str(volume.get("ts_ms")) if volume and volume.get("ts_ms") is not None else None)

            if regime_ts != last_regime_ts or volume_ts != last_volume_ts:
                last_regime_ts = regime_ts
                last_volume_ts = volume_ts
                await websocket.send_json(
                    {
                        "type": "snap",
                        "ts_ms": int(time.time() * 1000),
                        "regime": regime,
                        "volume": volume,
                    }
                )

            await asyncio.sleep(poll_sec)
    except WebSocketDisconnect:
        return


@app.get("/scan/step1")
def scan_step1():
    """
    Step 1 scanner (no parameters):
      current day high >= prev_close*(1+2%) OR current day low <= prev_close*(1-2%)
    Reads: md:ticks:eq
    """
    from scan_step1_step2 import step1_symbols

    passed = step1_symbols(pct=SCAN_PCT)
    ranked = sorted(
        passed.items(),
        key=lambda kv: max(kv[1]["pct_high"], -kv[1]["pct_low"]),
        reverse=True,
    )

    return {
        "params": {"pct": SCAN_PCT},
        "count": len(passed),
        "symbols": [
            {"symbol": sym, **d}
            for sym, d in ranked
        ],
    }


@app.get("/scan/step2")
def scan_step2():
    """
    Step 2 scanner (no parameters):
      options with (High-Open)/Open >= +30% for the Step-1 underlyings
    Reads: md:ticks:opt (latest per tradingsymbol)
    """
    from scan_step1_step2 import step1_symbols, step2_options

    step1 = step1_symbols(pct=SCAN_PCT)
    hits = step2_options(set(step1.keys()), move=SCAN_MOVE, mode=SCAN_MODE)

    return {
        "params": {"pct": SCAN_PCT, "move": SCAN_MOVE, "mode": SCAN_MODE},
        "step1_count": len(step1),
        "hits": [
            {
                "underlying": und,
                "tradingsymbol": tsym,
                "move_pct": mp,
                "base_price": base,
                "high_price": high,
            }
            for (und, tsym, mp, base, high) in hits
        ],
    }
