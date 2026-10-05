import os
import time
from typing import Dict, Any, List, Optional, Tuple

from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from .config import (
    WS_WARMUP_SEC, STRIKES_AROUND, MAX_WS_SUBS, SUBSCRIBE_MODE,
    STREAM_EQ, STREAM_OPT,
    STREAM_MAXLEN_EQ, STREAM_MAXLEN_OPT,
)
from .utils import now_ms, paise_to_rupees
from .redis_store import RedisStore
from .scripmaster import load_scripmaster, resolve_eq_tokens, build_atm_option_tokens


# Equity ticks unchanged in every core field are still re-emitted after this
# long, so freshness checks downstream don't age out a live-but-quiet book.
EQ_HEARTBEAT_MS = int(float(os.getenv("EQ_TICK_HEARTBEAT_SEC", "5")) * 1000)

_PRICE_FIELDS = ("ltp", "bid", "ask")
_EXACT_FIELDS = ("c", "vol", "bid_sz", "ask_sz")


def eq_core_changed(last: Optional[Dict[str, Any]], core: Dict[str, Any], eps: float,
                    now: int, heartbeat_ms: int) -> bool:
    """True when an equity tick should be emitted.

    Prices compare with an ``eps`` noise guard; previous close, cumulative
    volume and top-of-book sizes compare exactly. A heartbeat re-emits an
    unchanged book every ``heartbeat_ms`` (0 disables it).
    """
    if last is None:
        return True
    for k in _PRICE_FIELDS:
        a, b = core.get(k), last.get(k)
        if a is None or b is None:
            if a != b:
                return True
        elif abs(a - b) > eps:
            return True
    for k in _EXACT_FIELDS:
        if core.get(k) != last.get(k):
            return True
    emitted = last.get("emitted_ms")
    return bool(heartbeat_ms > 0 and (emitted is None or now - emitted >= heartbeat_ms))


def _extract_top5(levels: Any) -> Tuple[Optional[float], Optional[float], List[float], List[float]]:
    """
    levels: best_5_buy_data / best_5_sell_data from Angel SNAP_QUOTE payload.
    Each entry expected shape: {"price": <paise>, "quantity": <int>, ...}
    index 0 = best (highest bid / lowest ask) per Angel's documented ordering.

    Returns (best_price_rupees, best_qty, [top5 qty sizes], [top5 price levels rupees]).
    NOTE: field names assumed from Angel SmartAPI docs — verify against a
    live payload dump (print(data) in on_data) before relying on this in prod.
    """
    if not levels or not isinstance(levels, list):
        return None, None, [], []
    sizes: List[float] = []
    prices: List[float] = []
    best_price: Optional[float] = None
    best_qty: Optional[float] = None
    for i, lvl in enumerate(levels[:5]):
        if not isinstance(lvl, dict):
            continue
        qty_raw = lvl.get("quantity", lvl.get("qty", 0))
        try:
            qty = float(qty_raw)
        except Exception:
            qty = 0.0
        px = paise_to_rupees(lvl.get("price")) or 0.0
        sizes.append(qty)
        prices.append(px)
        if i == 0:
            best_price = px if px else None
            best_qty = qty
    return best_price, best_qty, sizes, prices


class MarketDataProducer:
    def __init__(self, auth_token: str, feed_token: str, client_code: str, api_key: str, symbols: list[str]):
        self.auth_token = auth_token
        self.feed_token = feed_token
        self.client_code = client_code
        self.api_key = api_key
        self.symbols = symbols

        self.rs = RedisStore()
        self.df = load_scripmaster()

        self.eq_map = resolve_eq_tokens(self.df, symbols)
        self.eq_token_to_symbol = {v["token"]: k for k, v in self.eq_map.items()}

        # Publish to md:ticks:eq only when regime/volume-relevant fields change.
        # Tracks last published (ltp, prev_close, cum_vol) per symbol.
        self._last_eq_core: Dict[str, Dict[str, Optional[float]]] = {}
        self._ltp_eps: float = 0.005  # ~0.5 paisa noise guard after rupees conversion

        # option token -> meta
        self.opt_meta: Dict[str, dict] = {}

        # ✅ This will be published to Redis for the greeks poller
        self.active_expiry_by_underlying: Dict[str, str] = {}

        self.spot_ltp: Dict[str, float] = {}
        self.ws_open_t: Optional[float] = None
        self.options_subscribed = False          # first option pass done (run_greeks waits on it)
        self.options_planned: set = set()        # symbols whose option chain has been planned
        self._opt_batch_no = 0

        self.sws = SmartWebSocketV2(
            auth_token=self.auth_token,
            api_key=self.api_key,
            client_code=self.client_code,
            feed_token=self.feed_token,
            max_retry_attempt=10,
            retry_strategy=0,
            retry_delay=2,
            retry_multiplier=2,
            retry_duration=60,
        )

        self.EXCH_NSE = getattr(SmartWebSocketV2, "NSE_CM", 1)
        self.EXCH_NFO = getattr(SmartWebSocketV2, "NSE_FO", 2)

        # mode: SNAP_QUOTE recommended for OHLC/vol/OI
        self.MODE_LTP = getattr(SmartWebSocketV2, "LTP", 1)
        self.MODE_QUOTE = getattr(SmartWebSocketV2, "QUOTE", 2)
        self.MODE_SNAP = getattr(SmartWebSocketV2, "SNAP_QUOTE", 3)

        if SUBSCRIBE_MODE == "LTP":
            self.mode_eq = self.MODE_LTP
            self.mode_opt = self.MODE_LTP
        elif SUBSCRIBE_MODE == "QUOTE":
            self.mode_eq = self.MODE_QUOTE
            self.mode_opt = self.MODE_QUOTE
        else:
            self.mode_eq = self.MODE_SNAP
            self.mode_opt = self.MODE_SNAP

        self.sws.on_open = self.on_open
        self.sws.on_data = self.on_data
        self.sws.on_error = self.on_error
        self.sws.on_close = self.on_close

    def start(self):
        if not self.eq_map:
            raise RuntimeError("No NSE EQ tokens resolved from ScripMaster.")
        print(f"[WS] EQ tokens resolved: {len(self.eq_map)}")
        self.sws.connect()

    def on_open(self, wsapp):
        self.ws_open_t = time.time()
        eq_tokens = [info["token"] for info in self.eq_map.values()]

        # store meta in redis
        for sym, info in self.eq_map.items():
            self.rs.hset_meta(f"meta:eq:{info['token']}", {
                "symbol": sym,
                "tradingsymbol": info["tradingsymbol"],
                "exchange": "NSE",
            })

        token_list = [{"exchangeType": self.EXCH_NSE, "tokens": eq_tokens}]
        self.sws.subscribe(correlation_id="EQ01", mode=self.mode_eq, token_list=token_list)
        print(f"[WS] opened; subscribed EQ={len(eq_tokens)} mode={SUBSCRIBE_MODE}")

    def on_error(self, wsapp, error):
        print("[WS] error:", error)

    def on_close(self, wsapp):
        print("[WS] closed")

    def _publish_active_expiry(self):
        """
        ✅ Publish active expiries for greeks poller to Redis.
        """
        self.rs.hset_meta("md:active_expiry", self.active_expiry_by_underlying)
        self.rs.set_latest("md:active_expiry:ts_ms", str(now_ms()), ex_sec=3600)

    def _maybe_subscribe_options(self):
        """
        First pass after warm-up (enough spots or WS_WARMUP_SEC); afterwards any
        symbol whose first spot tick arrives late is planned and subscribed on
        its own, so a slow symbol never ends up with no option chain.
        """
        if self.ws_open_t is None:
            return
        if not self.options_subscribed:
            elapsed = time.time() - self.ws_open_t
            enough = (len(self.spot_ltp) >= max(10, int(0.5 * len(self.eq_map)))) or (elapsed >= WS_WARMUP_SEC)
            if not enough:
                return
        pending = [s for s in self.symbols if s in self.spot_ltp and s not in self.options_planned]
        if not pending:
            return
        first = not self.options_subscribed

        # build option token plan
        planned: List[dict] = []
        for sym in pending:
            self.options_planned.add(sym)
            contracts, expiry_iso = build_atm_option_tokens(self.df, sym, self.spot_ltp[sym], STRIKES_AROUND)
            if not contracts:
                continue

            if expiry_iso:
                self.active_expiry_by_underlying[sym] = expiry_iso

            planned.extend(contracts)

        # dedupe by token (also against contracts already subscribed)
        seen = set(self.opt_meta)
        unique = []
        for c in planned:
            t = c["token"]
            if t not in seen:
                seen.add(t)
                unique.append(c)

        eq_count = len(self.eq_map)
        total = eq_count + len(self.opt_meta) + len(unique)

        if total > MAX_WS_SUBS:
            cap = max(0, MAX_WS_SUBS - eq_count - len(self.opt_meta))
            unique = unique[:cap]
            print(f"[WS] capped option tokens to {len(unique)} to stay under MAX_WS_SUBS={MAX_WS_SUBS}")

        self.options_subscribed = True

        if not unique:
            if first:
                print("[WS] no option contracts planned (many symbols may not have options)")
            # ✅ publish active expiries (even if partial/empty)
            self._publish_active_expiry()
            return

        # store opt meta in redis + memory
        tokens = []
        for c in unique:
            tok = c["token"]
            self.opt_meta[tok] = c
            tokens.append(tok)
            self.rs.hset_meta(f"meta:opt:{tok}", {
                "underlying": c["underlying"],
                "tradingsymbol": c["tradingsymbol"],
                "expiry": c["expiry"],
                "strike": str(c["strike"]),
                "cp": c["cp"],
                "exchange": "NFO",
            })

        # subscribe in batches (correlation ids unique across passes)
        BATCH = 50
        for i in range(0, len(tokens), BATCH):
            batch = tokens[i:i + BATCH]
            token_list = [{"exchangeType": self.EXCH_NFO, "tokens": batch}]
            self._opt_batch_no += 1
            self.sws.subscribe(correlation_id=f"OPT{self._opt_batch_no:03d}", mode=self.mode_opt, token_list=token_list)

        # ✅ publish active expiries for greeks poller
        self._publish_active_expiry()

        print(f"[WS] subscribed OPT={len(tokens)} for {pending} mode={SUBSCRIBE_MODE} "
              f"(EQ={eq_count}, total={eq_count+len(self.opt_meta)})")

    def _emit_eq(self, sym: str, tok: str, data: Dict[str, Any]):
        ltp = paise_to_rupees(data.get("last_traded_price"))
        if ltp is not None:
            self.spot_ltp[sym] = ltp

        prev_close = paise_to_rupees(data.get("closed_price"))
        cum_vol_raw = data.get("volume_trade_for_the_day")
        try:
            cum_vol = float(cum_vol_raw) if cum_vol_raw not in (None, "") else None
        except Exception:
            cum_vol = None

        bid, bid_qty, bid_sizes, bid_prices = _extract_top5(data.get("best_5_buy_data"))
        ask, ask_qty, ask_sizes, ask_prices = _extract_top5(data.get("best_5_sell_data"))
        ltq_raw = data.get("last_traded_quantity")

        # Only emit when core fields used by downstream jobs actually change,
        # plus a heartbeat so a live-but-quiet book keeps a fresh source time
        # (downstream quote-age checks use the tick time).
        core = {"ltp": ltp, "c": prev_close, "vol": cum_vol, "bid": bid, "ask": ask,
                "bid_sz": bid_qty, "ask_sz": ask_qty}
        now = now_ms()
        changed = eq_core_changed(self._last_eq_core.get(sym), core, self._ltp_eps, now, EQ_HEARTBEAT_MS)
        if not changed:
            return

        payload = {
            "ts_recv": str(now),
            "ts_exch": str(data.get("exchange_timestamp") or ""),
            "token": tok,
            "symbol": sym,
            "ltp": str(ltp if ltp is not None else ""),
            "o": str(paise_to_rupees(data.get("open_price_of_the_day")) or ""),
            "h": str(paise_to_rupees(data.get("high_price_of_the_day")) or ""),
            "l": str(paise_to_rupees(data.get("low_price_of_the_day")) or ""),
            "c": str(prev_close or ""),
            "vol": str(cum_vol_raw or ""),
            "tbq": str(data.get("total_buy_quantity") or ""),
            "tsq": str(data.get("total_sell_quantity") or ""),
            "bid": str(bid if bid is not None else ""),
            "ask": str(ask if ask is not None else ""),
            "bid_sz": str(bid_qty if bid_qty is not None else ""),
            "ask_sz": str(ask_qty if ask_qty is not None else ""),
            "bid_depth5": ",".join(str(x) for x in bid_sizes),
            "ask_depth5": ",".join(str(x) for x in ask_sizes),
            "bid_depth5_px": ",".join(str(x) for x in bid_prices),
            "ask_depth5_px": ",".join(str(x) for x in ask_prices),
            "ltq": str(ltq_raw if ltq_raw not in (None, "") else ""),
        }
        self.rs.xadd(STREAM_EQ, payload, maxlen=STREAM_MAXLEN_EQ)
        self._last_eq_core[sym] = dict(core, emitted_ms=now)

    def _emit_opt(self, tok: str, data: Dict[str, Any]):
        meta = self.opt_meta.get(tok)
        if not meta:
            return

        bid, bid_qty, bid_sizes, bid_prices = _extract_top5(data.get("best_5_buy_data"))
        ask, ask_qty, ask_sizes, ask_prices = _extract_top5(data.get("best_5_sell_data"))
        ltq_raw = data.get("last_traded_quantity")

        payload = {
            "ts_recv": str(now_ms()),
            "ts_exch": str(data.get("exchange_timestamp") or ""),
            "token": tok,
            "underlying": meta["underlying"],
            "tradingsymbol": meta["tradingsymbol"],
            "expiry": meta["expiry"],
            "strike": str(meta["strike"]),
            "cp": meta["cp"],
            "ltp": str(paise_to_rupees(data.get("last_traded_price")) or ""),
            "oi": str(data.get("open_interest") or ""),
            "vol": str(data.get("volume_trade_for_the_day") or ""),
            "o": str(paise_to_rupees(data.get("open_price_of_the_day")) or ""),
            "h": str(paise_to_rupees(data.get("high_price_of_the_day")) or ""),
            "l": str(paise_to_rupees(data.get("low_price_of_the_day")) or ""),
            "c": str(paise_to_rupees(data.get("closed_price")) or ""),
            "tbq": str(data.get("total_buy_quantity") or ""),
            "tsq": str(data.get("total_sell_quantity") or ""),
            "bid": str(bid if bid is not None else ""),
            "ask": str(ask if ask is not None else ""),
            "bid_sz": str(bid_qty if bid_qty is not None else ""),
            "ask_sz": str(ask_qty if ask_qty is not None else ""),
            "bid_depth5": ",".join(str(x) for x in bid_sizes),
            "ask_depth5": ",".join(str(x) for x in ask_sizes),
            "bid_depth5_px": ",".join(str(x) for x in bid_prices),
            "ask_depth5_px": ",".join(str(x) for x in ask_prices),
            "ltq": str(ltq_raw if ltq_raw not in (None, "") else ""),
        }
        self.rs.xadd(STREAM_OPT, payload, maxlen=STREAM_MAXLEN_OPT)

    def on_data(self, wsapp, data: Dict[str, Any]):
        tok = str(data.get("token", ""))

        # equity tick
        if tok in self.eq_token_to_symbol:
            sym = self.eq_token_to_symbol[tok]
            self._emit_eq(sym, tok, data)
            self._maybe_subscribe_options()
            return

        # option tick
        if tok in self.opt_meta:
            self._emit_opt(tok, data)
            return
