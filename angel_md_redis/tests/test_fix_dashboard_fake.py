"""In-memory read-mostly fake Redis + realistic seed for the dashboard tests.

Every write method records itself in `writes` so tests can assert the
dashboard never writes.
"""

from __future__ import annotations

import fnmatch
import json
import time
import unittest

NOW_MS = int(time.time() * 1000)
SYM = "RELIANCE"
TSYM = "RELIANCE27OCT261400CE"
TSYM_PE = "RELIANCE27OCT261360PE"
TID = "TRD_RELIANCE_1"


class FakeRedis:
    def __init__(self):
        self.kv, self.hashes, self.streams, self.zsets, self.sets, self.ttls = {}, {}, {}, {}, {}, {}
        self.writes = []

    # ── seeding (not part of the redis API used by the dashboard) ──
    def seed_str(self, k, v, ttl=-1):
        self.kv[k] = v if isinstance(v, str) else json.dumps(v)
        self.ttls[k] = ttl

    def seed_hash(self, k, mapping):
        self.hashes[k] = {str(a): (b if isinstance(b, str) else json.dumps(b)) for a, b in mapping.items()}

    def seed_stream(self, k, entries):
        """entries: list of (id_ms, fields) oldest first."""
        out = self.streams.setdefault(k, [])
        for i, (ms, f) in enumerate(entries):
            out.append((f"{ms}-{i}", {a: str(b) for a, b in f.items()}))

    def seed_zset(self, k, mapping):
        self.zsets[k] = dict(mapping)

    def seed_set(self, k, members):
        self.sets[k] = set(members)

    # ── read API ──
    def ping(self):
        return True

    def dbsize(self):
        return len(self._all())

    def _all(self):
        return set(self.kv) | set(self.hashes) | set(self.streams) | set(self.zsets) | set(self.sets)

    def type(self, k):
        if k in self.kv:
            return "string"
        if k in self.hashes:
            return "hash"
        if k in self.streams:
            return "stream"
        if k in self.zsets:
            return "zset"
        if k in self.sets:
            return "set"
        return "none"

    def ttl(self, k):
        if k not in self._all():
            return -2
        return self.ttls.get(k, -1)

    def get(self, k):
        if k in self.hashes or k in self.streams or k in self.zsets:
            raise Exception("WRONGTYPE Operation against a key holding the wrong kind of value")
        return self.kv.get(k)

    def hgetall(self, k):
        return dict(self.hashes.get(k, {}))

    def hget(self, k, f):
        return self.hashes.get(k, {}).get(f)

    def scan_iter(self, match="*", count=None):
        for k in sorted(self._all()):
            if fnmatch.fnmatchcase(k, match):
                yield k

    def xlen(self, k):
        return len(self.streams.get(k, []))

    def xrevrange(self, k, max="+", min="-", count=None):
        rows = list(reversed(self.streams.get(k, [])))
        return rows[:count] if count else rows

    def zrevrange(self, k, start, end, withscores=False):
        items = sorted(self.zsets.get(k, {}).items(), key=lambda kv: -kv[1])
        items = items[start:(None if end == -1 else end + 1)]
        return items if withscores else [m for m, _ in items]

    def smembers(self, k):
        return set(self.sets.get(k, set()))

    def lrange(self, k, a, b):
        return []

    # ── writes (must never be called by the dashboard) ──
    def _w(self, name, *a, **kw):
        self.writes.append((name, a, kw))

    def set(self, *a, **kw):
        self._w("set", *a, **kw)

    def hset(self, *a, **kw):
        self._w("hset", *a, **kw)

    def xadd(self, *a, **kw):
        self._w("xadd", *a, **kw)

    def delete(self, *a, **kw):
        self._w("delete", *a, **kw)

    def zadd(self, *a, **kw):
        self._w("zadd", *a, **kw)


def ago(sec: float) -> int:
    return NOW_MS - int(sec * 1000)


def seeded(now_ms: int = None) -> FakeRedis:
    """A Redis snapshot with every layer populated for RELIANCE (+ a stale TCS)."""
    global NOW_MS
    if now_ms:
        NOW_MS = now_ms
    r = FakeRedis()
    t = lambda s: str(ago(s))  # noqa: E731
    # ingestion
    r.seed_hash("md:active_expiry", {SYM: "2026-10-27", "TCS": "2026-10-27"})
    r.seed_str("md:active_expiry:ts_ms", t(5))
    r.seed_hash("meta:eq:2885", {"symbol": SYM, "tradingsymbol": "RELIANCE-EQ", "exchange": "NSE"})
    r.seed_hash("meta:opt:111", {"underlying": SYM, "tradingsymbol": TSYM, "expiry": "2026-10-27", "strike": "1400", "cp": "CE", "exchange": "NFO"})
    r.seed_hash("meta:opt:112", {"underlying": SYM, "tradingsymbol": TSYM_PE, "expiry": "2026-10-27", "strike": "1360", "cp": "PE", "exchange": "NFO"})
    r.seed_hash("meta:opt:200", {"underlying": "TCS", "tradingsymbol": "TCS27OCT263000CE", "expiry": "2026-10-27", "strike": "3000", "cp": "CE", "exchange": "NFO"})
    r.seed_stream("md:ticks:eq", [(ago(3), {"ts_recv": t(3), "symbol": SYM, "ltp": "1381.5", "token": "2885"}),
                                  (ago(2), {"ts_recv": t(2), "symbol": "TCS", "ltp": "3010"})])
    r.seed_stream("md:ticks:opt", [(ago(1), {"ts_recv": t(1), "underlying": SYM, "tradingsymbol": TSYM, "ltp": "21.4"})])
    r.seed_stream("md:greeks:snap", [(ago(20), {"ts_recv": t(20), "underlying": SYM, "expiry": "2026-10-27",
                                                 "data_json": json.dumps([{"strikePrice": "1400", "optionType": "CE", "delta": "0.45"}])})])
    # old list form + new dict form
    r.seed_str(f"md:greeks:latest:{SYM}:2026-10-27", [{"strikePrice": "1400", "optionType": "CE", "delta": "0.45"}], ttl=3500)
    r.seed_str(f"md:greeks:latest:{SYM}:2026-11-24", {"ts_ms": ago(30), "data": [{"strikePrice": "1400", "optionType": "CE"}]})
    r.seed_stream("md:features:opt", [(ago(1), {"ts_recv": t(1), "underlying": SYM, "tradingsymbol": TSYM, "delta": "0.45"})])
    r.seed_stream("md:greeks:phase:signal", [(ago(10), {"ts_ms": t(10), "tradingsymbol": TSYM, "underlying": SYM, "phase": "MARKUP"})])
    r.seed_str(f"md:greeks:phase:latest:{TSYM}", {"ts_ms": ago(10), "tradingsymbol": TSYM, "phase": "MARKUP"})
    r.seed_str(f"md:greeks:phase:underlying:latest:{SYM}:CE", {"ts_ms": ago(10), "tradingsymbol": TSYM, "phase": "MARKUP"})
    r.seed_str(f"md:greeks:phase:underlying:latest:{SYM}:PE", {"ts_ms": ago(10), "tradingsymbol": TSYM_PE, "phase": "ACCUMULATION"})
    # candles
    for tf, sec in (("1m", 30), ("5m", 200), ("10m", 400), ("30m", 900)):
        r.seed_stream(f"md:candles:{tf}", [(ago(sec), {"ts_ms": t(sec + 60), "symbol": SYM, "tf": tf, "o": "1380", "h": "1385",
                                                        "l": "1378", "c": "1381", "v": "1000"})])
    r.seed_stream("md:candles:1d", [(ago(86400), {"ts_ms": t(86400), "symbol": SYM, "tf": "1d", "c": "1370", "date": "2026-10-02"})])
    r.seed_str(f"md:pivots:prevday:{SYM}", {"date": "2026-10-02", "P": "1370", "R1": "1390", "S1": "1350"})
    r.seed_stream("md:pivots:prevday", [(ago(86400), {"ts_ms": t(86400), "symbol": SYM, "P": "1370"})])
    # module 1-3
    for stream, prefix, doc in (
        ("md:ema:cross", "md:ema:cross:latest:", {"state": "bullish", "ema9": "1381", "ema26": "1376"}),
        ("md:supertrend:bias", "md:supertrend:bias:latest:", {"bias": "CALL", "st_1m": "CALL"}),
        ("md:htf:trend", "md:htf:trend:latest:", {"bias": "CALL"}),
        ("md:level:entry", "md:level:entry:latest:", {"signal": "BUY CALL", "level": "R1"}),
        ("md:momentum:confirm", "md:momentum:confirm:latest:", {"signal": "Confirmed"}),
        ("md:entry:trigger", "md:entry:trigger:latest:", {"signal": "BUY CALL", "strength": "STRONG", "reason": "all aligned"}),
        ("md:expected_move:signal", "md:expected_move:latest:", {"expected_move_pct": "0.012", "target_price": "1398"}),
        ("md:strike:select", "md:strike:select:latest:", {"status": "OK", "tradingsymbol": TSYM, "strike": "1400"}),
        ("md:strike:intel", "md:strike:intel:latest:", {"status": "OK", "tradingsymbol": TSYM, "strike_score": "82.5",
                                                         "top": json.dumps([{"rank": 1, "tradingsymbol": TSYM}]),
                                                         "ranked": json.dumps([TSYM]), "reasons": json.dumps(["liquid"])}),
        ("md:capital:alloc", "md:capital:alloc:latest:", {"status": "OK", "trade_notional": "25000"}),
        ("md:probability", "md:probability:latest:", {"probability": "71", "grade": "B", "decision": "TRADE", "tradingsymbol": TSYM,
                                                       "signal_ts_ms": str(ago(14))}),
        ("md:icare", "md:icare:latest:", {"status": "APPROVED", "tradingsymbol": TSYM, "recommended_lots": "2",
                                           "gross_ev": "520", "charges": "60", "net_ev": "460", "expected_value": "460",
                                           "exec_mode": "paper", "flags": json.dumps(["CROSSED"]), "signal_ts_ms": str(ago(12))}),
        ("md:oi:underlying:signal", "md:oi:underlying:latest:", {"max_pain": "1380", "support": "1350", "resistance": "1400",
                                                                  "positioning": "BULLISH_POSITIONING"}),
        ("md:strikeflow:signal", "md:strikeflow:latest:", {"sweep": "1"}),
        ("md:stockflow:signal", "md:stockflow:latest:", {"entry_ok": "1"}),
        ("md:composite:signal", "md:composite:latest:", {"score": "0.66"}),
    ):
        payload = dict(doc, ts_ms=ago(15), symbol=SYM)
        r.seed_stream(stream, [(ago(15), {k: v for k, v in payload.items()})])
        r.seed_str(prefix + SYM, payload, ttl=3500)
    r.seed_str("md:indicator:score:latest:" + SYM, {"ts_ms": ago(15), "score": "0.7"})
    # stale TCS entry trigger (yesterday)
    r.seed_str("md:entry:trigger:latest:TCS", {"ts_ms": ago(86400), "signal": "NEUTRAL", "symbol": "TCS"})
    r.seed_str("md:volume:latest", {SYM: {"signal": "Bullish Volume", "buy_pct": "64", "ts_ms": str(ago(20))},
                                    "TCS": {"signal": "Neutral", "ts_ms": str(ago(20))}, "ts_ms": str(ago(20))})
    r.seed_stream("md:volume:signal", [(ago(20), {"ts_ms": t(20), "symbol": SYM, "signal": "Bullish Volume"})])
    r.seed_str("md:regime:latest", {"ts_ms": ago(25), "regime": "Bullish"})
    r.seed_stream("md:regime", [(ago(25), {"ts_ms": t(25), "regime": "Bullish"})])
    # microstructure by symbol + contract
    for name in ("bidask", "imbalance", "smartmoney", "orderflow"):
        r.seed_str(f"md:{name}:latest:{SYM}", {"ts_ms": ago(2), "key": SYM})
        r.seed_str(f"md:{name}:latest:{TSYM}", {"ts_ms": ago(2), "key": TSYM, "bid": "21.3", "ask": "21.5"})
        r.seed_stream(f"md:{name}:signal", [(ago(2), {"ts_ms": t(2), "key": TSYM})])
    # a stale contract (bid-ask from 10 minutes ago)
    r.seed_str(f"md:bidask:latest:{TSYM_PE}", {"ts_ms": ago(600), "key": TSYM_PE, "flags": "STALE"})
    for name, prefix in (("oi", "md:oi:latest:"), ("liquidity", "md:liquidity:score:latest:"), ("optexit", "md:optexit:latest:")):
        r.seed_str(prefix + TSYM, {"ts_ms": ago(5), "tradingsymbol": TSYM})
    r.seed_stream("md:oi:signal", [(ago(5), {"ts_ms": t(5), "tradingsymbol": TSYM})])
    r.seed_stream("md:liquidity:score:signal", [(ago(5), {"ts_ms": t(5), "tradingsymbol": TSYM})])
    r.seed_stream("md:optexit:signal", [(ago(5), {"ts_ms": t(5), "tradingsymbol": TSYM})])
    r.seed_str(f"md:greeks_change:latest:{SYM}", {"ts_ms": ago(30), "tradingsymbol": TSYM})
    r.seed_str(f"md:greeks_change:latest:{TSYM}", {"ts_ms": ago(30), "tradingsymbol": TSYM})
    r.seed_stream("md:greeks_change:signal", [(ago(30), {"ts_ms": t(30), "symbol": SYM})])
    # decision
    r.seed_zset("md:probability:rank", {SYM: 71.0, "TCS": 40.0})
    r.seed_str(f"md:ranking:latest:{SYM}:CALL", {"rank_ts_ms": ago(11), "rank": 1, "trade_score": 77.0, "rank_decision": "TAKE_TRADE",
                                                  "tradingsymbol": TSYM, "rank_emit": "1"})
    r.seed_zset("md:ranking:rank", {f"{SYM}:CALL": 77.0})
    r.seed_hash("md:ranking:book", {f"{SYM}:CALL": {"candidate_id": "c1", "arrived_ms": ago(11), "emitted": True, "last_decision": "TAKE_TRADE"}})
    r.seed_stream("md:ranking", [(ago(11), {"ts_ms": t(11), "symbol": SYM, "rank_decision": "TAKE_TRADE"})])
    r.seed_str("md:ranking:cycle:latest", {"ts_ms": ago(9), "outcome": "TAKE", "scanned": 3})
    r.seed_stream("md:ranking:cycle", [(ago(9), {"ts_ms": t(9), "outcome": "TAKE"})])
    r.seed_str(f"md:icare:origin:{TSYM}", {"probability": "71", "tradingsymbol": TSYM})
    r.seed_str("md:account:latest", {"ts_ms": ago(10), "mode": "paper", "total_capital": 100000, "available_margin": 80000})
    # execution
    report = {"trade_id": TID, "symbol": SYM, "option_symbol": TSYM, "execution_status": "FILLED", "filled_lots": 2,
              "requested_lots": 2, "average_fill_price": 21.5, "reference_price": 21.5, "charges": {"total": 42.1},
              "reject_reasons": [], "signal_ts_ms": ago(12), "received_ms": ago(11), "timestamp": ago(10), "mode": "paper"}
    r.seed_str(f"md:exec:latest:{TID}", report)
    r.seed_str(f"md:exec:latest:tsym:{TSYM}", report)
    r.seed_str(f"md:exec:state:{TID}", {"trade_id": TID, "status": "FILLED", "updated_ms": ago(10)})
    r.seed_hash("md:exec:active", {})
    r.seed_hash("md:exec:missed", {"TRD_X": {"tsym": TSYM, "due_ms": NOW_MS + 60000}})
    r.seed_str("md:control:kill_switch", "0")
    r.seed_stream("md:exec", [(ago(11), {"event": "COMMAND", "trade_id": TID, "tradingsymbol": TSYM, "ts_ms": t(11)}),
                              (ago(10), {"event": "REPORT", "trade_id": TID, "tradingsymbol": TSYM, "ts_ms": t(10)})])
    r.seed_stream("md:exec:fill", [(ago(10), {"trade_id": TID, "tradingsymbol": TSYM, "symbol": SYM, "ts_ms": t(10)})])
    # TSL
    r.seed_str(f"md:tsl:latest:{TSYM}", {"ts_ms": ago(3), "tradingsymbol": TSYM, "status": "TRAILING", "current_trailing_stop": 19.8, "mode": "shadow"})
    r.seed_str(f"md:tsl:state:{TID}", {"trade_id": TID, "updated_ms": ago(3), "status": "TRAILING"})
    r.seed_str(f"md:tsl:state:{TID}:exit_context", {"reason": "x"})
    r.seed_str("md:tsl:chain:CH1", {"chain_id": "CH1", "symbol": SYM, "ts_ms": ago(300)})
    r.seed_str(f"md:tsl:block:{SYM}:CALL", {"until_ms": NOW_MS + 60000})
    r.seed_set("md:tsl:chains", ["CH1"])
    r.seed_stream("md:tsl", [(ago(3), {"ts_ms": t(3), "tradingsymbol": TSYM, "event": "UPDATE"})])
    r.seed_stream("md:tsl:reentry", [(ago(500), {"ts_ms": t(500), "symbol": SYM, "tradingsymbol": TSYM})])
    # journal
    r.seed_str(f"md:position:open:{TSYM}", {"trade_id": TID, "symbol": SYM, "tradingsymbol": TSYM, "side": "CE", "lots": 2,
                                            "lot_size": 500, "qty": 1000, "entry_premium": 21.5, "entry_ts_ms": ago(600),
                                            "last_premium": 22.5, "sl_premium": 18, "target_premium": 28})
    closed = {"trade_id": "TRD_OLD", "symbol": SYM, "tradingsymbol": TSYM_PE, "pnl": "-120.5", "exit_reason": "SL",
              "exit_ts_ms": str(ago(3600)), "exec_mode": "paper"}
    r.seed_stream("md:journal", [(ago(3600), closed)])
    r.seed_str("md:journal:closed:TRD_OLD", closed)
    r.seed_str("md:journal:stats", {"ALL": {"samples": 4, "wins": 3, "sum_win_pct": 30.0, "sum_loss_pct": 10.0, "pnl": 900.0},
                                    SYM: {"samples": 2, "wins": 1, "sum_win_pct": 10.0, "sum_loss_pct": 5.0, "pnl": 100.0}})
    import datetime as _dt
    r.seed_str(f"md:journal:daily:{_dt.date.today().isoformat()}", {"realized_pnl": -120.5, "trades": 1, "wins": 0})
    return r


def make_client():
    """Factory for DASHBOARD_REDIS_FACTORY=test_fix_dashboard_fake:make_client."""
    return seeded()


class FakeSelfTest(unittest.TestCase):
    def test_fake_types(self):
        r = seeded()
        self.assertEqual(r.type("md:active_expiry"), "hash")
        self.assertEqual(r.type("md:ticks:eq"), "stream")
        self.assertEqual(r.type("md:ranking:rank"), "zset")
        self.assertEqual(r.ttl("nope"), -2)
        self.assertEqual(r.writes, [])


if __name__ == "__main__":
    unittest.main()
