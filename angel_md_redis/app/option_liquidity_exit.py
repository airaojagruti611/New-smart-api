"""
app/option_liquidity_exit.py
───────────────────────
Option Exit When Spread Widens & Liquidity Disappears — core logic.

All thresholds compare against SESSION averages (cumulative mean since
market open, reset daily) — not a short rolling window — because the whole
point is detecting a departure from "how this contract has behaved all day,"
which a 20-tick window would itself absorb and mask.

Stage 1 — spread drift    : spread_pct > 1.2x session avg spread_pct
Stage 2 — bid size shrink : bid_size < 0.5x session avg bid_size
Stage 3 — refresh slowing : short-window refresh rate < 0.6x session avg
                             refresh rate (i.e. a >40% drop)
Stage 4 — bid pull        : best bid drops > BID_PULL_DROP_PCT in one tick
                             while the underlying spot barely moves
Stage 5 — one-sided book  : ask side has refreshed recently, bid side is
                             both thin AND stale (no refresh for STAGE5_STALE_SEC)

Exit rule: Stage 3 + Stage 4 together -> EXIT_NOW (per the brief: "do not
wait for Stage 5"). Stage 5 alone -> ALREADY_TRAPPED (still exit, but it
means the ideal exit window already passed).

No I/O here — pure dataclasses + functions/classes. Redis wiring lives in
run_option_liquidity_exit.py.
"""

from __future__ import annotations

import datetime as dt
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple

SESSION_MIN_SAMPLES = 20

SPREAD_DRIFT_MULT = 1.2
BID_SHRINK_RATIO = 0.5

REFRESH_RATE_WINDOW_SEC = 10.0
REFRESH_RATE_DROP_RATIO = 0.6   # current < 0.6x session avg -> >40% drop

BID_PULL_DROP_PCT = 5.0
SPOT_MOVE_TOL_PCT = 0.1

STAGE5_STALE_SEC = 5.0


def _today_str() -> str:
    return dt.date.today().isoformat()


@dataclass
class SessionAvg:
    """Cumulative mean since session start; resets when the calendar day changes."""

    day: str = field(default_factory=_today_str)
    _sum: float = 0.0
    _n: int = 0

    def _maybe_reset(self) -> None:
        today = _today_str()
        if today != self.day:
            self.day = today
            self._sum = 0.0
            self._n = 0

    def push(self, v: float) -> None:
        self._maybe_reset()
        if v and v > 0:
            self._sum += v
            self._n += 1

    @property
    def avg(self) -> Optional[float]:
        self._maybe_reset()
        if self._n < SESSION_MIN_SAMPLES:
            return None
        return self._sum / self._n

    @property
    def n(self) -> int:
        self._maybe_reset()
        return self._n


class RefreshRateTracker:
    """
    A "refresh" = a change in (price, size) on a given side since the last
    tick. Tracks a short rolling-window rate (refreshes/sec, last
    REFRESH_RATE_WINDOW_SEC) and a session-cumulative average rate, per side.
    Also exposes time-since-last-refresh per side (for Stage 5 staleness).
    """

    def __init__(self, window_sec: float = REFRESH_RATE_WINDOW_SEC):
        self.window_sec = window_sec
        self.day = _today_str()

        self._last_bid_key: Optional[Tuple[float, float]] = None
        self._last_ask_key: Optional[Tuple[float, float]] = None
        self._last_bid_refresh_ts: Optional[int] = None
        self._last_ask_refresh_ts: Optional[int] = None

        self._bid_events: Deque[int] = deque()
        self._ask_events: Deque[int] = deque()

        self._session_start_ts: Optional[int] = None
        self._session_bid_refreshes = 0
        self._session_ask_refreshes = 0

    def _maybe_reset_session(self, ts_ms: int) -> None:
        today = _today_str()
        if today != self.day:
            self.day = today
            self._session_start_ts = ts_ms
            self._session_bid_refreshes = 0
            self._session_ask_refreshes = 0

    def _prune(self, buf: Deque[int], now_ms: int) -> None:
        cutoff = now_ms - int(self.window_sec * 1000)
        while buf and buf[0] < cutoff:
            buf.popleft()

    def update(
        self, ts_ms: int, bid: float, bid_sz: float, ask: float, ask_sz: float
    ) -> Dict[str, Optional[float]]:
        self._maybe_reset_session(ts_ms)
        if self._session_start_ts is None:
            self._session_start_ts = ts_ms

        bid_key = (bid, bid_sz)
        ask_key = (ask, ask_sz)

        if self._last_bid_key is not None and bid_key != self._last_bid_key:
            self._bid_events.append(ts_ms)
            self._last_bid_refresh_ts = ts_ms
            self._session_bid_refreshes += 1
        if self._last_ask_key is not None and ask_key != self._last_ask_key:
            self._ask_events.append(ts_ms)
            self._last_ask_refresh_ts = ts_ms
            self._session_ask_refreshes += 1

        self._last_bid_key = bid_key
        self._last_ask_key = ask_key

        self._prune(self._bid_events, ts_ms)
        self._prune(self._ask_events, ts_ms)

        bid_rate = len(self._bid_events) / self.window_sec
        ask_rate = len(self._ask_events) / self.window_sec
        combined_rate = (len(self._bid_events) + len(self._ask_events)) / self.window_sec

        elapsed = max(1.0, (ts_ms - self._session_start_ts) / 1000.0)
        session_combined_rate = (self._session_bid_refreshes + self._session_ask_refreshes) / elapsed
        session_combined_rate = session_combined_rate if elapsed > self.window_sec else None

        bid_stale_sec = (
            None if self._last_bid_refresh_ts is None else (ts_ms - self._last_bid_refresh_ts) / 1000.0
        )
        ask_stale_sec = (
            None if self._last_ask_refresh_ts is None else (ts_ms - self._last_ask_refresh_ts) / 1000.0
        )

        return {
            "bid_rate": bid_rate,
            "ask_rate": ask_rate,
            "combined_rate": combined_rate,
            "session_combined_rate": session_combined_rate,
            "bid_stale_sec": bid_stale_sec,
            "ask_stale_sec": ask_stale_sec,
        }


@dataclass(frozen=True)
class LiquidityExitResult:
    spread_pct: float
    spread_session_avg: Optional[float]
    bid_size: float
    bid_size_session_avg: Optional[float]
    refresh_rate: Optional[float]
    refresh_session_avg: Optional[float]
    bid_stale_sec: Optional[float]
    ask_stale_sec: Optional[float]
    bid_drop_pct: Optional[float]
    spot_move_pct: Optional[float]

    stage1_spread_drift: bool
    stage2_bid_shrink: bool
    stage3_refresh_slowing: bool
    stage4_bid_pull: bool
    stage5_one_sided: bool

    exit_status: str  # "NONE" / "WATCH" / "WARNING" / "EXIT_NOW" / "ALREADY_TRAPPED"


class OptionLiquidityExitDetector:
    """Per-contract stateful tracker."""

    def __init__(self):
        self.spread_avg = SessionAvg()
        self.bid_size_avg = SessionAvg()
        self.refresh = RefreshRateTracker()
        self._prev_bid: Optional[float] = None
        self._prev_spot: Optional[float] = None

    def analyze(
        self,
        ts_ms: int,
        bid: float,
        ask: float,
        bid_sz: float,
        ask_sz: float,
        spot: Optional[float],
    ) -> LiquidityExitResult:
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
        spread_pct = ((ask - bid) / mid * 100.0) if mid > 0 else 0.0

        spread_avg = self.spread_avg.avg
        bid_avg = self.bid_size_avg.avg

        rr = self.refresh.update(ts_ms, bid, bid_sz, ask, ask_sz)

        # Stage 4: bid pull, checked BEFORE pushing new state into session avgs
        bid_drop_pct = None
        spot_move_pct = None
        stage4 = False
        if self._prev_bid is not None and self._prev_bid > 0 and bid > 0 and bid < self._prev_bid:
            bid_drop_pct = (self._prev_bid - bid) / self._prev_bid * 100.0
            if self._prev_spot is not None and spot is not None and self._prev_spot > 0:
                spot_move_pct = abs(spot - self._prev_spot) / self._prev_spot * 100.0
                stage4 = bid_drop_pct > BID_PULL_DROP_PCT and spot_move_pct <= SPOT_MOVE_TOL_PCT
            elif spot is None or self._prev_spot is None:
                # No spot reference available -> can't confirm "stock didn't move";
                # flag conservatively on drop magnitude alone.
                stage4 = bid_drop_pct > BID_PULL_DROP_PCT

        self._prev_bid = bid if bid > 0 else self._prev_bid
        if spot is not None:
            self._prev_spot = spot

        stage1 = spread_avg is not None and spread_pct > SPREAD_DRIFT_MULT * spread_avg
        stage2 = bid_avg is not None and bid_sz < BID_SHRINK_RATIO * bid_avg

        session_rate = rr["session_combined_rate"]
        stage3 = (
            session_rate is not None
            and session_rate > 0
            and rr["combined_rate"] < REFRESH_RATE_DROP_RATIO * session_rate
        )

        ask_active = rr["ask_stale_sec"] is not None and rr["ask_stale_sec"] < STAGE5_STALE_SEC
        bid_stale = rr["bid_stale_sec"] is not None and rr["bid_stale_sec"] >= STAGE5_STALE_SEC
        bid_thin = bid_avg is not None and bid_sz < BID_SHRINK_RATIO * bid_avg
        stage5 = ask_active and bid_stale and bid_thin

        if stage3 and stage4:
            exit_status = "EXIT_NOW"
        elif stage5:
            exit_status = "ALREADY_TRAPPED"
        elif stage2 and stage1:
            exit_status = "WARNING"
        elif stage1:
            exit_status = "WATCH"
        else:
            exit_status = "NONE"

        self.spread_avg.push(spread_pct)
        self.bid_size_avg.push(bid_sz)

        return LiquidityExitResult(
            spread_pct=round(spread_pct, 4),
            spread_session_avg=round(spread_avg, 4) if spread_avg else None,
            bid_size=bid_sz,
            bid_size_session_avg=round(bid_avg, 1) if bid_avg else None,
            refresh_rate=round(rr["combined_rate"], 3),
            refresh_session_avg=round(session_rate, 3) if session_rate else None,
            bid_stale_sec=round(rr["bid_stale_sec"], 2) if rr["bid_stale_sec"] is not None else None,
            ask_stale_sec=round(rr["ask_stale_sec"], 2) if rr["ask_stale_sec"] is not None else None,
            bid_drop_pct=round(bid_drop_pct, 3) if bid_drop_pct is not None else None,
            spot_move_pct=round(spot_move_pct, 4) if spot_move_pct is not None else None,
            stage1_spread_drift=stage1,
            stage2_bid_shrink=stage2,
            stage3_refresh_slowing=stage3,
            stage4_bid_pull=stage4,
            stage5_one_sided=stage5,
            exit_status=exit_status,
        )
