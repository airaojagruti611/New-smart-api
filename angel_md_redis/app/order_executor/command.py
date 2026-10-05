"""
The approved command (brief §2) built from an ICARE APPROVED payload, and the
fresh pre-trade validation (brief §3) — DECISION.md §7 E3/E4.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import List, Optional

from app.freshness import stream_id_ms, ts_field_ms

from .config import ExecConfig
from .market import Quote


def _f(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


@dataclass(frozen=True)
class ExecCommand:
    trade_id: str
    source_id: str                      # md:icare message id (idempotency)
    signal_ts_ms: int                   # SOURCE time of the upstream signal (never ICARE's ts_ms)
    symbol: str
    tradingsymbol: str
    side: str                           # CE / PE (we only BUY options)
    direction: str                      # BUY
    strike: float
    requested_lots: int                 # ICARE recommended_lots = the upper bound
    lot_size: float
    signal_premium: Optional[float]     # premium the approval was computed at
    sl_premium: Optional[float]
    target_premium: Optional[float]
    max_risk_amount: Optional[float]
    allocated_capital: Optional[float]
    ev_per_lot: Optional[float]
    rank: str = ""
    trade_score: Optional[float] = None
    probability: Optional[float] = None
    recommended_sl_pct: Optional[float] = None
    recommended_tsl_pct: Optional[float] = None    # pass-through only (T14)
    tick: float = 0.05
    freeze_qty: Optional[float] = None
    stock_option: bool = True
    expiry: str = ""
    max_slippage_pct: float = 0.50
    execution_timeout_seconds: float = 10.0
    source_ms: int = 0                  # md:icare XADD time (stream-id ms); 0 = unknown

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "ExecCommand":
        return ExecCommand(**{k: d[k] for k in ExecCommand.__dataclass_fields__ if k in d})


def signal_time_ms(icare: dict, source_id: str) -> int:
    """
    Cross-agent contract: `signal_ts_ms` (upstream signal source time) when ICARE sends it,
    else the md:icare stream-id ms. ICARE's `ts_ms` is its PUBLISH time (= now even for a
    replayed backlog), so it is never used. 0 = unknown -> validate() fails closed.
    """
    return ts_field_ms(icare, "signal_ts_ms") or stream_id_ms(source_id) or 0


def build_command(icare: dict, trade_id: str, source_id: str, spec: Optional[dict], cfg: ExecConfig) -> ExecCommand:
    spec = spec or {}
    premium = _f(icare.get("premium"))
    sl = _f(icare.get("stop_loss_premium"))
    return ExecCommand(
        trade_id=trade_id,
        source_id=source_id,
        signal_ts_ms=signal_time_ms(icare, source_id),
        symbol=str(icare.get("symbol") or "").upper(),
        tradingsymbol=str(icare.get("tradingsymbol") or "").upper(),
        side=str(icare.get("side") or "").upper(),
        direction="BUY",
        strike=_f(icare.get("strike")) or 0.0,
        requested_lots=int(_f(icare.get("recommended_lots")) or 0),
        lot_size=_f(icare.get("lot_size")) or _f(spec.get("lot_size")) or 0.0,
        signal_premium=premium,
        sl_premium=sl,
        target_premium=_f(icare.get("target_premium")),
        max_risk_amount=_f(icare.get("max_risk_allowed")),
        allocated_capital=_f(icare.get("max_capital")),
        # GROSS EV per lot: ICARE's expected_value is net of charges and the partial-fill check
        # subtracts per-order brokerage itself (no double count)
        ev_per_lot=_f(icare.get("gross_ev")) if _f(icare.get("gross_ev")) is not None else _f(icare.get("expected_value")),
        rank=str(icare.get("rank") or ""),
        trade_score=_f(icare.get("trade_score")),
        probability=_f(icare.get("probability")),
        recommended_sl_pct=round((premium - sl) / premium * 100.0, 2) if premium and sl is not None else None,
        recommended_tsl_pct=_f(icare.get("trailing_stop_pct")),
        tick=_f(spec.get("tick")) or 0.05,
        freeze_qty=_f(spec.get("freeze_qty")),
        stock_option=str(spec.get("kind") or "OPTSTK") == "OPTSTK",
        expiry=str(spec.get("expiry") or icare.get("expiry") or ""),
        max_slippage_pct=cfg.max_slippage_pct,
        execution_timeout_seconds=cfg.timeout_sec,
        source_ms=stream_id_ms(source_id) or 0,
    )


@dataclass(frozen=True)
class Context:
    """Execution-time facts the runner gathers (all can change after ICARE approved)."""
    hhmm: str                       # IST "HH:MM"
    kill_switch: bool = False
    expiry_today: bool = False
    available_margin: Optional[float] = None
    open_trades: int = 0            # open positions + OTHER trades currently executing
    underlying_busy: bool = False   # open or executing trade on the same underlying


def validate(cmd: ExecCommand, q: Optional[Quote], ctx: Context, now_ms: int, cfg: ExecConfig) -> List[str]:
    """All failing checks, most important first. Empty = executable (sizing checks margin/depth)."""
    reasons: List[str] = []
    if cmd.requested_lots <= 0 or cmd.lot_size <= 0:
        reasons.append("INVALID_COMMAND")
    if cmd.sl_premium is None or cmd.sl_premium <= 0:
        reasons.append("NO_STOP_LOSS")              # risk cannot be bounded (E7)
    # (a) command age: md:icare XADD time (no worker can re-stamp it; the group starts at "0",
    #     so a replayed backlog is caught here). Unknown time fails closed.
    if cmd.source_ms <= 0 or now_ms - cmd.source_ms > cfg.command_ttl_sec * 1000:
        reasons.append("COMMAND_EXPIRED")
    # (b) signal age: origin of the chain (intel -> probability -> ranking -> ICARE can take > 5 s)
    if cmd.signal_ts_ms <= 0 or now_ms - cmd.signal_ts_ms > cfg.max_signal_age_sec * 1000:
        reasons.append("SIGNAL_EXPIRED")
    if ctx.kill_switch:
        reasons.append("KILL_SWITCH")
    if ctx.hhmm < cfg.no_entry_before:
        reasons.append("OPENING_WINDOW")
    if ctx.hhmm >= cfg.no_entry_after:
        reasons.append("MARKET_CLOSED")
    if cmd.stock_option and ctx.expiry_today and ctx.hhmm >= cfg.expiry_cutoff:
        reasons.append("EXPIRY_CUTOFF")
    if ctx.underlying_busy:
        reasons.append("DUPLICATE_UNDERLYING")
    if ctx.open_trades >= cfg.max_open_trades:
        reasons.append("EXPOSURE_LIMIT")
    if q is None or q.age_ms(now_ms) > cfg.max_quote_age_sec * 1000:
        reasons.append("STALE_QUOTE")
        return reasons
    if not q.tradable:
        reasons.append("NO_QUOTE")
        return reasons
    if cmd.sl_premium is not None and q.ask <= cmd.sl_premium + 1e-9:
        reasons.append("PRICE_BELOW_STOP")          # signal invalidated: buying at/below ICARE's SL
    if q.spread_pct > cfg.max_spread_pct:
        reasons.append("SPREAD_TOO_WIDE")
    if cmd.signal_premium and q.ask > cmd.signal_premium * (1.0 + cfg.max_drift_pct / 100.0):
        reasons.append("MARKET_CONDITION_CHANGED")
    return reasons
