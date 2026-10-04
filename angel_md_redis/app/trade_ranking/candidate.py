"""
Trade Ranking Engine — input / output data model (DECISION.md §6 R3, R15).

`Candidate` holds raw module outputs in their native scales; the runner fills
it from Redis, the pure engine normalises and scores it. `None` = missing or
stale — never 0 (R6).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, FrozenSet, List, Optional


@dataclass(frozen=True)
class Candidate:
    symbol: str
    side: str                                   # CE / PE
    tradingsymbol: str
    candidate_id: str = ""
    ts_ms: int = 0
    strike: Optional[float] = None
    expiry: str = ""
    # Probability (Module 12)
    probability: Optional[float] = None
    probability_decision: str = ""
    p_oi: Optional[float] = None
    # Module 1 indicator score -2..+2
    indicator_score: Optional[float] = None
    # Volume analyzer label + surge
    volume_signal: Optional[str] = None
    volume_surge: bool = False
    # Regime breadth label + SIE market phase
    regime: Optional[str] = None
    market_phase: Optional[str] = None
    # Bid-ask: SIE execution quality 0..100 + contract imbalance label
    execution_quality: Optional[float] = None
    imbalance: Optional[str] = None
    # OI positioning label (fallback when p_oi is missing)
    oi_positioning: Optional[str] = None
    # SIE scores
    greeks_score: Optional[float] = None
    liquidity_score: Optional[float] = None
    liquidity_band: str = ""
    spread_pct: Optional[float] = None
    strike_score: Optional[float] = None
    em_fit: Optional[float] = None
    # Module 8
    em_direction: Optional[str] = None
    em_direction_score: Optional[float] = None    # -1..+1
    em_confidence: Optional[float] = None
    # Direction votes (labels)
    htf_bias: Optional[str] = None
    st_bias: Optional[str] = None
    amd_phase: Optional[str] = None
    # Premium projection (per unit, SIE)
    premium: Optional[float] = None
    lot_size: Optional[float] = None
    projected_gain: Optional[float] = None
    projected_gain_iv_down: Optional[float] = None
    adverse_change: Optional[float] = None
    # Journal bucket (EV source switch)
    history_samples: int = 0
    history_win_rate: Optional[float] = None
    history_avg_win_pct: Optional[float] = None
    history_avg_loss_pct: Optional[float] = None
    # Module 7 safe lots
    liquidity_max_lots: Optional[float] = None
    # Module 18 proposal (runner computes it with app/adaptive_tsl)
    tsl_pct: Optional[float] = None
    tsl_rule: str = ""
    # Circuit (no source today -> None, gate inactive)
    ltp: Optional[float] = None
    upper_circuit: Optional[float] = None
    lower_circuit: Optional[float] = None
    # Names of inputs the runner found stale (critical ones -> DATA_INSUFFICIENT)
    stale: FrozenSet[str] = frozenset()
    sector: str = ""


@dataclass(frozen=True)
class Context:
    """Account / portfolio state for one rank cycle."""
    total_capital: float = 0.0
    available_margin: float = 0.0
    day_pnl: float = 0.0
    open_positions: int = 0
    open_risk: float = 0.0
    open_symbols: FrozenSet[str] = frozenset()
    open_sectors: Dict[str, int] = field(default_factory=dict)    # sector -> open positions
    blocked: FrozenSet[str] = frozenset()                         # "SYM:SIDE" (Module 18)
    kill_switch: bool = False
    # ICARE limits (one source of truth, read from the same env)
    max_risk_per_trade: float = 7500.0
    max_risk_pct: float = 2.0
    daily_loss_limit_pct: float = 2.0
    max_portfolio_risk_pct: float = 5.0
    max_open_trades: int = 5


@dataclass(frozen=True)
class RankResult:
    symbol: str
    option: str
    side: str
    direction: str                              # CALL / PUT
    candidate_id: str
    status: str                                 # SCORED / DATA_INSUFFICIENT / REJECT
    decision: str                               # TAKE_TRADE / WATCH / REJECT / DATA_INSUFFICIENT
    confidence: str                             # EXCEPTIONAL / HIGH_CONVICTION / TAKE / CONDITIONAL / ""
    trade_score: Optional[float]
    weighted_score: Optional[float]
    rank: Optional[int]
    probability: Optional[float]
    components: Dict[str, Optional[float]]
    agreement: Optional[float]
    votes: Dict[str, str]
    conflict_factor: float
    risk_factor: float
    dq: float
    missing: List[str]
    gates: Dict[str, bool]                      # gate name -> passed
    expected_gain_pct: Optional[float]
    expected_value: Optional[float]
    ev_source: str
    ev_per_risk: Optional[float]
    reward_risk: Optional[float]
    feasible_lots: int
    capital_required: Optional[float]
    risk_amount: Optional[float]
    initial_stop_loss_pct: Optional[float]
    trailing_stop_pct: Optional[float]
    trailing_activation_pct: Optional[float]
    tsl_rule: str
    reasons: List[str]
    warnings: List[str]
    reject_reasons: List[str]
    flags: List[str]
    sector: str = ""

    def to_dict(self) -> dict:
        return asdict(self)
