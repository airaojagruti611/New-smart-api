"""Executor thresholds (DECISION.md §7 E3–E14). Every value has an EXEC_* env override."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class ExecConfig:
    max_slippage_pct: float = 0.50          # cap = reference(ask) x (1 + this)      (E6, X-Q3)
    timeout_sec: float = 10.0               # whole command                        (E9, X-Q3)
    step_sec: float = 2.0                   # ladder step interval                 (E6)
    ladder_steps: int = 5                   # start -> cap in this many steps      (E6)
    tight_spread_ticks: int = 2             # spread <= this many ticks: start at the ask
    command_ttl_sec: float = 5.0            # older command = COMMAND_EXPIRED      (E3)
    max_quote_age_sec: float = 3.0          # STALE_QUOTE                          (E4)
    max_spread_pct: float = 1.0             # SPREAD_TOO_WIDE                      (E4, X-Q2)
    max_drift_pct: float = 2.0              # ask vs ICARE premium                 (E4, X-Q7)
    no_entry_before: str = "09:20"          # OPENING_WINDOW                       (E4, X-Q10)
    no_entry_after: str = "15:20"           # MARKET_CLOSED (journal's no-new-entry time)
    expiry_cutoff: str = "13:00"            # stock options on expiry day          (E4, X-Q10)
    allow_reduce: bool = True               # fewer lots than ICARE allowed        (E7, X-Q4)
    min_lots: int = 1
    max_lots_per_order: int = 10            # fallback when freeze qty is unknown  (E8)
    depth_take: float = 1.0                 # share of visible depth one slice may take
    cost_margin: float = 1.5                # continue if benefit > cost x this    (E9, X-Q5)
    min_residual_value: float = 2000.0      # Rs; smaller remainders are dropped   (E9, X-Q5)
    max_open_trades: int = 5                # same value as ICARE_MAX_OPEN_TRADES
    margin_buffer_pct: float = 1.0          # same as ICARE_MARGIN_BUFFER_PCT
    max_signal_age_sec: float = 60.0        # upstream signal_ts_ms older = SIGNAL_EXPIRED (= ICARE_MAX_SIGNAL_AGE_SEC)
    # exits (E17): SELL ladder bid -> floor, never abandoned, never a market order
    exit_max_slippage_pct: float = 2.0      # floor = bid x (1 - this)
    exit_timeout_sec: float = 10.0          # then re-anchor on the current bid and keep going
    exit_step_sec: float = 1.0              # re-price every 1 s (E17)
    place_reconcile_sec: float = 15.0       # unknown place outcome: look for our tag this long
    exit_quote_max_age_sec: float = 30.0    # exits: older quote = wait; after this long use the last bid

    @staticmethod
    def from_env() -> "ExecConfig":
        d = ExecConfig()
        return ExecConfig(
            max_slippage_pct=_env_f("EXEC_MAX_SLIPPAGE_PCT", d.max_slippage_pct),
            timeout_sec=_env_f("EXEC_TIMEOUT_SEC", d.timeout_sec),
            step_sec=_env_f("EXEC_STEP_SEC", d.step_sec),
            ladder_steps=int(_env_f("EXEC_LADDER_STEPS", d.ladder_steps)),
            tight_spread_ticks=int(_env_f("EXEC_TIGHT_SPREAD_TICKS", d.tight_spread_ticks)),
            command_ttl_sec=_env_f("EXEC_COMMAND_TTL_SEC", d.command_ttl_sec),
            max_quote_age_sec=_env_f("EXEC_MAX_QUOTE_AGE_SEC", d.max_quote_age_sec),
            max_spread_pct=_env_f("EXEC_MAX_SPREAD_PCT", d.max_spread_pct),
            max_drift_pct=_env_f("EXEC_MAX_DRIFT_PCT", d.max_drift_pct),
            no_entry_before=os.getenv("EXEC_NO_ENTRY_BEFORE", d.no_entry_before),
            no_entry_after=os.getenv("JOURNAL_EOD_HHMM", d.no_entry_after),
            expiry_cutoff=os.getenv("EXEC_EXPIRY_CUTOFF", d.expiry_cutoff),
            allow_reduce=os.getenv("EXEC_ALLOW_REDUCE", "1") == "1",
            min_lots=int(_env_f("EXEC_MIN_LOTS", d.min_lots)),
            max_lots_per_order=int(_env_f("EXEC_MAX_LOTS_PER_ORDER", d.max_lots_per_order)),
            depth_take=_env_f("EXEC_DEPTH_TAKE", d.depth_take),
            cost_margin=_env_f("EXEC_COST_MARGIN", d.cost_margin),
            min_residual_value=_env_f("EXEC_MIN_RESIDUAL_VALUE", d.min_residual_value),
            max_open_trades=int(_env_f("ICARE_MAX_OPEN_TRADES", d.max_open_trades)),
            margin_buffer_pct=_env_f("ICARE_MARGIN_BUFFER_PCT", d.margin_buffer_pct),
            max_signal_age_sec=_env_f("EXEC_MAX_SIGNAL_AGE_SEC", d.max_signal_age_sec),
            exit_max_slippage_pct=_env_f("EXEC_EXIT_MAX_SLIPPAGE_PCT", d.exit_max_slippage_pct),
            exit_timeout_sec=_env_f("EXEC_EXIT_TIMEOUT_SEC", d.exit_timeout_sec),
            exit_step_sec=_env_f("EXEC_EXIT_STEP_SEC", d.exit_step_sec),
            place_reconcile_sec=_env_f("EXEC_PLACE_RECONCILE_SEC", d.place_reconcile_sec),
            exit_quote_max_age_sec=_env_f("EXEC_EXIT_QUOTE_MAX_AGE_SEC", d.exit_quote_max_age_sec),
        )
