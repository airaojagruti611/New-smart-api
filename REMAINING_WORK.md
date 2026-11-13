# Option Rider — Remaining Work

> Gap analysis of `Option Rider Algo For Algobuilders-3.docx` against the code in `angel_md_redis/`.
> Status as of 2026-11-13 (686 tests passed). Every module has code, but a rule-by-rule audit of the
> `-3.docx` (incl. each developer acceptance checklist) found the gaps listed under **Spec gaps** below.
> Size: S = hours, M = 1–3 days, L = a week or more.

---

## ✅ Done

| # | Module | Code |
|---|---|---|
| 1 | Indicator Signals (Supertrend, EMA Cross, Pivots, multi-timeframe filter) | `supertrend.py`, `ema_cross.py`, `pivots.py`, `htf_trend_filter.py`, `indicator_score.py` |
| 2 | Volume Analyzer | `volume_analyzer.py` |
| 3 | Market Regime Detector | `market_regime_detector.py` |
| 4 | Bid-Ask Analyzer (all 7 sub-parts) | `bidask_analyzer.py`, `smart_money.py`, `order_flow.py`, `stock_entry_exit.py`, `option_liquidity_exit.py`, `bidask_imbalance.py`, `composite_score.py` |
| 5 | Open Interest Calculations | `oi_analysis.py` |
| 6 | Greeks Calculator | `greeks_poller.py`, `option_pricing.py` |
| 7 | Liquidity Score | `liquidity_score.py` |
| 8 | Expected Move Calculator + Step 6 adaptive learning (record mode) | `expected_move.py`; `trade_outcome.py` / `run_paper_tracker.py` (actual vs expected spot move) → `strike_learning.py` |
| 9 | Greeks Change Predictor | `greeks_change.py`, `greeks_phase.py` |
| 10 | Strike Price Selector / Strike Intelligence Engine | `strike_select.py`, `strike_intel.py` |
| 11 + 20 | Lot Sizing + Capital Allocation (ICARE) | `icare.py`, `capital_alloc.py`, `broker_funds.py` (reads `md:account:latest`) |
| 12 | Probability Engine | `probability_engine.py` |
| 13 | Trade Ranking Engine | `app/trade_ranking/` |
| 14 | Order Executor / Trade Entry | `app/order_executor/` |
| 15 | Exit Order Module | `app/order_executor/exit_engine.py`, `run_trade_journal.py` |
| 16 | Circuit Breaker / Kill Switch | `circuit_breaker.py`, `run_circuit_breaker.py` |
| 17 | Risk Management (incl. volatility-scaled risk) | `icare.py` (`ICARE_VOL_RISK_*`) |
| 18 | Adaptive Trailing SL & Re-entry | `adaptive_tsl.py` |
| 19 | Slippage Estimator | `slippage_estimator.py`, `run_slippage_estimator.py` |
| 21 | Portfolio Exposure Monitor | `portfolio_exposure.py`, `run_portfolio_exposure.py` |
| 22 | Trade Journal Engine | `trade_journal.py`, `run_trade_journal.py` |
| — | Broker Bridge (Angel SmartAPI) | `app/order_executor/broker.py` (disabled until static IP) |
| — | Add-ons 1–11 | `addons.py`, `run_addons.py` |

Module 23 (Accumulation Phase Detector) was dropped from the `-3.docx` spec.

---

## ❌ Spec gaps (from the -3.docx audit, 2026-11-13)

### A. Exit Order Module (doc ~4380–4716) — highest priority, it protects money
- [ ] Exit cond. 2: Fibonacci 0.236 retracement exit `H − (H − E) × 0.236` — not used for exits (M)
- [ ] Exit cond. 3: spread-deterioration exit — `md:optexit` signal exists but does not close positions (S–M)
- [ ] Exit cond. 4: selling pressure + adverse OI at the held strike (option OI vs futures short build-up) (M)
- [ ] Exit cond. 5: trend-exhaustion exit (multi-signal, configurable) (M)
- [ ] Exit cond. 6: Greeks exit incl. theta burden `|θ/day| × days / premium`; disable when Greeks are stale (M)
- [ ] Exit aggregator + priority (SL > TSL > Fib > spread > flow > Greeks > exhaustion); one `EXIT_POSITION` command with `primary_reason`, `triggered_reasons`, TSL / Fib prices, urgency (M)
- [ ] Exit policy config (Fib level, spread limit, confirmation count, intraday/overnight) (S)
- [ ] Exit record: secondary triggers + market / Greeks snapshot at exit (S)
- [ ] **Decision needed:** cond. 1 says a TSL hit exits without confirmation; TSL step 5 says confirm first. Code confirms (can delay the exit) (S)

### B. Circuit Breaker (doc ~4717–4950)
- [ ] Prototype caps: ₹4,500 daily, ₹1,500 per trade, max 5 entries/day, halt after 3 consecutive losses (code: ₹25,000 / ₹10,000, no entry or loss-streak count) (S)
- [ ] Daily P&L net of brokerage/charges; toggle realized-only vs realized + unrealized; critical-fault response toggle (S)
- [ ] Extra hard limits: max order qty/value, capital deployed, correlated loss, total open risk (S–M)
- [ ] 7-state machine: ARMED, WARNING, TRADING_HALTED, EMERGENCY_STOP, RECONCILING, LOCKED, RECOVERY_CHECK (code has 3) (M)
- [ ] Recovery check before reset (orders, positions, data freshness, risk budget, engine health) + authorised reset (M)
- [ ] Fault monitor: stale market feed by default, NaN/invalid prices, clock skew, lot-size violations, exit engine stopped with open positions, duplicate commands (M)
- [ ] Dashboard: circuit state panel + manual kill button recording who/when/why; alert channel (M)
- [ ] Three-way broker reconciliation + cancel verification (L, live-blocked)

### C. Risk Management + Slippage (doc ~4950–5460)
- [ ] Volatility multiplier table: Low 0.8 / Normal 1.0 / High 0.7 / Extreme 0–0.4 or block, plus time-to-expiry / theta sizing (code: different formula, off by default) (M)
- [ ] Risk per lot includes charges + slippage buffer (S)
- [ ] Correlation engine; reserve capital for pending orders (M)
- [ ] Fresh funds + margin re-check right before each order / slice (S)
- [ ] Low-capital mode: report the capital shortfall (S)
- [ ] Slippage: dynamic limit `min(policy, trade economics, liquidity)`, reject stale quotes, slice recommendation (M)
- [ ] Slippage: join estimate to actual fill → estimation error + recalibration (M)

### D. Greeks regime, trade structuring, signals (doc ~1046–2675)
- [ ] Greeks Phase 4 **Markdown** (put delta ↑, call delta collapse, put OI ↑) (M)
- [ ] 4-phase weighted scoring matrix + outputs (accumulation probability, breakout readiness, trend strength, exit readiness) + Greeks Participation Score (M)
- [ ] Trade Structuring rejects: confidence < 60, expected move < 0.5 %, delta > 0.85, IV too high; strike tier by move (>1.5 % ITM, >0.5 % ATM); SL below structure level (S–M)
- [ ] Vacuum-zone publisher for the Expected Move ×0.4 multiplier (currently always None) (M)
- [ ] Volume-weakening exit; market-breadth gate at entry; Trade Ranking NO_TRADE veto on weak trend / high vol (S each)
- [ ] Learning buckets add IV / OI / bid-ask / volume dimensions; ICARE correlation input (S–M)

### E. Portfolio Intelligence (doc ~5460–5720)
- [ ] Breadth upgrade: data freshness + coverage, (A−D)/(A+D), 1/5/15-min and sector breadth, divergence (M)
- [ ] Volatility-regime classifier (low / normal / high / extreme vs own history; VIX, IV, realised vol) (M)
- [ ] Recommender snapshot APPROVE / REDUCE_SIZE / WAIT / REJECT with reason codes, dynamic position count, deployable capital (M)
- [ ] Streamlit "Portfolio Intelligence" panel (M)
- [ ] Historical intelligence: analogues, walk-forward validation, calibration, drift monitor (L)

### F. AI Trade Journal (doc ~5723–5989)
- [ ] Record premium path with timestamps of max/min, bid/ask/depth snapshots, indicator context, post-exit observation window (M–L)
- [ ] Event log + database tables (trades, trade_events, snapshots, parameters, outcomes, counterfactual exits, predictions, model versions, data quality) (L)
- [ ] Counterfactual exit replay (A/B trail settings on identical paths) with expectancy, drawdown, MFE capture, cut-short winners (L)
- [ ] Exit probabilities P(+20/40/60/100 % before stop) and expected net P&L per exit rule (L)
- [ ] Parameter recommendation → validation → approval flow; per-trade learning report (M–L)
- [ ] (Spec calls it future) profit-lock + runner two-layer exit (M)

---

## ⏳ Needs live account / market time (cannot be finished in code alone)

- [ ] Register a static IP with Angel One, then enable live trading (`EXEC_LIVE_ENABLED=1`) — DECISION.md §7 X-Q11
- [ ] Order-update WebSocket, live exit path E17, and the 1-lot live check of modify-quantity (P29)
- [ ] Run the full pipeline against live Redis during market hours; replay the 2026-09-21 archive in shadow mode (P22, P30)
- [ ] Turn on `STRIKE_USE_LEARNING=1` once `md:learning:strike_stats` has ≥ 50 samples per bucket
- [ ] Check that the SNAP_QUOTE feed fills `upper_circuit_limit` / `lower_circuit_limit` (add-on 1)
- [ ] Decide whether to enable `CB_EMERGENCY_EXIT=1` and `ICARE_VOL_RISK_BASELINE_PCT` (both off by default)
- [ ] Add critical worker streams to `CB_HEARTBEAT_STREAMS` once you know which run steadily
