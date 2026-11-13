# Option Rider — Remaining Work

> Gap analysis of `Option Rider Algo For Algobuilders-3.docx` against the code in `angel_md_redis/`.
> Status as of 2026-11-13. All code items are built and unit-tested (686 passed, 2 Linux-only skipped).
> What remains needs a live Angel One account, market hours, or accumulated trade data.

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

## ⏳ Needs live account / market time (cannot be finished in code alone)

- [ ] Register a static IP with Angel One, then enable live trading (`EXEC_LIVE_ENABLED=1`) — DECISION.md §7 X-Q11
- [ ] Order-update WebSocket, live exit path E17, and the 1-lot live check of modify-quantity (P29)
- [ ] Run the full pipeline against live Redis during market hours; replay the 2026-09-21 archive in shadow mode (P22, P30)
- [ ] Turn on `STRIKE_USE_LEARNING=1` once `md:learning:strike_stats` has ≥ 50 samples per bucket
- [ ] Check that the SNAP_QUOTE feed fills `upper_circuit_limit` / `lower_circuit_limit` (add-on 1)
- [ ] Decide whether to enable `CB_EMERGENCY_EXIT=1` and `ICARE_VOL_RISK_BASELINE_PCT` (both off by default)
- [ ] Add critical worker streams to `CB_HEARTBEAT_STREAMS` once you know which run steadily
