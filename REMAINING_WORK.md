# Option Rider — Remaining & Partly Done Work

> Gap analysis of `Option Rider Algo For Algobuilders-3.docx` against the code in `angel_md_redis/app/`.
> Status as of 2026-11-11 (last commit: `56a99f2 Testing changes`). The repo is built through **Module 9 — Greeks Change Predictor**.

---

## ✅ Done (for reference)

| # | Module | Code |
|---|---|---|
| 1 | Indicator Signals (Supertrend, EMA Cross, Pivots, multi-timeframe filter) | `supertrend.py`, `ema_cross.py`, `pivots.py`, `htf_trend_filter.py`, `indicator_score.py` |
| 2 | Volume Analyzer | `volume_analyzer.py` |
| 3 | Market Regime Detector | `market_regime_detector.py` |
| 4 | Bid-Ask Analyzer (all 7 sub-parts) | `bidask_analyzer.py`, `smart_money.py`, `order_flow.py`, `stock_entry_exit.py`, `option_liquidity_exit.py`, `bidask_imbalance.py`, `composite_score.py` |
| 5 | Open Interest Calculations | `oi_analysis.py` |
| 6 | Greeks Calculator | `greeks_poller.py`, `option_pricing.py` |
| 7 | Liquidity Score | `liquidity_score.py` |
| 8 | Expected Move Calculator | `expected_move.py` |
| 9 | Greeks Change Predictor | `greeks_change.py`, `greeks_phase.py` |

---

## ⚠️ Partly Done (needs rework to match the doc)

### 10. Strike Price Selector — `strike_select.py`
- **Current:** only picks ATM, OTM-n or the OI-target strike.
- **Doc requires:** a *Strike Intelligence Engine* that scores ATM ±10 strikes 0–100 and ranks them, weighing:
  - [x] Expected move fit
  - [x] Liquidity (OI + Volume) — uses `md:liquidity:score`; volume-only fallback (Greeks API has no OI)
  - [x] Bid-ask execution quality
  - [x] Greeks (Delta / Gamma / Theta / Vega) + projected Greeks at +1 EM (in-process reprice)
  - [x] Historical performance of similar setups — behind `STRIKE_USE_LEARNING=1`, needs >= 50 samples/bucket
  - [x] Trade only if the top-ranked strike clears a confidence threshold — `STRIKE_MIN_CONFIDENCE` (default 70)
- **Status:** done; ATM +/- `STRIKE_CANDIDATES_AROUND` (default 3, max 10). Not yet validated on live Redis.

### Capital Allocation — `capital_alloc.py`
- **Current:** only splits capital between calls and puts by market regime.
- **Doc requires:** capital handling folded into **Risk Management (18)** and **Trade Ranking (section 21 – Capital Allocation)** — extend accordingly.
- [x] Reserve + max utilisation budget, charges model, whole-lot limits (budget/risk/liquidity/portfolio/side), score-weighted batch allocation, low-capital fallback to next top3 strike
- [ ] Broker funds (`app/broker_funds.py` is a stub) and real open-position schema

### 8. Expected Move — Step 6: Adaptive Learning
- **Current:** not implemented.
- **Doc requires:** store every trade's expected vs actual move and learn which strike (ATM / ATM+1 / ATM+2 / ITM / OTM) performed best per setup.
- [x] Record mode: paper tracker -> `md:journal:outcome` -> bucket stats `md:learning:strike_stats`
- [ ] Actual underlying move vs expected (needs a spot source in the tracker)
- [ ] Turn on `STRIKE_USE_LEARNING=1` after enough samples

---

## ❌ Not Started

### 11. Lot Sizing
- [ ] Account information input
- [ ] Inputs from Trade Decision Layer, Strike Engine, Portfolio Engine
- [ ] Low-capital affordability logic

### 12. Probability Engine
- [ ] Pure function — combines weighted module signals into one 0–100 confidence score
- [ ] Modules must NOT be weighted equally
- [ ] Returns a JSON object
- [ ] Never places orders, calculates SL, or selects strikes

### 13. Trailing Stoploss
- [ ] Initial SL
- [ ] Dynamic trailing SL logic
- [ ] Reset only when the Probability Engine generates a completely new setup

### 14. Trade Ranking Engine (27 sub-sections)
- [ ] Inputs from all upstream modules; weighted (not equal)
- [ ] Normalize every score
- [ ] First-layer quality check
- [ ] Directional agreement + directional conflict penalty
- [ ] Probability as a separate factor (not just another score)
- [ ] Expected profitability
- [ ] Strike selection / lot sizing / trailing SL scores
- [ ] Dynamic stop-loss selection and logic
- [ ] Final trade score + decision bands (TAKE / WATCH / REJECT / NO TRADE)
- [ ] Hard rejection conditions
- [ ] Compare ALL candidates; don't force multiple trades
- [ ] Capital allocation
- [ ] Final output object

### 15. Order Executor / Trade Entry (30 sub-sections)
- [ ] Receive and re-verify the command from Trade Ranking
- [ ] Latest bid-ask; spread calculation
- [ ] Near-available-rate execution
- [ ] Slippage budget (executor's responsibility)
- [ ] Available margin check; maximum affordable lots
- [ ] Lot sizing vs executor quantity
- [ ] Brokerage / per-order cost; brokerage-aware completion decision
- [ ] Partial fill manager; don't keep reordering forever
- [ ] Liquidity-based order slicing
- [ ] Execution state machine + decision logic
- [ ] Market-runaway and margin-change handling
- [ ] Kill switch integration
- [ ] Final execution record → Trade Journal; feed back into Trade Ranking

### 16. Exit Order Module
- [ ] Seven exit conditions
- [ ] Exit priority (which condition wins)
- [ ] Exit decision configuration
- [ ] Output command to Order Executor
- [ ] Developer acceptance checklist

### 17. Circuit Breaker / Kill Switch
- [ ] Daily loss cap
- [ ] Max loss per trade and total exposure
- [ ] Entry/exit mismatch detection
- [ ] Command watchdog (commands not followed / not filled)
- [ ] Software bug / abnormal behaviour detection
- [ ] Emergency open-position protection
- [ ] Manual kill switch
- [ ] Recovery and restart protection
- [ ] Loss-cap config + circuit breaker state machine

### 18. Risk Management
- [ ] Distinguish capital from margin
- [ ] Dynamic capital utilisation engine
- [ ] Capital distribution among trades
- [ ] Low-capital mode (maximise affordability without forcing trades)
- [ ] Risk-based position sizing
- [ ] Dynamic risk adjustment by volatility
- [ ] Portfolio-level risk and correlated trades
- [ ] Broker margin and funds synchronisation

### 19. Slippage Estimator
- [ ] Best available bid/ask
- [ ] Spread and expected slippage
- [ ] Execution cost from order-book depth
- [ ] Estimate before order placement

### 20. Portfolio Exposure Monitor
- [ ] Live exposure across open positions

### 21. Trade Journal Engine
- [ ] Store every trade (stock, strike, expected/actual move, entry, exit, OI, volume, bid-ask, P&L)
- [ ] Feed back into Trade Ranking and Expected Move learning

### Broker Bridge — AngelOne SmartAPI
> The code has no order placement at all.
- [ ] Order placement
- [ ] Order status polling
- [ ] Rejection handling
- [ ] Lot size and freeze quantity rules for NSE options

---

## ❌ Add-ons (none built)

- [ ] 1. Upper/lower circuit logic
- [ ] 2. Daily volume % gain vs 20 SMA
- [ ] 3. Box breakout/breakdown from the last 60-minute range
- [ ] 4. Volume spike
- [ ] 5. Sector-wise buying/selling heatmap
- [ ] 6. Pre-market data analyser
- [ ] 7. Heavy buying/selling in the past few minutes
- [ ] 8. Fast price move up/down
- [ ] 9. Volume breakers up/down
- [ ] 10. Intraday volume increase %
- [ ] 11. Average sector change

---

## 🧹 Housekeeping

- [ ] `Option-rider-algo-specs.md` is out of date. It still lists 23 modules, including "Smart Stop Loss", a separate "Capital Allocation" layer and "Accumulation Phase Detector", which the new doc removed or merged. It also marks 10–23 as not yet specified. Regenerate it from `-3.docx`.
- [ ] Git shows the old `Option Rider Algo For Algobuilders.docx` and its lock files as deleted, and the new `-3.docx` as untracked. Commit that rename if intended.

---

## Suggested Build Order

1. Broker Bridge
2. Strike Selector (rework)
3. Lot Sizing
4. Probability Engine
5. Trailing Stoploss
6. Trade Ranking Engine
7. Slippage Estimator
8. Risk Management
9. Order Executor
10. Exit Order Module
11. Circuit Breaker / Kill Switch
12. Portfolio Exposure Monitor
13. Trade Journal Engine
14. Add-ons
