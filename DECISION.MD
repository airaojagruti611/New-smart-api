# DECISION — Plan for the Partly Done Modules

> Covers the three **⚠️ Partly Done** items in `REMAINING_WORK.md`.
> Source spec: `Option Rider Algo For Algobuilders-3.docx` (§8 Step 6, §10 Strike Price Selector, §14.21 Capital Allocation, §18 Risk Management).
> Written 2026-11-11. Estimates are in **developer-days (1 day ≈ 6–7 focused hours)** and include tests and pipeline wiring.

---

## Summary

| # | Item | What changes | Estimate |
|---|---|---|---|
| A | Strike Price Selector → **Strike Intelligence Engine** | `strike_select.py` goes from "pick ATM/OTM-n" to "score and rank ATM ±N strikes 0–100" | **5 days** |
| B | Capital Allocation → **Dynamic Capital Engine** | `capital_alloc.py` goes from "regime CE/PE split × 5%" to reserve, deployable budget, score-weighted allocation and whole-lot limits | **3.5 days** |
| C | Expected Move Step 6 → **Adaptive Learning (record mode)** | New trade-outcome store plus per-bucket stats that feed back into strike ranking | **4 days** |
| — | Integration, dashboard/Excel/health updates, buffer | | **2 days** |
| | **Total** | | **≈ 14.5 dev-days (~3 weeks)** |

Order: **A → B → C**. B uses A's strike score and lot size. C learns from the fields A records.

---

## Current State (what exists today)

- **Pipeline:** `entry_trigger` → `md:strike:select` → (`greeks_change`, `capital_alloc`).
- `strike_select.py` only chooses ATM, OTM-n (on "strong" signals) or the OI-target strike. It doesn't score anything.
- `greeks_change.py` (Module 9) runs **after** one strike has been chosen. It never compares candidate strikes.
- `capital_alloc.py`: `side_budget = capital × regime CE/PE %` and `trade_notional = side_budget × 5%`. It has no reserve, no lot rounding, no cost per lot and no comparison across candidates.
- `STRIKES_AROUND` defaults to **0**, so the WebSocket only streams the **ATM** contract. Non-ATM strikes have no live bid-ask, OI or volume.
  - `greeks_poller` already caches the **full chain** of Greeks per underlying/expiry (`md:greeks:latest:{UND}:{EXP}`), so Greeks are available for every strike.
- No trade has ever been placed (no Broker Bridge or Executor), so there is **no outcome data** to learn from yet.

---

## A. Strike Intelligence Engine — 5 days

### What I will implement
1. **Candidate generation.** Build ATM ±N strikes on the signal side (CE for BUY CALL, PE for BUY PUT), using the existing `build_atm_option_tokens`. Set N with `STRIKE_CANDIDATES_AROUND`; default 3, maximum 10.
2. **Per-strike inputs:**
   - Greeks (Δ, Γ, Θ, Vega, IV), from the full-chain cache in `md:greeks:latest:{UND}:{EXP}`
   - Liquidity score, from `md:liquidity:score:latest:{TS}` when the strike is streamed. Otherwise fall back to OI and volume from the Greeks/OI snapshot, and **reject** if there's no data at all.
   - Bid-ask execution quality, from `md:bidask:latest:{TS}`
   - Expected-move range, from `md:expected_move:latest:{SYM}`
   - Projected Greeks at +1 EM, computed **in-process** by calling the pure functions in `greeks_change.py` (`resolve_current_state`, `reprice_option`). This means no new stream.
3. **Doc scoring model (§10):**
   ```
   Strike Score = 25% Liquidity + 25% Expected-Move Fit + 20% Delta Suitability
                + 15% Theta Efficiency + 10% Gamma Opportunity + 5% Vega Stability
   ```
   - **EM fit:** 100 inside the expected range, decaying linearly to 0 at 2× EM beyond it.
   - **Delta suitability:** a target band from the regime. Strong trend 0.60–0.75, normal 0.45–0.60, high volatility 0.35–0.50, expiry day 0.55–0.70. Read from `md:regime:latest`.
   - **Theta efficiency:** `theta × expected hold time`. Hold time is configurable (`STRIKE_EXPECTED_HOLD_MIN`, default 30).
   - **Gamma:** rewarded in trending regimes, penalized in choppy ones.
   - **Vega:** penalized when IV is expected to contract.
   - All weights and bands come from env/config, so they can be tuned after backtesting.
4. **Hard filters:** liquidity < 70 → reject; spread above the configured %; no Greeks; stale data older than X seconds.
5. **Output.** Keep `md:strike:select` **backward compatible**: same fields, with the top-ranked strike as `strike`/`token`/`tradingsymbol`, so `greeks_change` and `capital_alloc` keep working. Add:
   - `strike_score`, `confidence`, `reasons` (✓ list)
   - `top3` (JSON): strike, score, Δ, Θ-risk for the hold, liquidity, execution quality
   - `status=SKIP reason=below_confidence` when the best score < `STRIKE_MIN_CONFIDENCE` (default 70; the doc example uses 90)
6. **OI target.** The existing `oi_target_strike` override becomes a **score bonus** instead of a hard override.

### Decision needed: data coverage for non-ATM strikes
Streaming ±10 strikes × 2 sides × 220 stocks ≈ 9,000 tokens, which is far over the SmartAPI WebSocket subscription limit.
**My choice:** default ±3 on the signal side only, scored with Greeks-chain data (all strikes) plus live bid-ask where it's streamed. Strikes without live bid-ask get a reduced execution score instead of being invented. Raise `STRIKES_AROUND` only if you confirm the token budget.

### Files
- `app/strike_select.py`: new `score_strike()`, `rank_strikes()` and `StrikeCandidate` dataclass; pure, no I/O
- `run_strike_select.py`: fetch the extra Redis inputs and emit the new fields
- `tests/test_strike_select.py`: new; covers the doc's worked example (24500 CE wins at 94)
- `app/dashboard_data.py`, `export_client_excel.py`, `pipeline_health.py`: show score, confidence and top 3

### Breakdown
| Task | Days |
|---|---|
| Pure scoring/ranking module + config | 1.5 |
| In-process Greeks projection per candidate | 0.5 |
| Runner wiring, Redis reads, fallbacks, backward-compatible output | 1.5 |
| Unit tests (doc examples, edge cases, missing data) | 1.0 |
| Dashboard / Excel / health columns | 0.5 |
| **Total** | **5.0** |

---

## B. Dynamic Capital Engine — 3.5 days

### What I will implement (doc §18.2–18.5, §14.21)
1. **Capital vs. cost:** `cost_per_lot = premium × lot_size + est_charges`. Lot size comes from ScripMaster; premium comes from the option LTP or ask.
2. **Budget formulas:**
   ```
   C_deploy = max(0, C_available − C_reserve)
   B_new    = min(C_deploy, C_available × U_max)
   ```
   - `C_available`: from config `TOTAL_CAPITAL` minus capital already used by open `md:position:open:*`. Live broker funds (`getRMS`) arrive with the Broker Bridge, behind a stub interface I'll add now.
   - `C_reserve` and `U_max`: config (defaults ₹ reserve = 20%, `U_max` = 80%).
3. **Score-weighted allocation.** Score candidates from the same scan window (default 60 s) together instead of one at a time:
   ```
   W_i = S_i / ΣS_j     A_i = B_new × W_i
   ```
   - `S_i` = strike score × confidence (from A), scaled down in high-volatility or poor-liquidity regimes
   - Empty or zero denominator → allocate nothing
4. **Whole-lot limit:** `L_i = min(L_budget, L_risk, L_liquidity, L_portfolio)`, rounded **down**. If that's 0, reject; never round up.
   - `L_risk`: max loss per trade ÷ (premium × SL% × lot_size). Uses a configured SL% until Trailing SL (13) exists.
   - `L_liquidity`: a fraction of top-of-book depth on the ask side
   - `L_margin`: the same as `L_budget` for long options. Kept as a hook for the Broker Bridge.
5. **Regime CE/PE split** stays as an **extra cap** per side, not the main sizing logic.
6. **Low-capital mode:** if 1 lot of the best strike isn't affordable, try the next-ranked affordable strike from `top3`. Never force a trade.
7. **Output:** `md:capital:alloc` gains `lots`, `qty`, `cost_per_lot`, `allocated`, `deployable_budget`, `binding_limit` (which `L_*` capped it) and `reason`. Existing fields stay.

### Files
- `app/capital_alloc.py`: rewritten as pure functions (`deployable_budget`, `allocate`, `lot_limits`)
- `run_capital_alloc.py`: batch window, open-position read, lot size lookup
- `tests/test_capital_alloc.py`: new; covers the doc's Scenario A (4 trades) and B (12 trades) with a ₹1,00,000 account

### Breakdown
| Task | Days |
|---|---|
| Pure budget + allocation + lot-limit functions | 1.0 |
| Batch-window runner, open-position accounting, lot size/premium lookup | 1.0 |
| Low-capital fallback to the next-ranked strike | 0.5 |
| Tests (doc scenarios, zero/denominator, rounding) | 0.75 |
| Dashboard/Excel fields | 0.25 |
| **Total** | **3.5** |

### Out of scope here
Correlated-trade risk, volatility-based risk adjustment and broker funds sync belong to **Risk Management (18)** in `REMAINING_WORK.md`. I'll leave hooks for them.

---

## C. Adaptive Learning (record mode) — 4 days

### Constraint
Nothing can be learned until trades exist, and the Executor isn't built yet. So I'll build the **recording and stats** half now, run it on **paper trades** (every `OK` strike-select treated as a virtual entry), and keep weight adjustment **off by default** until the sample size passes a threshold.

### What I will implement
1. **Outcome record:** a minimal schema that the full Trade Journal (21) will extend later. One row per (paper or real) trade, containing:
   - Context: stock, market phase/regime, expected move, chosen strike, strike bucket (ATM / ATM±1 / ATM±2 / ITM / OTM), Δ and Θ at entry, liquidity score, bid-ask spread
   - Results: entry/exit premium, holding time, P&L, **MFE / MAE**, actual underlying move vs. expected
   - Stored in `md:journal:outcome` (stream) and archived to Parquet/CSV with the existing archiver pattern
2. **Paper-trade tracker:** opens a virtual position on `md:strike:select OK` and tracks MFE/MAE from `md:ticks:opt`. Closes on a configurable rule: target = +1 EM premium, stop = SL%, time-out = expected hold, or end of session.
3. **Stats job:** groups outcomes by (regime, EM bucket, strike bucket, delta band) and computes win rate, expectancy and average MFE/MAE. Published to `md:learning:strike_stats`. This answers the doc's questions, e.g. "for a 40-pt EM, does ATM+1 beat ATM?"
4. **Feedback into A:** a 6th scoring term, **Historical Performance (15%)**, as in the §8 model. It's used only when a bucket has ≥ `LEARN_MIN_SAMPLES` (default 50) trades; otherwise it's neutral at 50. Off by default with `STRIKE_USE_LEARNING=0`.

### Files
- `app/trade_outcome.py`: schema + MFE/MAE tracker (pure)
- `run_paper_tracker.py`: new worker
- `app/strike_learning.py` + `run_strike_learning.py`: bucket stats
- `run_archiver_outcomes.py`: archive to Parquet/CSV
- `run_all.ps1` / `run_all.sh` / `stop_all.ps1` / `pipeline_health.py`: register the new workers
- `tests/test_trade_outcome.py`, `tests/test_strike_learning.py`

### Breakdown
| Task | Days |
|---|---|
| Outcome schema + MFE/MAE tracker | 1.0 |
| Paper-trade worker + exit rules | 1.0 |
| Bucket stats job + Redis/archive output | 1.0 |
| Feedback term in strike score (gated) | 0.5 |
| Tests + run scripts + health registration | 0.5 |
| **Total** | **4.0** |

---

## Integration & Buffer — 2 days
- End-to-end run during market hours: check the stream flow `entry_trigger → strike_select → greeks_change / capital_alloc → paper_tracker`.
- Update `Option-rider-algo-specs.md` and `RUN.md` for the new env vars and workers.
- Fix issues found in the live run.

---

## Timeline

| Week | Work |
|---|---|
| Week 1 | A — Strike Intelligence Engine (5 d) |
| Week 2 | B — Dynamic Capital Engine (3.5 d) → start C |
| Week 3 | C — Adaptive Learning (rest) + integration / live test (2 d) |

**≈ 14.5 developer-days, about 3 working weeks.** The learning output only becomes meaningful after a few weeks of paper-trade data has built up.

---

## Open Decisions (defaults used unless you say otherwise)

| Decision | Default I'll use |
|---|---|
| Strikes scored per signal | ATM ±3 on the signal side (WebSocket token limit) |
| Minimum confidence to emit a strike | 70 (doc example says 90; too strict until weights are tuned) |
| Capital source until Broker Bridge exists | `TOTAL_CAPITAL` env var |
| Reserve / max deployment | 20% / 80% |
| Learning weight in strike score | Off until ≥ 50 trades per bucket |
| Paper trades on every OK strike-select | Yes |
