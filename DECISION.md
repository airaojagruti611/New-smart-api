# DECISION — Strike Intelligence, Probability Engine & ICARE

Status: **ACCEPTED** with defaults (Q1–Q6) · P0–P8 ✅ implemented (shadow / paper mode) · Date: 2026-10-04 · Scope: `angel_md_redis/`

Addendum 2026-10-04: **Adaptive Trailing Stop Loss & Re-entry Engine** (Module 18) — **ACCEPTED** with defaults (T-Q1…T-Q7) · P9–P15 ✅ implemented (shadow mode default) · §5.

Addendum 2026-10-04: **Trade Ranking Engine** (Module 13) — **ACCEPTED** with defaults (R-Q1…R-Q8) · P16–P21 ✅ implemented, P22 ✅ except the live-Redis run / archive replay (shadow mode default) · §6.

Addendum 2026-10-04: **Order Executor / Trade Entry Engine** (Module 14) — **ACCEPTED**, scope: shadow + paper now, live code disabled (X-Q1), live blocked until a static IP is registered (X-Q11); X-Q2…X-Q10 defaults accepted; checked against the 2026 SEBI / NSE / Angel One rules (§7.6) · P23–P28 ✅ implemented, P29 ◐ (live adapter + gates done and disabled; order WebSocket, exit path E17 and the 1-lot live check still open), P30 ✅ except a live-Redis run (shadow mode default) · §7. Also **T14**: the trailing stop distance is a % of the current (highest) premium, not of the entry price · §5.2.

Implements the Trade Decision Layer (spec Modules 10, 11, 12, 20) from the
pasted design: **Strike Intelligence Engine (SIE)**, **Probability Engine**,
and **Intelligent Capital Allocation & Risk Engine (ICARE)**, plus the
trade journal / adaptive-learning hooks.

---

## 1. Where we are today (what already exists)

| Need from the design | Existing module | Redis output | Gap |
|---|---|---|---|
| Greeks per strike (Δ Γ Θ ν IV) | `greeks_poller` + `joiner` + `greeks_phase` | `md:greeks:phase:latest:{TSYM}` | Only strikes that are subscribed. `STRIKES_AROUND=0` → **ATM only** |
| Intrinsic / extrinsic | `option_pricing.bs_price_greeks` | — | compute in SIE |
| Liquidity score 0–100 per contract | `liquidity_score` | `md:liquidity:score:latest:{TSYM}` | Same strike-coverage gap |
| Bid-ask spread / mid | `bidask_analyzer` | `md:bidask:latest:{TSYM}` | — |
| Expected move (direction, magnitude, confidence) | `expected_move` (Module 8) | `md:expected_move:latest:{SYM}` | — |
| Greeks Change Predictor | `greeks_change` (Module 9) | `md:greeks_change:latest:{SYM}` | Runs for **one** contract per symbol, not a chain |
| AMD phase (Accumulation/Markup/Distribution) | `greeks_phase` | `md:greeks:phase:*` | — |
| Confluence | `composite_score`, `entry_trigger` (aligned flags) | `md:composite:latest:{SYM}`, `md:entry:trigger` | — |
| OI positioning | `oi_analysis` | `md:oi:underlying:latest:{SYM}` | — |
| Market regime | `market_regime_detector` (breadth) | `md:regime:latest` | Only BULLISH/BEARISH/NEUTRAL breadth — no "strong trend / high vol / expiry" phases |
| Strike selection | `strike_select` (ATM / OTM offset / OI target) | `md:strike:select` | Rule-based, no scoring — **replaced by SIE** |
| Capital allocation | `capital_alloc` (regime CE/PE split × risk %) | `md:capital:alloc` | No lots, margin, SL risk, portfolio checks — **replaced by ICARE** |
| Account / margin data | — | — | **Missing** (no broker account adapter) |
| Open positions | read-only prefix `md:position:open:{TSYM}` | — | Nobody writes it yet |
| Stop loss | — (Module 18 not built) | — | **Missing** |
| Trade journal / historical win rate | — (Module 22 not built) | — | **Missing** |

Current chain: `entry_trigger → strike_select → capital_alloc` (no orders placed anywhere).

---

## 2. Decisions

### D1 — Follow the existing code pattern exactly
Each engine = **pure module** `app/<name>.py` (dataclasses + functions, no I/O)
+ **Redis runner** `run_<name>.py` (consumer group, `stream` + `latest:` key,
`LOGIC_IN/LOGIC_OUT` debug logging) + **tests** `tests/test_<name>.py`.
Every threshold/weight is a named constant overridable by env var.

### D2 — New pipeline order (matches the design's "Final Decision Pipeline")

```
md:entry:trigger (BUY CALL / BUY PUT)
      │
run_strike_intel.py   → md:strike:intel        (top-3 ranked strikes)
      │
run_probability.py    → md:probability         (0–100 score, grade, decision)
      │
run_icare.py          → md:icare               (lots + risk report, APPROVED/REJECTED)
      │
(Order Executor — not in scope; nothing is sent to the broker)
```
*(Update: Order Executor (Module 14) is now planned after ICARE — see §7.)*

### D3 — Shadow mode first, do not delete legacy modules
`strike_select` and `capital_alloc` keep running unchanged (Module 9
`greeks_change` reads `md:strike:select:latest`). New engines run **in
parallel** writing new streams. Dashboard shows legacy vs new side by side.
Switch-over (and retirement of legacy) is a later, explicit decision.

### D4 — Strike coverage: ATM ± N, default N = 5 (configurable up to 10)

> **P0 result:** local `.env` already runs `STRIKES_AROUND=10`; the 2026-09-21
> archive shows greeks + liquidity for 19–21 strikes × CE/PE per symbol. No
> config change needed. SIE uses `SIE_STRIKES_AROUND=5` inside that chain.
> RUN.md / DEPLOYMENT.MD updated from 0 → 10. Live check:
> `python check_strike_coverage.py [--around N]` (exit 0 = full coverage).
SIE needs a chain, but today only ATM is subscribed. Set
`STRIKES_AROUND=5` (env). With 2 symbols in `symbols.txt`:
2 × 2 sides × 11 strikes = 44 WS subs (cap is `MAX_WS_SUBS=950`, fine).
Scaling to ~180 F&O stocks would need ~4,000 subs → exceeds the cap;
then SIE would use only the **trade-direction side** and N=3 — revisit later.

### D5 — SIE scoring (per candidate strike, all components normalized 0–100)

| Component | Weight | Source / formula |
|---|---|---|
| Liquidity | 25% | `liquidity_score` from `md:liquidity:score:latest:{TSYM}`. **Hard reject < 70** (env `SIE_MIN_LIQUIDITY`) |
| Expected-move fit | 25% | Range = spot ± `expected_move`. Strike inside range → 100; beyond → linear decay to 0 at 2× EM distance |
| Delta suitability | 20% | 100 inside the regime's preferred band, linear decay outside (see D6) |
| Theta efficiency | 15% | `theta_risk = |θ/day| × hold_minutes / 375` as % of premium → lower is better |
| Gamma opportunity | 10% | Γ normalized across the chain; flipped to a penalty when phase = choppy/sideways |
| Vega stability | 5% | ν/premium; penalized when IV trend is falling (IV contraction) |

**Greeks change per strike (Module 4 of the design):** reuse
`greeks_change.reprice_option` / `option_pricing.bs_price_greeks` to reprice
**every candidate** at spot + expected_move after `hold_minutes`
(IV unchanged; ±2 vol pts as stress). Output: predicted Δ/Θ/Γ and
`expected_premium_gain`. Recorded as an approximation (BSM, no skew).

**Output:** top 3 strikes, each with strike_score, confidence,
Δ/Γ sensitivity, theta risk for hold time, liquidity score,
execution-quality score (spread % + depth), reasons list (✓/✗).
Also intrinsic / extrinsic value.

> **P2 implementation notes** (`app/strike_intel.py`, `run_strike_intel.py`):
> * Trade side only; ATM ± `SIE_STRIKES_AROUND` from ScripMaster; top `SIE_TOP_N`=3.
> * Missing components are **excluded and the score renormalized**;
>   `confidence = score × completeness` (unlike the Probability Engine, a strike
>   is not penalized for, e.g., an absent expected move — its confidence is).
> * EM fit: inside spot ± EM = 100, linear to 0 one further EM beyond (ITM side too).
> * Delta: 100 inside band, linear to 0 at 0.25 outside. Theta: decay over the
>   hold (`|θ/day| × hold/375`) as % of premium, 0 at 10%. Vega: ν/premium,
>   0 at 10%, ×2 when IV trend is down. Gamma: γ / chain max, inverted when EM
>   direction is NEUTRAL (choppy).
> * Execution quality = 70% spread (0 at 3%) + 30% top-5 bid depth (full at 20 lots).
> * Projection reprices at spot ± EM in the **trade direction** after the hold;
>   gain = model(after) − model(now) added to the observed mid; also IV −2 pts stress.
>   `em_conflict=1` when Module 8 direction opposes the trade (left to Probability).
> * Hard rejects: liquidity < `SIE_MIN_LIQUIDITY` (70), band RED, spread > 3%,
>   no premium, no Greeks.
> * Shadow wiring: `run_all.sh/.ps1`, layers archiver (`md:strike:intel`),
>   `pipeline_health.py`.
>
> **Replay on 2026-09-21 archive:** median stock-option liquidity score was **48**
> (only ~12% GREEN ≥75), so the design's 70 cutoff leaves 1–3 eligible strikes per
> side. EMs were small (7–17 pts vs 10–20 pt strike steps). Kept 70 (design value);
> if too strict in shadow mode, `SIE_MIN_LIQUIDITY=50` matches the liquidity
> module's own YELLOW band.

### D6 — Market phase for delta band (derived, since regime detector lacks it)

| Phase | Rule (first match) | Preferred Δ |
|---|---|---|
| Expiry day | today == contract expiry | 0.55–0.70 |
| High volatility | ATM `iv_pct` change ≥ `IV_TREND_UP_PCT` (5%) or EM pct ≥ 2% | 0.35–0.50 |
| Strong trend | EM direction `STRONG_*` and HTF + Supertrend aligned | 0.60–0.75 |
| Normal trend | otherwise | 0.45–0.60 |

Expected hold time: env `SIE_HOLD_MINUTES` default 60 (Module 8's horizon).

### D7 — Probability Engine = pure function, input mapping

Weights exactly as given: confluence 25, direction 20, intensity 15, AMD 10,
historical 10, OI 10, liquidity 5, greeks 5 (= 100).

| Input | Mapped from (normalized 0–100) |
|---|---|
| confluence | `composite_score` (−1..+1 → 0..100, sign-aligned to trade side) + entry_trigger `aligned` |
| direction | Module 8 `direction_score` sign-aligned to trade side |
| intensity | volume signal + `volume_surge` + EM confidence |
| amd | `greeks_phase.phase`: MARKUP=100, ACCUMULATION=70, NEUTRAL=40, DISTRIBUTION=10 (for longs) |
| historical | journal win rate for (symbol, phase, delta bucket); **50 (neutral) until samples exist** |
| oi | `oi_positioning` aligned with side |
| liquidity | chosen strike's liquidity score |
| greeks | SIE projected-Greek profile score |

Hard filters (reject regardless of score): liquidity < 40, spread % > threshold,
sideways regime, option liquidity band RED, **historical samples < 30** (see D8).
Bands: <50 REJECT · 50–65 WATCHLIST · 65–75 SMALL_POSITION · 75–85 TRADE · ≥85 HIGH_CONVICTION.
Grades A+/A/B/C/D. Returns exactly the JSON object in the design.
Never places orders, never picks strikes, never sets SL.

> **P1 implementation notes:** option spread threshold default **3%**
> (`MAX_SPREAD_PCT`, the design gave no number). Probability is rounded
> **half-up** and bands/grades use the rounded value so label and number
> agree. Missing components contribute 0 and are listed in `missing`.
> Sideways = regime label in {SIDEWAYS, RANGE, RANGEBOUND, CHOPPY}; mapping
> breadth `NEUTRAL` → sideways is decided in the P3 runner.

> Note: this number is a **weighted score, not a calibrated probability**.
> Once the journal has enough trades we calibrate it (bucket score → observed
> win rate). Until then UI labels it "Probability score".

### D8 — "Historical Samples < 30" filter would block every trade on day 1
Decision: during shadow mode this filter emits a **flag** (`LOW_SAMPLES`)
instead of rejecting; it becomes a hard reject when
`PROB_ENFORCE_MIN_SAMPLES=1`. Needs your confirmation (Q2).

### D9 — ICARE

* **Trade Quality** (0–100): Trend 20, EM 20, Probability 20, Strike 15,
  Liquidity 10, Greeks 10, Bid-Ask 5.
* **Risk class:** >95 A+ (15%) · 90–95 A (10%) · 80–90 B (7%) · 70–80 C (3%) · <70 REJECT.
* **Expected value:** `EV = p_win × avg_win − (1−p_win) × avg_loss`.
  - Before the journal has data: `avg_win` = SIE `expected_premium_gain` × qty,
    `avg_loss` = premium − stop-loss price, `p_win` = probability score / 100.
    Tagged `ev_source=model`. After ≥30 trades in the bucket: `ev_source=journal`.
  - Only EV > 0 proceeds.
* **Stop loss (Module 18 not built):** SL premium = Module 9 adverse scenario
  (−1 EM) premium, floored by `ICARE_MAX_SL_PCT` (default 30% of premium).
* **Lots** = `MIN(margin limit, risk limit, capital-allocation limit, portfolio limit)`:
  - margin limit = available margin ÷ margin per lot. For **option buying**,
    margin per lot = premium × lot size (+ charges buffer).
  - risk limit = `MAX_RISK_PER_TRADE` ÷ ((entry − SL) × lot size)
  - capital limit = capital × class % ÷ (premium × lot size)
  - portfolio limit = `MAX_OPEN_TRADES`, `MAX_PORTFOLIO_RISK`, per-symbol cap
* **Portfolio protection rejects:** margin utilization > max, daily loss > limit,
  portfolio risk > limit, sector exposure > limit (sector map: optional
  `sectors.json`, check skipped if absent — flagged).
* Confidence never raises risk above per-trade / daily / portfolio caps
  (design's closing rule) — enforced by the MIN() and the hard rejects.
* Output: full execution report from the design (status APPROVED / REJECTED + reasons).

### D10 — Account data adapter (`app/broker_account.py`)
* **Live:** Angel SmartAPI `rmsLimit()` (getRMS, 2 req/s) for available cash /
  margin / utilised; optional `margin/v1/batch` API for exact margin
  (10 req/s, up to 50 positions). Polled every 30 s, cached at
  `md:account:latest`.
* **Paper (default):** reads `TOTAL_CAPITAL` env and computes used margin from
  open positions in `md:position:open:*`. `ACCOUNT_MODE=paper|live`.

### D11 — Journal & adaptive learning (later phase, offline only)
* `md:journal` stream + parquet via existing archiver: every field listed in
  both "Adaptive Learning" sections (phase, EM, strike, Δ/Θ at entry, liquidity,
  spread, hold time, PnL, MFE, MAE, quality, lots, margin, drawdown).
* Learning = **offline report** (`learn_weights.py`) that proposes new weights
  per bucket. Weights are **never auto-updated live**; a human approves them
  into env/config. Reason: avoid overfitting a few trades into live risk.
* Since no order executor exists, the journal is fed by **paper trades**
  (simulate fill at ICARE approval mid, exit by SL / target / time) so data
  accumulates now.

### D12 — Out of scope for this change
Order execution, Trade Ranking Engine (Module 13, multi-symbol capital split),
smart SL (18), slippage estimator (19). Hooks are left in the payloads.
*(Update: smart SL (18) is now planned as the Adaptive Trailing Stop Loss &
Re-entry Engine — see §5. Trade Ranking (13) is now planned — see §6.
Order execution is now planned as the Order Executor (Module 14) — see §7.)*

---

## 3. Implementation plan (phases)

| Phase | Deliverable | Files |
|---|---|---|
| **P0** | Widen strike coverage; verify per-strike greeks + liquidity keys exist for ATM±5 | `.env` `STRIKES_AROUND=5`, check script |
| **P1** | Probability Engine (pure, no deps) + tests incl. design's worked example (= 83.7 → 84) | `app/probability_engine.py`, `tests/test_probability_engine.py` |
| **P2** | SIE: chain loader, phase/delta band, 6 component scores, per-strike repricing, top-3 | `app/strike_intel.py`, `run_strike_intel.py`, `tests/test_strike_intel.py` |
| **P3** | Probability runner wired to SIE output + upstream latest keys | `run_probability.py` |
| **P4** | Broker account adapter (paper + live getRMS) | `app/broker_account.py`, `run_account.py` |
| **P5** | ICARE: quality, EV, class, lots MIN(), portfolio guards, report | `app/icare.py`, `run_icare.py`, `tests/test_icare.py` (design's 10/6/7/5 → 5 lots example) |
| **P6** | Paper-trade journal + MFE/MAE tracking | `app/trade_journal.py`, `run_trade_journal.py` |
| **P7** | Wiring: `run_all.sh/.ps1`, `stop_all`, layers archiver, `pipeline_health.py`, Streamlit tabs, `export_client_excel.py`, spec + RUN.md updates | existing files |
| **P8** | Offline learning report | `learn_weights.py` |

Each phase is testable on its own; P1→P5 gives a full shadow pipeline.

---

## 3b. Implementation notes P3–P8 (decisions made while building)

**P3 `run_probability.py`** — input mapping
* confluence = % agreeing of 6 votes: HTF bias, Supertrend bias, EMA state, volume
  signal, entry `aligned`, composite score sign.
* direction = Module 8 `direction_score` (−1..+1) side-aligned → 0..100.
* intensity = mean(volume signal −2..+2 side-aligned, Module 8 confidence) + 10 on volume surge.
* amd = greeks phase of the chosen contract (MARKUP 100 / ACCUMULATION 70 / NEUTRAL 40 / DISTRIBUTION 10).
* historical = journal bucket win rate, neutral 50 below 30 trades.
* **Sideways** = Module 8 direction NEUTRAL **and** market breadth NEUTRAL.
* Also writes `md:probability:rank` (zset) as the Trade Ranking view.

**P4 `app/broker_account.py`, `run_account.py`** — paper ledger by default; `ACCOUNT_MODE=live`
uses `rmsLimit()` (`net` = available margin, `utiliseddebits` = used, `m2m*` = day PnL) and falls
back to the paper ledger (flagged) if the call fails. ICARE also falls back (flag
`ACCOUNT_FALLBACK_PAPER`) if `md:account:latest` is older than 2 min.

**P5 `app/icare.py`, `run_icare.py`**
* Trade-quality mapping: trend = Probability confluence; expected move = Module 8 confidence
  (0 when Module 8 direction opposes the trade); probability; strike = SIE score;
  liquidity; greeks = SIE greeks sub-score; bid-ask = SIE execution quality. Missing = 0.
* Probability `SMALL_POSITION` caps allocation at class C (3%). `REJECT`/`WATCHLIST` never trade.
* Stop loss = SIE adverse-move repricing (spot one EM against the trade), clamped to
  **10–30 % of premium**; target = premium + projected gain.
* Max risk per trade = **min(₹7,500, 2 % of capital)** — the design's ₹7,500 would be 7.5 % of
  the current ₹1,00,000 `TOTAL_CAPITAL`.
* Added a 5th limit to the MIN(): Module 7 `final_entry_size` (safe lots for the contract's
  liquidity), and one open position per underlying.
* Sector map `sectors.json` (RELIANCE ENERGY, TCS/INFY IT); unknown symbols flagged `SECTOR_UNCHECKED`.

**P6 `app/trade_journal.py`, `run_trade_journal.py`** — paper only: entry at **ask**, marks and
exits at **bid**; exits SL / TARGET / TIME (SIE hold window) / EOD 15:20; no new entries after
15:20. Positions persist in `md:position:open:{TSYM}` (restart-safe; also read by the liquidity
module's scale-out logic). Stats buckets: ALL, SYM:{sym}, SYM_PHASE:{sym}|{phase}.

**P7 wiring** — `run_all.sh/.ps1`, layers archiver (`md:probability`, `md:icare`, `md:journal`),
`pipeline_health.py` (Modules 10/11/12/13/22 + account), Streamlit tab
"Strike · Probability · ICARE", client Excel rows, RUN.md, spec index note.

**P8 `learn_weights.py`** — read-only report: delta bucket × phase / hold, theta risk, spread,
probability calibration, quality band, risk class, exit reason; suggests best delta band per
phase only for buckets with ≥ 30 trades. Never edits config.

**Verified** — 117 unit tests; end-to-end run through real Redis (DB 15, then flushed):
SIE → probability 92 A+ → ICARE APPROVED 3 lots (risk-limited) → paper open/close →
journal stats + daily PnL + learning report.

**Expect in shadow mode:** with real stock-option data, Module 8 confidence is often low
(e.g. 22) and liquidity median ~48, so ICARE will REJECT most signals on trade quality < 70.
That is the design working as specified; review `md:icare` reasons after a few sessions
before loosening anything.

## 4. Open questions (answered 2026-10-04: all defaults accepted)

1. **Strike window:** OK with ATM ± 5 (vs ± 10 in the design)?
2. **Historical samples < 30:** flag-only during shadow mode (D8) — agree?
3. **Risk numbers:** capital, max risk per trade (design example ₹7,500),
   daily loss limit, max open trades, max margin utilization %?
4. **Account mode:** start with paper (`TOTAL_CAPITAL`) or connect live `getRMS` now?
5. **Long-only?** Engines assume option **buying** (BUY CALL / BUY PUT) only — confirm.
6. **Replace or parallel:** keep legacy `strike_select`/`capital_alloc` until you approve switch-over (D3)?

---

## 5. Adaptive Trailing Stop Loss & Re-entry Engine (Module 18) — IMPLEMENTED (shadow)

Source: design brief section "13. Trailing Stoploss". The brief's "13" is a
section number, **not** spec Module 13 (Trade Ranking). It is filed as
**Module 18 — Smart Stop Loss** (Risk Layer), which `pipeline_health.py`
already lists as "NOT IMPLEMENTED".

### 5.1 What exists today (relevant to this engine)

| Brief input | Existing source | Gap |
|---|---|---|
| Entry / current / highest price, open trade | `run_trade_journal.py` paper positions `md:position:open:{TSYM}` (`entry_premium`, `last_premium`, `max_premium`, `sl_premium`, marks at **bid** every `JOURNAL_MARK_SEC`) | No R-multiple field (computable: `(px−entry)/(entry−sl)`) |
| Current exits | `trade_journal.exit_reason`: SL / TARGET / TIME / EOD | Fixed levels from ICARE; no trailing |
| Supertrend | `md:supertrend:bias:latest:{SYM}` (MTF) | — |
| EMA 9 / EMA 26 | `md:ema:cross:latest:{SYM}` (`ema9`, `ema26`) | — |
| Pivots | `md:pivots:prevday:{SYM}` (P, R1/R2, S1/S2) | — |
| Swing high / low | — | **Missing** — new fractal helper on underlying 1m candles (`candle_io.read_last_candles`) |
| Fibonacci levels | — | **Missing** — compute from today's swing range (record only, T5) |
| ATR | `supertrend.atr_wilder()` (function, not published) | Compute in-engine from 1m/5m candles |
| Δ Γ Θ IV, DTE | `md:greeks:phase:latest:{TSYM}`, ScripMaster expiry | — |
| Expected move, direction score | `md:expected_move:latest:{SYM}` (Module 8) | — |
| Bid-ask score | `md:bidask:latest:{TSYM}` | — |
| Liquidity score | `md:liquidity:score:latest:{TSYM}` | — |
| OI behaviour | `md:oi:underlying:latest:{SYM}` (`buildup`), `md:oi:latest:{TSYM}` | — |
| Volume intensity | `md:volume:latest` | — |
| Distribution | `greeks_phase` phase `DISTRIBUTION`; `smart_money` `WATCH_DISTRIBUTION` | — |
| Regime (trending / sideways / volatile / news) | SIE `market_phase` (D6) + `md:regime:latest` breadth | No "news driven" source → not used (flag `NO_NEWS_FEED`) |
| Probability re-check | `app/probability_engine.py` (pure) + input mapping inside `run_probability.py` | Mapping must be refactored into a reusable function |

### 5.2 Decisions

#### T1 — Same pattern as D1; engine only manages an existing trade
`app/adaptive_tsl.py` (pure: dataclasses + functions, no I/O) +
`run_adaptive_tsl.py` (Redis runner) + `tests/test_adaptive_tsl.py`.
Every threshold is a named constant with an env override (`TSL_*`).
It never opens a trade by itself: a re-entry is only a **signal** that goes
back through ICARE for sizing + portfolio guards (T9).

#### T2 — Pipeline position

```
md:icare (APPROVED) ─► run_trade_journal.py  (Position Manager, paper fills)
                              │  md:position:open:{TSYM}
                              ▼
                       run_adaptive_tsl.py   (Module 18)
                              │  md:tsl:latest:{TSYM}, md:tsl (stream)
                              ▼
                       run_trade_journal.py  (acts as Exit Engine: closes on validated TSL exit)
                              │  md:journal (exit_reason=TRAILING_STOP)
                              ▼
                       run_adaptive_tsl.py   re-entry watch ─► md:tsl:reentry ─► run_icare.py ─► journal
```

Single writer rule: the TSL runner **never writes** `md:position:open:*`;
it writes its own state `md:tsl:state:{trade_id}` and the latest/stream
keys. Only the journal opens/closes positions (no race on the position key).
Order Executor / Trade Ranking from the brief's diagram stay out of scope (D12).

#### T3 — What is trailed: the **option premium at bid**
We only buy options (Q5), so "price" = the contract's premium, marked at
**bid** (what we can sell for — same as the journal). Market structure
(swings, Supertrend, EMA, pivots) is read on the **underlying** and
**side-aligned**: for a BUY PUT, "bullish trend" means underlying bearish,
"swing high break" means underlying breaking below its last swing low.
For the premium itself the engine keeps 1-minute premium bars per position
(built from its own polls) to get premium swing highs/lows.

#### T4 — Step 1 · Volatility score (0–100)

| Component | Weight | Normalisation |
|---|---|---|
| ATR % of spot (14 × 1m, Wilder) | 30 | 0 at 0.05 %, 100 at 0.40 % |
| Expected move % (Module 8) | 25 | 0 at 0.3 %, 100 at 2 % |
| IV change (contract `iv_pct` from greeks phase) | 15 | 50 at 0 %, 100 at +20 %, 0 at −20 % |
| Gamma: Δ shift for a 1 % spot move (`Γ × spot × 1 %`) | 15 | 0 at 0, 100 at 0.25 |
| Days to expiry | 15 | 100 at 0 DTE, 0 at ≥ 7 DTE |

Bands: **Low < 40 · Medium 40–70 · High ≥ 70**. Missing component →
excluded and renormalised (same as SIE, D5), flagged.

#### T5 — Step 2 · Trend strength (side-aligned, 0–100)

| Component | Weight | Source |
|---|---|---|
| Supertrend alignment | 25 | share of MTF timeframes aligned with trade side |
| EMA distance | 20 | `(ema9 − ema26) / ATR`, side-aligned, 100 at ≥ 1 ATR |
| Directional score | 25 | Module 8 `direction_score` side-aligned → 0..100 |
| Intensity | 15 | volume signal + surge (same mapping as P3 `intensity`) |
| AMD phase | 15 | MARKUP 100 / ACCUMULATION 70 / NEUTRAL 40 / DISTRIBUTION 10 |

Labels: **Weak < 40 · Moderate 40–60 · Strong 60–80 · Explosive ≥ 80**.
Pivots / Fibonacci are **recorded in context only** in v1 (no stop logic) —
a "structure stop" (premium repriced at the underlying swing via Δ) is a
later option behind `TSL_STRUCTURE_STOP=0`.

#### T6 — Step 3 · Dynamic TSL % selector (first match wins)

| # | Condition | Range | Pick inside range |
|---|---|---|---|
| 1 | Gamma explosion: DTE = 0 **and** gamma component ≥ 85 | 18–25 % | linear by volatility score |
| 2 | Expiry day: DTE = 0 | 15–20 % | linear by volatility score |
| 3 | Strong / Explosive + volatility Low | 4–6 % | linear by volatility score |
| 4 | Strong / Explosive + volatility Medium/High | 8–10 % | linear by volatility score |
| 5 | Moderate | 10–12 % | linear by volatility score |
| 6 | Weak (also sideways regime) | 12–15 % | linear by volatility score |

The brief's table has no "Strong + Medium volatility" row; Medium is
grouped with High (wider = safer). Explosive is treated as Strong.

**Ratchet rule (brief: "never widen after it has tightened"):**
`tsl_pct = min(previous_tsl_pct, new_tsl_pct)` and
`stop = max(previous_stop, highest × (1 − tsl_pct))`. Both only move one way.

**Profit tightening** (brief: "may tighten as profit grows"):
at ≥ 1R the stop is at least breakeven (entry); at ≥ 2R `tsl_pct × 0.8`;
at ≥ 3R `tsl_pct × 0.6`. R = `entry − ICARE SL`. Values env-configurable.

#### T7 — Step 4 · Trailing activation & the ICARE stop
* Until **activation**, the ICARE hard SL (`sl_premium`) is the only stop.
* Trailing **activates** when `highest ≥ entry × (1 + tsl_pct)`, i.e. once the
  trailing stop would sit at or above breakeven. (A 5 % trail from the first
  tick would stop out nearly every option trade on noise.)
* After activation: `stop = max(ICARE SL, highest × (1 − tsl_pct))`.
  Worked example from the brief: entry 100, high 120, 5 % → **114**;
  high 125 → **118.75** (unit test).
* `highest` = max **bid** since entry (journal's `max_premium`).

#### T8 — Step 5 · Exit validation (confirmed exit)
Exit signal when
`bid ≤ stop AND bidask_bearish AND direction_weak AND ema_weak`
**OR** `bid ≤ stop AND distribution_detected`.

| Check | Definition (side-aligned) |
|---|---|
| bidask_bearish | contract's bid-ask quantity imbalance (`md:imbalance:latest:{TSYM}`) is BEARISH |
| direction_weak | Module 8 direction score side-aligned < `TSL_DIR_WEAK` (0.0) **or** dropped ≥ 0.3 since entry |
| ema_weak | `ema9 − ema26` gap (side-aligned) shrinking over last 3 bars, or crossed against |
| distribution | greeks phase `DISTRIBUTION` on the contract, or smart money `WATCH_DISTRIBUTION` |

Safety valves (the confirmation must not let a loser run):
* **ICARE hard SL is always unconditional.**
* **Hard breach:** `bid ≤ stop × (1 − TSL_HARD_BREACH_PCT)` (default 3 %) → exit without confirmation.
* **Breach timeout:** bid below stop for > `TSL_CONFIRM_MAX_SEC` (default 60 s) → exit.
* **Stale input** (> 2 min old) counts as **confirming the exit** (fail-safe to flat).
* TIME and EOD exits from the journal stay unchanged. TARGET: while TSL is
  **active mode**, hitting ICARE's target does **not** close the trade — it
  tightens with the R tiers (let winners run). In shadow mode TARGET is unchanged.

#### T9 — Steps 6–8 · Exit context, re-entry watch, re-entry conditions
On a validated TSL exit, store (`md:tsl:context:{trade_id}`, also on the
journal record): `entry_price, exit_price, highest_price, trailing_stop,
tsl_pct, last_swing_high, last_swing_low, pivots, fib levels, trend label,
volatility score, exit_reason`. Re-entry is **only** considered after
`TRAILING_STOP` exits (not SL / TIME / EOD).

State machine per chain (chain = original trade + its re-entries):

```
NONE ─exit TSL─► WAITING_FOR_SWING_BREAK ─all checks pass─► REENTER (signal)
                    │                                        │
                    ├─ trend invalidated / watch timeout / EOD ─► EXPIRED
                    └─ 2 re-entries used & failed ──────────────► BLOCKED
```

`watch_price = max(highest premium of the trade, last premium swing high) + max(0.05, 0.15 %)`
(brief example: high 124 → 124.20).

Mandatory re-entry checks (all must pass; **stale/missing = fail**, fail-safe to flat):
1. **Price:** a completed 1-min premium bar **closes** ≥ `watch_price`, and the
   underlying closes beyond its last swing high (side-aligned).
2. **Supertrend** still side-aligned. 3. **EMA9 > EMA26** (CE) / **<** (PE).
4. **Bid-ask** imbalance BULLISH on the contract. 5. **Liquidity** ≥ `SIE_MIN_LIQUIDITY` (70).
6. **Probability** (recomputed now) ≥ floor of the original trade's band (e.g. TRADE → 75).
7. **OI:** fresh build-up in trade direction (underlying `LONG_BUILDUP` for CE,
   `SHORT_BUILDUP` for PE).

Re-entry is on the **same contract** (strike may have drifted from ATM; it
must still pass liquidity + spread hard filters). Brief example
100 → 124 → stop 118 → 115 → 123 = no trade; 124.20 close = REENTER (unit test).

#### T10 — Re-entry goes through ICARE, not straight to a fill
`md:tsl:reentry` (stream) carries the REENTER payload + the original
SIE/probability context + `parent_trade_id`, `reentry_no`. `run_icare.py`
consumes it like an entry (same quality / EV / lot / portfolio guards;
a re-entry can never size above the original trade's lots). APPROVED → journal
opens a paper position tagged with `parent_trade_id` / `reentry_no`.

#### T11 — Cooldown & blocking
* `TSL_MAX_REENTRIES=2` per chain.
* `TSL_COOLDOWN_MIN=10`: no re-entry within 10 min after **any** exit in the chain.
* Watch expires after `TSL_REENTRY_WATCH_MIN=60`, at 15:20, or when Supertrend flips against.
* If both re-entries fail (each closes at SL / TSL with PnL ≤ 0) → `BLOCKED` for
  (symbol, side). **"Completely new setup"** = a new entry_trigger for that
  symbol+side **after** the underlying Supertrend has flipped at least once since
  the block, or the next session. Until then ICARE rejects with `REENTRY_BLOCKED`.
  Key `md:tsl:block:{SYM}:{SIDE}`, TTL to end of day.

#### T12 — Shadow first (same principle as D3)
`TSL_MODE=shadow|active`, default **shadow**:
* shadow: engine computes everything and publishes; journal keeps its fixed
  SL/TARGET/TIME exits but stores the counterfactual
  (`tsl_would_exit_ts`, `tsl_would_exit_px`, `tsl_shadow_pnl`) on each record,
  and re-entry signals are published but ICARE tags them `SHADOW` and does not approve.
* active: journal closes on `exit_signal=true` with `exit_reason=TRAILING_STOP`;
  ICARE processes re-entries.
Switch to active only after comparing fixed vs trailing PnL from the journal
(`learn_weights.py` gets an "exit policy" section).

#### T13 — Output (exactly the brief's three shapes, plus diagnostics)
`md:tsl:latest:{TSYM}` / `md:tsl` stream:
* ACTIVE: `symbol, current_trailing_stop, trailing_percentage, highest_price, status, exit_signal, reentry_state`
* EXIT: `symbol, status=EXIT, exit_reason=TRAILING_STOP, reentry_state=WAITING_FOR_SWING_BREAK, watch_price`
* REENTER: `symbol, status=REENTER, entry_price, confidence, reason=SWING_HIGH_BREAK_WITH_CONFLUENCE`
  (`confidence` = recomputed probability score).

Extra fields: `trade_id, tradingsymbol, side, trend_strength, volatility_score,
tsl_rule, r_multiple, activated, checks{...}, flags[], mode`.
Loop: every `TSL_LOOP_SEC=2` s over open positions (reads bid from `md:bidask:latest:{TSYM}`).

#### T14 — The trail % is taken from the current premium, not the entry (added 2026-10-04)
Requirement: the dynamic stop is "% down from the **current** price".
Example: bought at 10, premium now 100, trail 10 % → the stop is **10 below 100 (= 90)**,
not 1 below the entry (10 % of 10).

```
stop distance = highest × tsl_pct        (highest = best bid since entry)
stop          = max(ICARE SL, previous stop, highest × (1 − tsl_pct))
```

* "Current" means the **highest bid since entry**, because the stop only ratchets up (T6):
  when the premium falls from 100 to 95 the stop stays at 90. It does not move down to 85.5.
* The entry price is used only for **activation** (T7: trailing starts once
  `highest ≥ entry × (1 + tsl_pct)`), the **breakeven floor** at ≥ 1R, and the R multiple
  used by the profit tiers. None of these sets the stop distance.
* `app/adaptive_tsl.manage()` already does this (`candidate = highest × (1 − pct)`).
  Phase **P23** adds the 10 → 100 → stop 90 example as a unit test so a later
  refactor cannot change it. The Trade Ranking proposal (R11) and the Order
  Executor (§7) only pass `tsl_pct` through. They never turn it into a price
  measured from the entry.

### 5.3 Implementation plan

| Phase | Deliverable | Files |
|---|---|---|
| **P9** | Pure helpers: fractal swing high/low (N=2), Fibonacci from day range, ATR %, 1-min premium bar builder | `app/market_structure.py`, `tests/test_market_structure.py` |
| **P10** | Pure engine: volatility score, trend strength, TSL selector + ratchet + R tiers, activation, exit validation + safety valves, exit context, re-entry state machine, cooldown/block. Tests for every worked example in the brief (114 / 118.75, never-widen, 124.20 re-entry, 2 failed re-entries → BLOCKED) | `app/adaptive_tsl.py`, `tests/test_adaptive_tsl.py` |
| **P11** | Refactor `run_probability.py` input gathering into `build_probability_inputs(r, symbol, side, strike_row)` (no behaviour change; existing tests stay green) | `run_probability.py` |
| **P12** | Runner: poll open positions, gather inputs, persist `md:tsl:state:*`, publish latest/stream/reentry, shadow vs active | `run_adaptive_tsl.py` |
| **P13** | Integration: journal honours TSL exit (active) / records counterfactual (shadow), re-entry tags; ICARE consumes `md:tsl:reentry` + `REENTRY_BLOCKED` check | `app/trade_journal.py`, `run_trade_journal.py`, `app/icare.py`, `run_icare.py`, their tests |
| **P14** | Wiring: `run_all.sh/.ps1`, `stop_all.ps1`, layers archiver (`md:tsl`, `md:tsl:reentry`), `pipeline_health.py` (Module 18 → implemented), Streamlit "Trailing stop" panel, client Excel rows, `learn_weights.py` exit-policy report, spec + RUN.md | existing files |
| **P15** | Verification: unit tests + end-to-end on Redis DB 15 with a scripted premium path (100 → 125 → 118 → 115 → 124.2) and a replay of the 2026-09-21 archive in shadow mode | — |

P9–P10 are testable with no Redis; P12 gives shadow output; P13 is the only
step that changes existing behaviour, and only when `TSL_MODE=active`.

### 5.4 Risks / notes
* **4–6 % trail on an option premium is very tight** (stock options often move
  5 % in a minute). The activation rule (T7) and confirmation (T8) soften it,
  but shadow data must confirm the ranges before `active`.
* Exit confirmation can delay an exit; the hard-breach / timeout valves cap that.
* Premium swing highs come from our own polls (no option candles in Redis) —
  resolution is limited to `TSL_LOOP_SEC`.
* All percentages are design values, not fitted; adjust only from journal evidence (D11).

### 5.5 Open questions (answered 2026-10-04: all defaults accepted)

* **T-Q1** Module number: file it as **Module 18 Smart Stop Loss** (brief called it "13")?
* **T-Q2** Trailing activation: **only after the stop would reach breakeven** (T7), or trail from the first tick as in a plain TSL?
* **T-Q3** Exit confirmation: **strict AND of all 4 checks + safety valves** (T8), or a looser "3 of 4"?
* **T-Q4** With TSL active, should hitting ICARE's fixed target still close the trade? Default: **no, the target only tightens the trail**.
* **T-Q5** Re-entry probability threshold: **floor of the original band** (e.g. 75), or the original trade's exact score (stricter)?
* **T-Q6** Cooldown 10 min: **after every exit in the chain**, or only after a failed re-entry?
* **T-Q7** Start in **shadow mode** (T12) and switch to active after N sessions of journal comparison?

### 5.6 Implementation notes P9–P15 (decisions made while building)

**Files** — `app/market_structure.py` (fractal swings N=2, Fibonacci, ATR %, 1-min premium bars),
`app/adaptive_tsl.py` (pure engine + chain state machine), `run_adaptive_tsl.py` (runner),
`tests/test_market_structure.py`, `tests/test_adaptive_tsl.py`, `tests/test_tsl_integration.py`.

**Deviations from the plan above**
* Gamma component = Δ shift per 1 % spot move, not "Γ ÷ chain max": the engine sees one contract,
  not the SIE chain. IV component = the contract's IV change (`iv_pct`), not a session median
  (no IV history is stored). T4 table updated.
* "Bid-ask bearish/bullish" = the contract's **quantity imbalance** signal. `md:bidask:latest`'s
  `signal` is spread-based (NORMAL / CAUTION / EXIT_TERRITORY), not directional.
* Exit-confirmation EMA check uses the per-bar `ema9 − ema26` gap history the engine records
  itself (`md:ema:cross:latest` holds only the last bar).
* Unknown volatility (all components missing) picks the middle of the range.

**Redis keys** — `md:tsl` (events: TRACK / ACTIVATED / STOP_MOVED / BREACH / EXIT / REENTER /
CLOSED / CHAIN_*, plus a 60 s heartbeat), `md:tsl:latest:{TSYM}`, `md:tsl:state:{trade_id}`,
`md:tsl:chain:{chain_id}`, `md:tsl:origin:{chain_id}`, `md:tsl:reentry`,
`md:tsl:block:{SYM}:{SIDE}`, sets `md:tsl:trades` / `md:tsl:virtual` / `md:tsl:chains`.
New producer keys: ICARE writes `md:icare:origin:{TSYM}` (the probability input, APPROVED only,
written before `md:icare` so the origin always exists); the journal writes
`md:journal:closed:{trade_id}`.

**Integration**
* Re-entry probability = `run_probability.build_inputs` on the original input with live votes
  (`live_sie_fields`, P11 — no behaviour change for normal entries); a hard-filter rejection
  fails the check. ICARE gets a probability-shaped message: premium = live mid, projected
  gain / adverse change scaled by the premium ratio, `max_lots` = original lots → 6th MIN() limit
  `reentry_cap`.
* A re-entry trades only if **both** the engine message and ICARE's env say `TSL_MODE=active`;
  otherwise ICARE rejects with `tsl_shadow_mode`.
* Journal (active mode): closes on `TRAILING_STOP` and ignores the fixed target **only while
  the engine's state is fresh** (`JOURNAL_TSL_MAX_AGE_SEC=60`); if the engine dies, the fixed
  SL / target / time exits apply again. ICARE SL always wins over a TSL exit.
* `tsl_*` fields (`tsl_status`, `tsl_would_exit_px`, `tsl_shadow_pnl`, `tsl_rule`, …) are written
  on every journal record in both modes; in active mode `tsl_shadow_pnl` is simply the engine's
  own exit price × qty.
* Shadow mode simulates re-entries as **virtual trades** (entry at ask, original stop distance and
  hold), so whole chains can be evaluated without trading. Blocks are not set in shadow mode.
* Race found in the end-to-end run: the 30 s pending window could expire just before ICARE +
  journal opened the re-entry, leaving the trade unlinked. A re-entry position is now linked when
  the chain is PENDING **or** WAITING.
* Wiring: `run_all.sh/.ps1`, layers archiver (`md:tsl`, `md:tsl:reentry`), `pipeline_health.py`
  (Module 18 now implemented), Streamlit "Strike · Probability · ICARE" tab (trailing-stop table),
  client Excel row, `learn_weights.py` sections "fixed vs adaptive trailing stop (by TSL rule)" and
  "re-entries", RUN.md, spec index.

**Verified** — 170 unit tests (53 new). End-to-end on Redis DB 15 (flushed afterwards) with a
simulated clock, through the real ICARE / journal / engine functions:
* active: entry 100.2 → trail activates at 110 → stop 118.53 at high 125 (5.18 %, STRONG_LOW_VOL)
  → 118 touch with strong signals held → confirmed exit at 117.5 → journal `TRAILING_STOP`
  +₹1,720 → watch 125.2, cooldown → 123/124.5 no trade → 126 close → REENTER (prob 92) →
  ICARE APPROVED 1 lot (`reentry_cap` 2) → re-entry hit SL −₹835 → chain DONE (a non-TSL exit
  ends the chain).
* shadow: same path; real position untouched, later closed on the fixed SL at −₹1,320 with
  `tsl_shadow_pnl` +₹1,730 on the record; virtual re-entry simulated; ICARE rejected the
  re-entry (`tsl_shadow_mode`).

**Before switching to `TSL_MODE=active`:** run a few sessions in shadow, then compare
`fixed_total_pnl` vs `tsl_total_pnl` per rule in `python learn_weights.py`. The 4–6 % rows are the
ones most likely to need widening (§5.4).

---

## 6. Trade Ranking Engine (Module 13) — IMPLEMENTED (shadow)

Source: design brief "14. Trade Ranking Engine". Role: the **final decision
filter before Risk / Capital** (ICARE). Probability answers *"how likely is
this to work?"*; Trade Ranking answers *"among everything available right now,
is this worth money, is it the best, and what SL/TSL does it imply?"*. It
outputs TAKE_TRADE / WATCH / REJECT (or NO_TRADE for a whole cycle) and never
forces a trade.

### 6.1 What exists today (relevant to this engine)

| Brief input (0–100) | Existing source | Native scale | Gap |
|---|---|---|---|
| Probability | `md:probability` (`probability`, `decision`, `p_*`) | 0–100 weighted score (D7, not calibrated) | — |
| Indicator signals | `md:indicator:score:latest:{SYM}` (`run_momentum_confirm.py`) | −2..+2 | side-align + normalise |
| Volume analyzer | `md:volume:latest` (`signal`), entry `volume_surge` | label (Strong Bullish…Strong Bearish) | map to −2..+2 like P3 `intensity` |
| Market regime | `md:regime:latest` (breadth) + SIE `market_phase` (D6) | label | side-align |
| Bid-ask | SIE `execution_quality` (spread + depth) + `md:imbalance:latest:{TSYM}` | 0–100 + label | blend |
| OI | `oi_score` (Probability `p_oi`) from `md:oi:underlying:latest:{SYM}` | 0–100 | — |
| Greeks | SIE `greeks_score` (Δ / Θ / Γ / ν sub-scores) | 0–100 | — |
| Liquidity | SIE `liquidity_score` (`md:liquidity:score:latest:{TSYM}`) | 0–100 | — |
| Expected move | `md:expected_move:latest:{SYM}` (`direction`, `direction_score`, `confidence`) + SIE EM fit | mixed | combine |
| Greeks change | `md:greeks_change:latest:{SYM}` — **legacy contract only** (D5 note) | — | use SIE per-strike projection; Module 9 only when its contract = chosen `TSYM` |
| Strike selection | SIE `strike_score`, `confidence` | 0–100 | — |
| Lot sizing / risk capacity | ICARE pure helpers (`stop_loss_points`, lot limits) | lots | **no standalone pre-sizing** — call ICARE helpers without portfolio state |
| TSL quality | `app/adaptive_tsl.py` (`volatility_score`, `trend_strength`, `select_tsl_pct`) | % | **snapshot builder lives in `run_adaptive_tsl.py`** — refactor (P17) |
| Expected value | `icare.expected_value()` | ₹ | reuse |
| Kill switch | — (`pipeline_health.py`: "Module 16 … NOT IMPLEMENTED") | — | **Missing** → new manual key (R13) |
| Circuit condition | — | — | **Missing** — no circuit limits in our ticks/ScripMaster |
| Trading profiles (`normal_intraday` 0.45, `conservative` 0.58) | — **not in this repo** (the brief quotes another codebase) | — | **Missing** → new profile table (R8) |
| Cross-candidate ranking | `md:probability:rank` zset (symbol → probability, written by P3) | — | Sorts by probability only — the brief's "don't do this"; **replaced** |

Today: `SIE → Probability → ICARE → journal → TSL`. Each message is decided
alone; nothing compares candidates with each other.

### 6.2 Decisions

#### R1 — Same pattern as D1, but split into a small package (brief §26)
`app/trade_ranking/` (pure, no I/O) + `run_trade_ranking.py` + `tests/test_trade_ranking*.py`.
The brief's 12 files are merged where they would be a few lines each:

| Brief file | Our module |
|---|---|
| `candidate_validator.py` | `validator.py` (presence + freshness, R6) |
| `score_normalizer.py` | `normalizer.py` (13 component maps, R5) |
| `direction_analyzer.py` + `conflict_penalty.py` | `direction.py` (R7) |
| `probability_adapter.py` | `profiles.py` (profile table + probability gate, R8) |
| `expected_value.py` + `risk_reward_analyzer.py` | `economics.py` (EV, reward/risk, lot feasibility; calls `icare` helpers, R9/R10) |
| `sl_tsl_analyzer.py` | `sl_tsl.py` (calls `adaptive_tsl`, R11) |
| `ranking_calculator.py` | `score.py` (weights, penalties, bands, R4/R12) |
| `portfolio_filter.py` + `rank_sorter.py` | `portfolio.py` (hard gates, sort, correlation filter, R13/R14) |
| `trade_decision.py` | `engine.py` — `TradeRankingEngine.evaluate(candidate)` / `.rank(candidates)` |

Every weight / threshold is a named constant with an env override (`RANK_*`).
**The engine never re-implements** strike selection (SIE), SL (ICARE) or TSL (Module 18):
it calls their pure functions and *scores* what they propose (brief §11, §14, §27).

#### R2 — Pipeline position

```
md:strike:intel ─► run_probability.py ─► md:probability
                                              │
                                     run_trade_ranking.py   (Module 13)
                                              │  candidate book → rank cycle
                                              ▼
                     md:ranking (stream) · md:ranking:latest:{SYM}:{SIDE}
                     md:ranking:rank (zset) · md:ranking:cycle (summary)
                                              │ TAKE_TRADE only, in rank order
                                              ▼
                                         run_icare.py  (Risk + Capital + final lots)
                                              ▼
                     run_trade_journal.py ─► run_adaptive_tsl.py (SL / TSL manage)
```

The brief's "Lot Sizing → Ranking → Risk → SL/TSL → Capital" maps onto our
code as: ranking does a **pre-sizing feasibility** check (can ≥ 1 lot fit the
risk budget?) and a **proposed** SL/TSL; ICARE stays the single authority for
final lots, capital and portfolio guards; Module 18 manages the position after
entry (brief §27). Slippage check (Module 19) stays out of scope.

#### R3 — Ranking needs a candidate book (signals arrive one by one)
* Every `md:probability` message whose decision is not a hard reject enters
  `md:ranking:book` (hash, field `SYM:SIDE`, latest wins). Entries expire after
  `RANK_CANDIDATE_TTL_SEC=120` or when the source data goes stale.
* A **rank cycle** runs on every new candidate and every `RANK_CYCLE_SEC=5` s:
  re-validate freshness → score all live candidates → sort → filter → publish.
* A new candidate waits up to `RANK_BATCH_WINDOW_SEC=3` s before it can be
  emitted, so near-simultaneous signals are compared instead of first-come-first-served.
* A candidate is emitted as TAKE_TRADE **once** (`candidate_id` = SIE entry ts + TSYM);
  re-scoring keeps it in the zset / latest key for the dashboard.

#### R4 — Weights (brief §4, as given; configurable)

| Component | Weight | Normalised from |
|---|---|---|
| Probability | 20 | `probability` |
| Indicator | 8 | indicator score −2..+2, side-aligned → 0..100 |
| Volume | 8 | volume signal −2..+2 side-aligned → 0..100, +10 on surge (cap 100) |
| Market regime | 10 | breadth aligned 100 / NEUTRAL 50 / against 0, averaged with phase fit (STRONG_TREND 100, NORMAL 70, HIGH_VOL 50, EXPIRY 50) |
| Bid-ask | 8 | 70 % SIE execution quality + 30 % imbalance (aligned 100 / neutral 50 / against 0) |
| OI | 8 | `p_oi` |
| Greeks | 6 | SIE `greeks_score` |
| Liquidity | 8 | SIE `liquidity_score` |
| Expected move | 7 | 50 % Module 8 confidence (0 when direction opposes) + 50 % SIE EM fit |
| Greeks change | 5 | SIE projection: `expected_premium_gain` % of premium (0 at 0 %, 100 at ≥ 30 %), × 0.5 if the IV −2 stress gain ≤ 0 |
| Strike selection | 5 | SIE `strike_score` |
| Lot / risk capacity | 4 | R10 |
| TSL quality | 3 | R11 |
| **Total** | **100** | weights validated to sum 100 at startup |

**Known overlap:** Probability already contains liquidity, OI, greeks and
confluence (D7), so those effectively weigh more than the table shows. Kept as
the brief specifies; the offline report (R16) measures each component's
lift so this can be corrected from evidence, not guesswork (R-Q1).

#### R5 — Normalisation (brief §5)
All components → 0..100. Signed inputs: `score = (x − min) / (max − min) × 100`
(for −100..+100 this is the brief's `(x + 100) / 200 × 100`), then
**side-aligned**: for BUY PUT the signed value is negated first, so "100" always
means "supports this trade".

#### R6 — Data validation: missing ≠ 0 (brief §6, §23)
* **Critical:** probability, liquidity, bid-ask (spread), premium / strike, greeks.
  Missing or stale → status **DATA_INSUFFICIENT** (not REJECT, not scored).
* **Non-critical** missing / stale → component excluded, weights renormalised
  (same rule as SIE D5), and a **data-quality factor**
  `dq = 1 − 0.5 × missing_weight_share` (e.g. 15 % of weight missing → ×0.925).
* Freshness: `RANK_MAX_AGE_SEC=120` for analytics, `RANK_MAX_QUOTE_AGE_SEC=30` for bid-ask / liquidity.
* Deliberate difference from Probability (D7) and ICARE (P5), which count
  missing as 0: the brief requires ranking not to punish absent data — the dq
  factor and DATA_INSUFFICIENT status cover it instead.

#### R7 — Directional agreement & conflict penalty (brief §7, §8)
Votes (each FOR / AGAINST / NEUTRAL, side-aligned): indicator, volume, regime
breadth, bid-ask imbalance, OI positioning, greeks phase (MARKUP/ACCUMULATION
= for, DISTRIBUTION = against), Module 8 direction, HTF bias, Supertrend bias.
`agreement = FOR / (FOR + AGAINST)` (neutral ignored).
* `conflict_factor = clamp(agreement / RANK_AGREEMENT_FULL, 0.5, 1.0)`, `RANK_AGREEMENT_FULL=0.8`
  → 80 %+ agreement is unpenalised; 60 % → ×0.75.
* `agreement < RANK_MIN_AGREEMENT (0.5)` → hard REJECT `DIRECTION_CONFLICT`
  (brief example: 2 for / 3 against = 40 % → reject).
* Fewer than 3 decisive votes → factor 0.9, flag `LOW_DIRECTIONAL_EVIDENCE`.

#### R8 — Probability: ranking input **and** gate (brief §9)
* Contributes 20 % (R4).
* Gate: `probability < profile.min_probability` → REJECT `PROBABILITY_BELOW_PROFILE`.
* New `TRADING_PROFILE` env (`app/trade_ranking/profiles.py`), because the
  brief's profiles do not exist here. Our probability is a **score** where < 50
  is already REJECT and < 65 never trades (ICARE `ALLOWED_PROB_DECISIONS`), so
  the brief's 0.45 / 0.58 cannot be used as-is. Proposed (R-Q2):

  | Profile | min probability | min trade score | min EV / risk | allow CONDITIONAL |
  |---|---|---|---|---|
  | `conservative` | 75 | 75 | 0.30 | no |
  | `normal_intraday` (default) | 65 | 70 | 0.15 | no |
  | `aggressive` | 65 | 60 | 0.05 | yes (class C cap) |

#### R9 — Expected value & reward/risk (brief §10, §13)
Reuse `icare.expected_value()` with the same model/journal switch as D9:
`p = probability / 100` (journal win rate once the bucket has ≥ 30 trades),
`profit = SIE expected_premium_gain × qty`, `loss = initial SL points × qty`.
* `ev_per_risk = EV / risk_amount` → comparable across trade sizes.
* `reward_risk = expected_gain_pct / initial_sl_pct`.
* **Risk factor** (brief §16 "risk penalty"): `clamp(0.7 + 0.15 × reward_risk, 0.7, 1.0)`
  — full at RR ≥ 2. This is what makes "87 % / +35 %" beat "92 % / +8 %" (brief §1).
* Gate: `EV ≤ 0` or `ev_per_risk < profile.min_ev_r` → REJECT `NEGATIVE_EV` / `LOW_EV`.

#### R10 — Lot feasibility (brief §12)
Call ICARE's lot limits **without** portfolio state: margin, risk
(`min(₹7,500, 2 % capital)` ÷ SL risk per lot), capital class, liquidity safe lots.
* `feasible_lots = 0` → hard REJECT `LOT_NOT_FEASIBLE` (brief: reduce or reject — ICARE already reduces).
* `lot_score = 60 + 40 × min(1, feasible_lots / RANK_TARGET_LOTS (2))`.
* Outputs `capital_required`, `risk_amount` (indicative; ICARE decides the final values).

#### R11 — Proposed SL / TSL (brief §13, §14, §27)
* Initial SL = `icare.stop_loss_points()` (SIE adverse repricing, clamped 10–30 %).
* TSL = `adaptive_tsl.select_tsl_pct()` on a snapshot built now (volatility, trend, DTE);
  activation = `tsl_pct` (T7). Requires refactor P17 (`build_snapshot()` out of the runner).
* `tsl_quality = expected_gain_pct / max(initial_sl_pct, tsl_pct)` → 0 at ≤ 0.5, 100 at ≥ 3.
* These are **proposals** carried in the payload; ICARE sets the real SL and
  Module 18 manages it after entry. TSL never gates entry by itself (brief §27);
  it only adjusts the score (3 %).

#### R12 — Final score & decision bands (brief §16, §17)
```
weighted    = Σ wᵢ · componentᵢ   (over present components, renormalised)
trade_score = weighted × conflict_factor × risk_factor × dq
```

| trade_score | decision | confidence |
|---|---|---|
| < 50 | REJECT | — |
| 50–59 | WATCH | — |
| 60–69 | WATCH (CONDITIONAL); TAKE only if profile allows, capital capped at class C | CONDITIONAL |
| 70–79 | TAKE_TRADE | TAKE |
| 80–89 | TAKE_TRADE | HIGH_CONVICTION |
| ≥ 90 | TAKE_TRADE | EXCEPTIONAL |

TAKE_TRADE also needs `trade_score ≥ profile.min_trade_score`. Bands are design
values; calibrated later from journal outcomes (R16), never auto-tuned live (D11).

#### R13 — Hard gates (brief §18) — score can never override these

| Gate | Check | Source |
|---|---|---|
| Probability | < profile minimum | R8 |
| Liquidity | < `SIE_MIN_LIQUIDITY` (70) or band RED | SIE |
| Spread | > `MAX_SPREAD_PCT` (3 %) | live `md:bidask:latest:{TSYM}` |
| Lot fit | feasible lots = 0 | R10 |
| Kill switch | `md:control:kill_switch` = `1` | **new** key (manual: `redis-cli set` / Streamlit button) |
| Duplicate | open position on the same underlying (`md:position:open:*`) | journal |
| Re-entry block | `md:tsl:block:{SYM}:{SIDE}` | Module 18 (T11) |
| Daily loss | day PnL ≤ −`ICARE_DAILY_LOSS_LIMIT_PCT` | `md:account:latest` |
| Exposure | open trades ≥ `ICARE_MAX_OPEN_TRADES` or portfolio risk ≥ limit | same env as ICARE |
| Stale data | critical input stale | R6 → DATA_INSUFFICIENT |
| Circuit | price within `RANK_CIRCUIT_BAND_PCT` (1 %) of circuit limit | **no source today** → flag `CIRCUIT_UNCHECKED`, gate inactive (R-Q6) |
| Direction conflict | agreement < 50 % | R7 |
| EV | EV ≤ 0 / below profile | R9 |

Portfolio gates use ICARE's env values (one source of truth); ICARE re-checks
them anyway (defence in depth).

#### R14 — Rank, de-correlate, never force (brief §19–§21)
1. Sort eligible candidates by `trade_score`, ties by `ev_per_risk`, then `probability`.
2. Correlation filter, best first: one side per underlying (CE vs PE on the same
   symbol → keep the higher), max `RANK_MAX_PER_SECTOR=1` new trade per sector
   per cycle including open positions (`sectors.json`; unknown sector → flag, not blocked).
3. Cap at free slots: `ICARE_MAX_OPEN_TRADES − open positions`.
4. Survivors → `md:ranking` as TAKE_TRADE **in rank order** (ICARE processes the
   stream sequentially, so rank 1 gets capital first). Losers of step 2/3 →
   WATCH with reason `OUTRANKED` / `CORRELATED` / `NO_SLOT`.
5. Zero survivors is a valid result: the cycle summary says **NO_TRADE** with
   counts (`scanned, rejected, insufficient, watch, eligible, taken`).

No "top N always buy" logic anywhere (brief §20, §23).

#### R15 — Output (brief §22) + hand-off
`md:ranking` / `md:ranking:latest:{SYM}:{SIDE}` carry the brief's object:
`symbol, option, direction, trade_score, rank, probability, <13 component scores>,
expected_gain_pct, expected_value, ev_per_risk, reward_risk, initial_stop_loss_pct,
trailing_stop_pct, trailing_activation_pct, decision, confidence, capital_required,
risk_amount, reasons[], warnings[], timestamp` + diagnostics
(`agreement, votes{}, conflict_factor, risk_factor, dq, missing[], gates{}, profile, cycle_id, candidate_id, mode`).
The **full probability payload is forwarded**, so `run_icare.build_inputs()`
works on a ranking message unchanged. Reasons/warnings are generated from
component thresholds (≥ 80 → reason, < 40 → warning).

`md:ranking:rank` (zset `SYM:SIDE → trade_score`) replaces `md:probability:rank`
as the Module 13 view; P3 keeps writing the old zset until switch-over.

#### R16 — Shadow first (same principle as D3 / T12)
`RANK_MODE=shadow|active`, default **shadow**:
* shadow: ranking publishes everything; ICARE keeps consuming `md:probability`
  as today. Each journal record gets the ranking fields that existed for that
  candidate (`rank_score`, `rank_decision`, `rank_confidence`) so we can see
  whether trades the ranker would have rejected lost money.
* active: ICARE consumes `md:ranking` (TAKE_TRADE only) instead of `md:probability`
  (`ICARE_INPUT_STREAM`); CONDITIONAL trades are capped at class C like SMALL_POSITION.
* `learn_weights.py` new section "trade ranking": win rate / PnL by decision band
  and confidence, per-component lift, EV calibration (model EV vs realised PnL),
  and "what ranking would have rejected". Weights stay human-approved (D11).

### 6.3 Implementation plan

| Phase | Deliverable | Files |
|---|---|---|
| **P16** ✅ | Pure engine: normaliser, validator (missing ≠ 0), direction / conflict, profiles + probability gate, EV / RR / lot feasibility via ICARE helpers, score + penalties + bands, hard gates, sort + correlation filter + NO_TRADE. Tests for every brief example (87 %/35 % beats 92 %/8 %; 8 for / 2 against = 80 %; 2 / 3 → reject; EV ₹3,100 example; missing OI ≠ 0; no forced trade when all < 70; CE+PE same symbol → one) | `app/trade_ranking/*.py`, `tests/test_trade_ranking.py` |
| **P17** ✅ | Refactor: `build_snapshot(r, pos)` out of `run_adaptive_tsl.py` so ranking can get a TSL proposal for a contract not yet held; expose ICARE lot limits as a function usable without `PortfolioState` (no behaviour change; existing 170 tests stay green) | `run_adaptive_tsl.py`, `app/icare.py` |
| **P18** ✅ | Runner: consume `md:probability`, candidate book, rank cycle + batch window, gather the 13 inputs from latest keys, publish stream / latest / zset / cycle summary, kill-switch key | `run_trade_ranking.py` |
| **P19** ✅ | Integration (only active mode changes behaviour): `ICARE_INPUT_STREAM`, CONDITIONAL cap; journal stores `rank_*` fields | `run_icare.py`, `app/icare.py`, `run_trade_journal.py`, their tests |
| **P20** ✅ | Wiring: `run_all.sh/.ps1`, `stop_all.ps1`, layers archiver (`md:ranking`, `md:ranking:cycle`), `pipeline_health.py` (Module 13 → `md:ranking:rank`; Module 16 kill switch partial), Streamlit ranking table + kill-switch toggle, client Excel rows, RUN.md, spec index | existing files |
| **P21** ✅ | Offline report: `learn_weights.py` "trade ranking" section | `learn_weights.py` |
| **P22** ◐ | Verification: unit tests + end-to-end on Redis DB 15 with 3 simultaneous scripted candidates (one TAKE, one CORRELATED, one REJECT) and one all-reject cycle → NO_TRADE; replay 2026-09-21 archive in shadow mode | — |

P16 is testable with no Redis; P18 gives shadow output; P19 is the only step
that changes existing behaviour, and only with `RANK_MODE=active`.

### 6.4 Risks / notes
* With today's data (liquidity median ~48, Module 8 confidence often low — §3b)
  most candidates will be REJECT / DATA_INSUFFICIENT. Expected; review
  `md:ranking:cycle` reasons before loosening anything.
* With 2 symbols in `symbols.txt`, cross-candidate ranking rarely has more than
  one candidate; its value appears when the universe grows (D4 scaling note).
* The batch window adds up to 3 s entry latency.
* Score weights and bands are design values, not fitted (same caveat as D7 / §5.4).

### 6.5 Open questions (answered 2026-10-04: all defaults accepted)

* **R-Q1** Weights: use the brief's table **as-is** despite overlap with the Probability score, or drop the overlapping components (liquidity / OI / greeks) to avoid double counting?
* **R-Q2** Profiles & probability gate: the R8 table with **`normal_intraday` (65)** as default?
* **R-Q3** CONDITIONAL (60–69): **WATCH only**, or tradable with a 3 % capital cap?
* **R-Q4** Correlation: **one new trade per sector per cycle** and one side per underlying?
* **R-Q5** Batch window **3 s** — acceptable entry delay?
* **R-Q6** Circuit gate: **flag-only** until a circuit-limit source exists, or fetch limits from the Angel quote API (extra REST calls)?
* **R-Q7** Kill switch: a **manual Redis key + Streamlit toggle** (partial Module 16) — OK?
* **R-Q8** Start in **shadow** and switch ICARE to `md:ranking` after N sessions of journal comparison?

### 6.6 Implementation notes P16–P22 (decisions made while building)

**Files** — `app/trade_ranking/` (`config.py`, `candidate.py`, `normalizer.py`, `validator.py`,
`direction.py`, `economics.py`, `sl_tsl.py`, `score.py`, `portfolio.py`, `engine.py`),
`run_trade_ranking.py`, `tests/test_trade_ranking.py`, `tests/test_trade_ranking_runner.py`.

**Deviations from the plan above**
* Package layout: profiles live in `config.py` with the weights (no `profiles.py`); `candidate.py`
  holds the data model (`Candidate`, `Context`, `RankResult`).
* P17 needed **no snapshot refactor**: `run_adaptive_tsl.Inputs` / `build_snapshot()` were already
  reusable, so the ranking runner imports them (same pattern as Module 18 importing `run_probability`).
  `sl_tsl.propose_tsl()` runs Module 18's volatility / trend / `select_tsl_pct` on that snapshot.
  ICARE got three extracted helpers — `margin_per_lot()`, `max_risk_allowed()`, `presize_lots()` —
  used by `evaluate()` too (no behaviour change; all 170 earlier tests unchanged and green).
* Pre-sizing lots = MIN(margin, risk, max lots per trade, Module 7 liquidity). The capital-class
  limit is left out (it needs ICARE's trade-quality class), so `feasible_lots`, `capital_required`
  and `risk_amount` are **indicative**; ICARE's numbers are final.
* Greeks change always uses the SIE projection of the chosen strike; Module 9's output is not
  read (it runs on the legacy `strike_select` contract, D5 note).
* Direction votes (9): indicator score sign, volume signal, regime breadth, contract bid-ask
  imbalance, underlying OI positioning, contract greeks phase (MARKUP/ACCUMULATION for,
  DISTRIBUTION against — not side-flipped, we only buy), Module 8 direction, HTF bias,
  Supertrend bias. With < 3 decisive votes a conflict is still a reject when ≥ 2 decisive votes
  are mostly against.
* Live values win over the SIE snapshot for the gates: liquidity score / band, spread and premium
  (mid) come from the latest keys; a bid-ask or liquidity key older than 30 s is **stale →
  DATA_INSUFFICIENT**. Probability messages with a Probability hard-filter rejection
  (sideways, RED band, …) never enter the book.
* `expected_value` in the output = EV per lot × feasible lots (≥ 1); `ev_per_risk` uses per-lot values.
* **Payload naming:** the ranking message is the probability payload + `rank_*` fields
  (`rank_decision`, `rank_confidence`, `rank_reject_reasons`, `rank_components`, …) with
  `trade_score` and `rank` unprefixed, plus `ranking_json` = the brief's §22 object. Prefixing
  avoids overwriting the probability `decision` that ICARE's `build_inputs()` reads.
* Stream volume: `md:ranking` gets a message on a candidate's first evaluation, on every decision
  change and on the TAKE_TRADE emission (`rank_emit=1`), not every 5 s cycle; `md:ranking:cycle`
  on change + 60 s heartbeat.
* Weights `RANK_W_<COMPONENT>` are checked at startup and the runner exits if they do not sum to 100.

**Integration**
* `run_icare.py`: `RANK_MODE=active` switches the input stream to `md:ranking` and accepts only
  `rank_emit=1` + `TAKE_TRADE`; `rank_confidence=CONDITIONAL` caps the allocation at 3 %
  (`CONDITIONAL_CAP`, only reachable with the `aggressive` profile); rank fields are forwarded.
* Journal: every record carries `trade_score, rank, rank_decision, rank_confidence, rank_mode,
  rank_components`. In shadow mode they are looked up from `md:ranking:latest:{SYM}:{SIDE}` when
  the contract matches, so `learn_weights.py` can show what the ranker would have done.
* `learn_weights.py`: sections by ranking decision / confidence / score band, per-component lift
  (≥ 70 vs < 70) and EV calibration (EV per lot × lots vs realised PnL).
* Wiring: `run_all.sh/.ps1` (`stop_all.ps1` is pid-file based, no change), layers archiver
  (`md:ranking`, `md:ranking:cycle`), `pipeline_health.py` (Module 13 → `md:ranking:rank`;
  Module 16 PARTIAL), Streamlit decision tab (ranking table, cycle summary, **kill-switch toggle**),
  client Excel row, RUN.md, spec index.

**Verified** — 207 unit tests (37 new). End-to-end through the real runner / ICARE / journal
functions on an **in-memory Redis** (the `md_redis` container was stopped and was not restarted):
3 simultaneous candidates → TCS CE 88.3 HIGH_CONVICTION emitted once (after the 3 s window, not
re-emitted next cycle), INFY CE 86.7 WATCH `CORRELATED_SECTOR`, RELIANCE PE REJECT
`SPREAD_ABOVE_MAX`; ICARE APPROVED the emission with the probability decision intact; journal
attached the ranking verdict; kill switch → NO_TRADE; stale quote → DATA_INSUFFICIENT; book expiry.
**Not yet done:** a run on real Redis (DB 15) and the 2026-09-21 archive replay in shadow mode.

**Known limitation:** an emitted candidate keeps its sector / slot in the ranking until it leaves
the book (120 s), even if ICARE rejects it — a same-sector runner-up waits up to 2 minutes.

**Before switching to `RANK_MODE=active`:** run a few sessions in shadow, then check
`python learn_weights.py` → "by ranking decision" (did WATCH / REJECT trades lose money?) and
"component lift" (R-Q1 overlap).

---

## 7. Order Executor / Trade Entry Engine (Module 14) — IMPLEMENTED (shadow; live disabled)

Source: design brief "16. Trade Execute". As with T-Q1, the "16" is the brief's
section number. The work is filed as **Module 14 — Order Executor / Trade Entry**, its number
in the spec index (`Option-rider-algo-specs.md`; Module 15 there is the Exit Order Module, which
E17 covers later). *(Corrected 2026-10-04: this section first said Module 15, copied from
`pipeline_health.py`, whose label was off by one. The label is now fixed.)* Module 16 is the
Circuit Breaker / Kill Switch, already PARTIAL.

Role: execute an **approved** trade at the lowest practical cost. Ranking decides
**WHAT**, ICARE decides **HOW MUCH** (risk / capital / portfolio), the executor
decides **HOW**. It never re-scores the strategy, never raises the quantity, and
never chases the price past its slippage budget.

### 7.1 What exists today (relevant to this engine)

| Brief need | Existing source | Gap |
|---|---|---|
| Approved command | `md:icare` APPROVED (`recommended_lots`, `lot_size`, `premium`, `sl_premium`, `target_premium`, `expected_value`, rank fields, full probability payload) | No `trade_id`, `max_slippage_pct` or timeout fields yet. The executor derives them (E3) |
| Latest bid / ask / spread | `md:bidask:latest:{TSYM}` (`bid`, `ask`, spread, signal) | Freshness bound for execution must be tighter than ranking's 30 s |
| Order-book depth | Tick fields `bid_depth5`, `ask_depth5`, `bid_depth5_px`, `ask_depth5_px` (read by `run_bidask_imbalance.py`) | Not cached as a per-contract latest key. Add one, or read from the bidask latest (P24) |
| Available margin | `md:account:latest` (`app/broker_account.py`, paper ledger, or live `rmsLimit()`) | The live value is up to 30 s old. Live mode re-reads it before each order (E7) |
| Margin per lot / risk lots | `icare.margin_per_lot()`, `max_risk_allowed()`, `presize_lots()` | — |
| Kill switch | `md:control:kill_switch` (manual key + Streamlit toggle, R13) | Not yet checked after approval |
| Portfolio exposure | ICARE guards + `md:position:open:*` | Must be re-checked at execution time (race with other approvals) |
| Fill / position | `run_trade_journal.py` opens a paper position at the **ask on approval** | No order, no partial fill, no slippage, no charges |
| Broker orders | `app/angel_auth.py` (SmartConnect session), `app/angel_rest.py` (greeks / candles only) | **Missing**: placeOrder / modifyOrder / cancelOrder / order book |
| Brokerage / charges | `icare.margin_buffer_pct` (1 % headroom only) | **Missing**: charges table |
| Slippage estimator (Module 19) | — | **Missing**. This engine produces the data it will need (E15) |

### 7.2 Decisions

#### E1 — Same pattern as R1: a small pure package + runner
`app/order_executor/` (pure, no I/O) + `run_order_executor.py` + `tests/test_order_executor*.py`.
The brief's 14 files are merged where they would be only a few lines each:

| Brief file(s) | Our module |
|---|---|
| `command_validator.py` | `command.py` (`ExecCommand` built from the ICARE payload, pre-trade validation, E3/E4) |
| `bid_ask_reader.py` + `spread_analyzer.py` | `market.py` (quote snapshot, spread %, depth within the price cap, E5) |
| `slippage_controller.py` + `price_execution_engine.py` | `pricing.py` (reference, cap, limit-price ladder, tick rounding, E6) |
| `margin_manager.py` + `quantity_manager.py` + `order_slicer.py` | `quantity.py` (executable lots MIN(), slice size, E7/E8) |
| `brokerage_calculator.py` | `charges.py` (per-order + statutory charges from config, E10) |
| `partial_fill_manager.py` | `fills.py` (fill aggregation, average price, continue / stop decision, E9) |
| `execution_state.py` | `state.py` (state machine, E12) |
| `order_reconciliation.py` | `broker.py` (`Broker` interface: `PaperBroker`, `AngelBroker`, reconcile, E14) |
| `execution_logger.py` + `executor_controller.py` | `engine.py` (`OrderExecutor.step()`, a pure transition on (state, quote, broker events, now)) + the runner |

`engine.step()` is pure. The runner owns the clock, Redis and broker calls, so the
whole algorithm (ladder, slicing, timeout, partial fill) can be tested tick by tick with no
broker. Every threshold is a named constant with an env override (`EXEC_*`).

#### E2 — Pipeline position

```
md:ranking (TAKE_TRADE) ─► run_icare.py ─► md:icare (APPROVED)
                                               │
                                     run_order_executor.py   (Module 14)
                                               │  md:exec (stream: every order / fill / cancel / final report)
                                               │  md:exec:latest:{trade_id}, md:exec:state:{trade_id}
                                               ▼
                                     md:exec:fill (final report: FILLED / PARTIAL_* only)
                                               │
                                     run_trade_journal.py  (opens the position at the ACTUAL avg fill & qty)
                                               ▼
                                     run_adaptive_tsl.py   (Module 18 manages it)
```

* The brief's "Capital → Risk → Portfolio check" is ICARE today (Module 17 Risk Management
  is not a separate worker). The executor re-checks only what can change in the seconds
  after approval (E4). It does not re-run ICARE.
* Single-writer rule (same as T2): the executor **never writes** `md:position:open:*`.
  The journal is still the only process that opens and closes positions. It now does so
  from `md:exec:fill` instead of `md:icare` (in `paper` / `live` mode, E16).
* Re-entries (T10) follow the same path: ICARE APPROVED → executor → journal.

#### E3 — Command = brief §2 object, built from the ICARE payload
| Command field | Source |
|---|---|
| `trade_id` | `TRD_{yyyymmdd}_{seq}`. Idempotent: `SET md:exec:lock:{icare msg id} NX`, so a redelivered message never orders twice |
| `symbol, option_symbol, strike, option_type, direction=BUY` | ICARE / SIE fields |
| `rank, trade_score, probability, expected_gain_pct` | forwarded rank / probability fields |
| `allocated_capital, max_risk_amount` | ICARE `capital_required`, `risk_amount` |
| `recommended_sl_pct, recommended_tsl_pct` | ICARE SL ÷ premium, ranking `trailing_stop_pct` (pass-through only, T14) |
| `requested_lots` | ICARE `recommended_lots` (the **upper bound**) |
| `signal_premium` | ICARE `premium` (price the approval was computed at) |
| `max_slippage_pct` | `EXEC_MAX_SLIPPAGE_PCT` = **0.50** |
| `execution_timeout_seconds` | `EXEC_TIMEOUT_SEC` = **10** |
| `ev_per_lot` | ICARE `expected_value` ÷ lots (used by E9) |

A command older than `EXEC_COMMAND_TTL_SEC=5` when picked up is rejected
`COMMAND_EXPIRED` and never executed late.

#### E4 — Fresh validation before any order (brief §3)
All checks must pass. Otherwise the result is `REJECTED_BEFORE_EXECUTION` with a reason code and **no order is sent**.

| Check | Rule | Reason code |
|---|---|---|
| Kill switch | `md:control:kill_switch` ≠ 1 | `KILL_SWITCH` |
| Market hours | `EXEC_NO_ENTRY_BEFORE` (**09:20**) ≤ now < 15:20 (journal's no-new-entry time). The first 5 minutes have the widest option spreads and price discovery is still settling | `MARKET_CLOSED` / `OPENING_WINDOW` |
| Stock-option expiry day | No new entries in **stock** options after `EXEC_EXPIRY_CUTOFF` (13:00) on expiry day. ITM stock options are **physically settled** and brokers force square-off / raise margin near expiry (§7.6) | `EXPIRY_CUTOFF` |
| Quote fresh | bid-ask key ≤ `EXEC_MAX_QUOTE_AGE_SEC` (**3 s**) old | `STALE_QUOTE` |
| Tradable | bid > 0 and ask > 0 and ask > bid | `NO_QUOTE` |
| Spread | `(ask − bid) / mid × 100 ≤ EXEC_MAX_SPREAD_PCT` | `SPREAD_TOO_WIDE` |
| Liquidity | ask depth inside the price cap ≥ 1 lot | `NO_DEPTH` |
| Price drift | `ask ≤ signal_premium × (1 + EXEC_MAX_DRIFT_PCT 2 %)`. SL, target and EV were computed at the signal price | `MARKET_CONDITION_CHANGED` |
| Margin | ≥ 1 lot affordable (E7) | `INSUFFICIENT_MARGIN` |
| Exposure | open + executing trades < `ICARE_MAX_OPEN_TRADES`; no open or executing position on the same underlying | `EXPOSURE_LIMIT` / `DUPLICATE_UNDERLYING` |
| Circuit | no data source (R13) → flag `CIRCUIT_UNCHECKED`, does not block | — |

"Executing" trades count against exposure (`md:exec:active` set), so two approvals arriving
together cannot both pass the check.

**Spread default.** The brief's example uses 0.50 %. With today's stock-option spreads
(Ranking gate `MAX_SPREAD_PCT` = 3 %), 0.50 % would reject almost everything.
Proposed `EXEC_MAX_SPREAD_PCT` = **1.0 %** while in shadow. Tune it from the shadow data (X-Q2).

#### E5 — Bid / ask snapshot (brief §4–§5)
* BUY reference = **ask**. SELL reference = **bid** (used later by exits, E17). The ICARE
  premium is never used as the execution price.
* Snapshot: bid, ask, bid qty, ask qty, spread ₹ / %, LTP, depth5 prices and sizes, quote age.
* `depth_within_cap` = Σ ask sizes at price levels ≤ cap (E6). This is the liquidity input to E7 / E8.

#### E6 — Controlled limit-order ladder, never a market order (brief §6–§7)
```
reference = ask at the first snapshot (fixed for the whole command)
cap       = floor_to_tick(reference × (1 + max_slippage_pct))      # 101 × 1.005 = 101.505 → 101.50
start     = ceil_to_tick(mid)                                      # bid 100 / ask 101 → 100.50
step      = max(tick, ceil_to_tick((cap − start) / EXEC_LADDER_STEPS(5)))
every EXEC_STEP_SEC (2 s) without a full fill: price = min(price + step, cap)
```
* Tick size comes from ScripMaster `tick_size`, **not a constant**. Since 3 Nov 2025 NSE uses
  **₹0.01** for options on stocks priced below ₹250 and ₹0.05 for the rest (§7.6).
  Rounding with the wrong tick gets the order rejected.
* **Tight-spread shortcut:** if `ask − bid ≤ 2 ticks`, there is nothing to improve, so the first
  limit goes **at the ask** (a marketable limit). Sitting at mid would only delay a momentum
  entry and risk adverse selection: a passive buy order fills mostly when the price is falling,
  which is exactly when the signal is failing.
* **Exchange Limit Price Protection (LPP):** NSE rejects limit orders outside a dynamic band
  around the 30-second average trade price. Our cap is within 0.5 % of the ask, so this should
  be rare. An LPP rejection is treated as `PRICE_OUT_OF_BAND`: re-quote once inside the band
  if that is still ≤ cap, otherwise abort.
* The **cap is anchored to the first reference and is never re-anchored**. If the ask moves
  above the cap, the open order is cancelled and the engine re-evaluates (brief §7). It waits
  for the ask to come back ≤ cap within the timeout, re-running E4 on each fresh quote. If the
  ask does not come back, the result is `ABORTED_SLIPPAGE_LIMIT`. It never follows the price up.
* `EXEC_ALLOW_MARKET=0` (fixed). Market orders are not used for entries.

#### E7 — Executable quantity (brief §9–§11, §23)
```
executable_lots = MIN(
    requested_lots,                                         # ICARE / lot sizing: never exceeded
    floor(available_margin / margin_per_lot(cap)),          # margin at the worst allowed price
    floor(max_risk_amount / (sl_points × lot_size)),        # risk budget at the actual price
    depth_lots_within_cap × EXEC_DEPTH_TAKE (1.0),          # liquidity (per slice, E8)
    EXEC_MAX_LOTS_PER_ORDER / exchange freeze qty           # broker / order limit
)
```
* The executor can **reduce** below the ICARE quantity but never increase it. Reducing is
  within ICARE's permission because ICARE's lots are a maximum and fewer lots means less risk.
  Below `EXEC_MIN_LOTS` (1) the trade is rejected `INSUFFICIENT_MARGIN` / `NO_DEPTH`.
  Rejecting instead of reducing is possible with `EXEC_ALLOW_REDUCE=0` (X-Q4).
* Margin source: `paper` uses the account ledger. `live` makes a fresh `rmsLimit()` call before
  the first order of a command (2 req/s limit), and uses `md:account:latest` for the later slices.
* The reason for any reduction (`REDUCED_MARGIN`, `REDUCED_RISK`, `REDUCED_DEPTH`) is in the report.

#### E8 — Liquidity-based slicing (brief §17)
* Slice = MIN(remaining lots, depth lots ≤ cap, max lots per order). After each slice: wait for
  the fill or the step timer, refresh the quote, then size the next slice.
  (10 lots, depth 3 → buy 3 → refresh, depth 2 → buy 2 → …)
* Only one working order per trade at a time. This keeps state simple and avoids
  over-filling when two slices fill together.
* The per-order maximum is the **NSE quantity freeze limit** for that stock (units, not lots;
  published per stock by NSE and revised from time to time; index limits changed again on
  5 Oct 2026). Angel's ScripMaster already carries it per contract (`freeze_qty`), so it is read
  from there daily (§7.7); lots per order = `floor((freeze_qty − 1) / lot_size)`. If it is missing, `EXEC_MAX_LOTS_PER_ORDER` (default 10)
  is used. Our sizes are far below the limits today, but an order above them is rejected outright.

#### E9 — Partial fills & the completion decision (brief §12–§15)
On every partial fill or slice end, with remaining lots > 0:
```
benefit  = ev_per_lot × remaining_lots × (signal_premium / current_ask)     # EV shrinks as entry worsens
cost_add = per_order_brokerage × orders_needed                              # flat ₹ per order (E10)
         + max(0, current_ask − avg_fill) × remaining_qty                   # extra slippage vs fills so far
continue  if benefit > cost_add × EXEC_COST_MARGIN (1.5) and time left and E4 still passes
otherwise cancel the remaining quantity → PARTIAL_FILL_STOPPED
```
* Statutory charges scale with turnover. They are paid whether the quantity comes in 1 or 3
  orders, so only **per-order** charges and the extra slippage count as *additional* cost.
* Residual guard: if the remaining value (`remaining_qty × ask`) is below
  `EXEC_MIN_RESIDUAL_VALUE` (₹2,000), the remaining quantity is not chased.
* Timeout (`execution_timeout_seconds`): cancel what is left. The result is
  `PARTIAL_FILL_TIMEOUT` (filled > 0) or `CANCELLED_TIMEOUT` (filled = 0).

#### E10 — Charges kept separate from slippage (brief §16)
`charges.json` (not hard-coded, brief §16), per segment:
`brokerage_per_order` (Angel F&O: flat ₹20), `stt_sell_pct`, `exchange_txn_pct`,
`sebi_per_crore`, `stamp_buy_pct`, `gst_pct` (on brokerage + exchange + SEBI).
Initial values (researched 2026-10-04, §7.6), marked **"verify against a contract note before live"** (X-Q6):

| Charge (equity options, NSE) | Value | Side |
|---|---|---|
| Brokerage (Angel One) | ₹20 per **executed order** | both |
| STT | **0.15 %** of premium (raised from 0.10 % by Budget 2026-27, effective 1 Apr 2026); 0.15 % of intrinsic value if exercised | sell |
| Exchange transaction | ≈ 0.035 % of premium (sources quote 0.03503 % and 0.03553 %, so confirm) | both |
| SEBI turnover fee | ₹10 per crore | both |
| Stamp duty | 0.003 % of premium | buy |
| GST | 18 % on (brokerage + exchange + SEBI) | both |

Worked round trip, 1 lot × 750 at ₹100 (₹75,000 each side): brokerage 40 + STT 112.5 +
exchange ≈ 52.5 + SEBI 0.15 + stamp 2.25 + GST ≈ 16.7 = **≈ ₹224 ≈ 0.30 % of premium**.
Crossing a 1 % spread costs about **₹750** for the same trade. **The spread, not the brokerage,
is the main execution cost for option buyers**, which is why E4 / E6 control it so tightly.
```
total_execution_cost = brokerage + exchange + SEBI + stamp + GST (+ STT on exit) + slippage_cost
slippage_cost        = (avg_fill − reference) × filled_qty
```
Both figures go in the report and the journal. The journal's PnL becomes **net of charges**
(gross is kept as `gross_pnl`).

#### E11 — Slippage definitions (resolves a mismatch in the brief, §8 vs §22)
The brief measures slippage once against the expected price (100.50) and once against the ask (−0.12). We record all three:

| Field | Formula | Meaning |
|---|---|---|
| `slippage` / `slippage_pct` (primary) | `avg_fill − reference(ask)` | Execution quality. Negative = price improvement (the brief's §22 example) |
| `slippage_vs_mid` | `avg_fill − mid at start` | Cost of crossing the spread |
| `signal_decay` | `reference − signal_premium` | Price moved between approval and execution (feeds Ranking / ICARE latency studies) |
| `implementation_shortfall` | `(avg_fill − arrival_mid) × filled_qty + charges + missed_cost` | Standard total execution cost (Perold). This is the target Module 19 learns |
| `missed_move_pct` | premium change 5 / 15 min after a cancelled or unfilled quantity | Opportunity cost of *not* filling. Without it, a too-tight cap looks free |

#### E12 — State machine (brief §18)
```
RECEIVED → VALIDATING → SIZING → WORKING ⇄ PARTIAL ─► FILLED
                │           │        │          ├─► PARTIAL_FILL_TIMEOUT / PARTIAL_FILL_STOPPED
                │           │        │          └─► (kill) PARTIAL_KILLED
                │           │        ├─► CANCELLED_TIMEOUT
                │           │        ├─► ABORTED_SLIPPAGE_LIMIT
                │           │        └─► (kill) KILLED
                └───────────┴─► REJECTED_BEFORE_EXECUTION
```
* State is persisted in `md:exec:state:{trade_id}` after every transition. On restart the
  runner **reconciles before anything else**: in live mode it reads the broker order book
  (order tag = `trade_id`) and cancels orphans; in paper mode it reads the stored state.
  A command is never re-sent blind.
* Terminal states release the `md:exec:active` slot.

#### E13 — Kill switch (brief §25)
Checked before validation, before every slice and on every ladder step.
On `1`: cancel the working order immediately and do not continue the entry. If part of the
order was already filled, the result is `PARTIAL_KILLED` with event `PARTIAL_POSITION_CREATED`.
The fill is still published to `md:exec:fill`, so the journal opens that partial position and
ICARE SL / Module 18 manage it. The executor never exits a position by itself (the brief's
Exit / Risk layer does).

#### E14 — Broker adapter & modes
`EXEC_MODE=shadow|paper|live`, default **shadow**:

| Mode | Orders | Journal |
|---|---|---|
| `shadow` | `PaperBroker` simulates the full algorithm | Unchanged: opens at the ask on `md:icare`. The exec report is attached to the record (`exec_*` fields) for comparison |
| `paper` | `PaperBroker` | Opens from `md:exec:fill` (actual simulated avg price, qty, charges) |
| `live` | `AngelBroker`: SmartConnect `placeOrder` (variety NORMAL, LIMIT, product INTRADAY, duration DAY, `ordertag=trade_id`), `modifyOrder`, `cancelOrder`, `orderBook` / individual order status polled every `EXEC_POLL_MS` (500 ms) within rate limits | Opens from `md:exec:fill` (broker-confirmed fills) |

* **PaperBroker fill model:** a BUY limit fills when limit ≥ ask. Fill qty = depth at levels ≤ limit
  (depth5), rounded down to whole lots, capped at the order qty. The rest stays working. Latency
  is `EXEC_PAPER_LATENCY_MS` (300). There is no queue-position model, so paper fills are
  **optimistic** (§7.4).
* **Regulatory constraints for live (SEBI retail algo framework, in force since 1 Apr 2026, §7.6):**
  - **LIMIT orders only.** Market and IOC orders are not allowed for API / algo orders.
    E6 already uses limit orders only. Duration is always `DAY`, never `IOC`.
  - Orders are accepted only from the **registered static IP** (whitelisted with Angel; the
    secondary IP can be changed at most once a week) and after **daily 2FA login**
    (no long-lived refresh sessions). The live runner checks the session at startup and
    refuses to trade if the IP / session check fails.
  - **< 10 orders per second** per exchange segment keeps us a "generic algo ID" user
    (no exchange registration needed). A Redis token bucket caps place + modify + cancel at
    `EXEC_MAX_OPS` = **5 / s** across all workers, below both this threshold and Angel's own
    order-API limit (documented at about 9–20 / s, so verify).
  - Fills are tracked through the **order-update WebSocket** (`SmartWebSocketOrderUpdate`),
    with the individual order-status API (10 req/s) as a fallback poll. Final quantities are
    always reconciled with the trade book.
  - Re-pricing uses **`modifyOrder`, not cancel + new**. This halves the API calls and avoids
    double fills during a cancel. A partially filled order can be modified (Angel forum).
    Whether the quantity in a modify means the original total or the remaining amount is
    reported inconsistently, so P29 tests it live with 1 lot before relying on it.
  - **Cancel-after-fill race:** a cancel can arrive after the exchange has already filled the
    order. The executor never assumes a cancel worked. It reads the final `filledshares` from
    order details before publishing the report.
* **Live is gated three ways:** `EXEC_MODE=live` **and** `EXEC_LIVE_ENABLED=1` **and** the
  per-day order-value limit `EXEC_LIVE_MAX_ORDER_VALUE`. Going live is a separate, explicit
  decision after P30 (X-Q1). Live **exits** (E17) must exist first.

#### E15 — Journal & feedback loop (brief §26–§27)
* `md:exec` events: `COMMAND, VALIDATED, REJECTED, ORDER_PLACED, ORDER_MODIFIED, FILL, PARTIAL,
  CANCELLED, KILLED, FINAL`. Every order carries bid, ask, spread, limit price and qty.
* Final report = brief §22 object + `signal_ts, order_ts, first_fill_ts, slippage_vs_mid,
  signal_decay, charges{}, total_execution_cost, reductions[], cancel_reason, mode`.
* The journal record gets these as `exec_*` fields. `learn_weights.py` gets a new
  **"execution"** section: slippage, fill ratio and time-to-fill by symbol × time-of-day
  bucket × spread bucket × order size (lots vs depth).
* This is the data set for **Module 19 Slippage Estimator** (not built here). A hook is
  left for later: if `md:slippage:est:{SYM}` exists, Ranking (R9) subtracts expected
  slippage from EV. That makes the loop *Ranking → Executor → Journal → Estimator → Ranking*.

#### E16 — Journal changes
* In `paper` / `live`, `md:exec:fill` replaces `md:icare` as the journal's open trigger:
  entry = `average_fill_price`, qty = filled lots × lot size, `entry_charges` stored, and
  ICARE's SL / target are kept as **prices** (not re-based on the fill).
* In `shadow`, the journal is unchanged apart from the `exec_*` fields.

#### E17 — Exits are out of scope for the paper phases but required before live
This brief covers **entry**. Exits stay as today (journal closes at bid in paper).
Before `live`, an **exit path** must exist: the journal / Module 18 exit signal goes to the
executor as a SELL command (reference = bid). Exits use a different policy: a wider slippage
budget (`EXEC_EXIT_MAX_SLIPPAGE_PCT`), the ladder steps *down*, and an SL exit is **never
abandoned** (at timeout it re-prices to the bid and keeps going). Because market orders are
not allowed (E14), a stop-loss exit is a **limit at bid − N ticks inside the LPP band, re-priced
every 1 s** until filled. This is phase P29.

#### E18 — Shadow first (same principle as D3 / T12 / R16)
Run `shadow` for several sessions. Compare the journal's "fill at ask" with the executor's
simulated fill (`exec_slippage`, fill ratio, rejections by reason), then switch to `paper`.
`live` needs its own decision (X-Q1).

#### Responsibility matrix (brief §29) mapped to our modules
| Brief module | Here |
|---|---|
| Trade Ranking | Module 13 `run_trade_ranking.py` |
| Strike Selector | SIE `run_strike_intel.py` |
| Lot Sizing / Capital Allocation / Risk Management / Portfolio Monitor | ICARE `run_icare.py` (Module 17 not separate) |
| **Order Executor, Bid-Ask Analyzer (execution view), Executor slippage control** | **Module 14 `run_order_executor.py` (this section)** |
| Bid-Ask Analyzer (signals) | `run_bidask_analyzer.py`, `run_bidask_imbalance.py` |
| Slippage Estimator | Module 19, not built (fed by E15) |
| Exit Engine | `run_trade_journal.py` (+ E17 for live) |
| Smart TSL | Module 18 `run_adaptive_tsl.py` (T14) |
| Trade Journal | Module 22 `run_trade_journal.py` |

### 7.3 Implementation plan

| Phase | Deliverable | Files |
|---|---|---|
| **P23** ✅ | TSL rule T14: unit test for 10 → 100, 10 % → stop 90 (plus "price falls to 95, stop stays 90"); docstring note. No behaviour change expected | `tests/test_adaptive_tsl.py`, `app/adaptive_tsl.py` (docstring) |
| **P24** ✅ | Market data for execution: per-contract depth5 in the bid-ask latest key (or a new `md:depth:latest:{TSYM}`), tick size from ScripMaster (₹0.01 / ₹0.05), daily NSE freeze-quantity load → `md:ref:freeze_qty:{SYM}`, stock-option expiry dates | `run_bidask_analyzer.py` / `run_bidask_imbalance.py`, `app/scripmaster.py` |
| **P25** ✅ | Pure engine: command + validation, quote / spread / depth-within-cap, cap & ladder with tick rounding, quantity MIN(), slicing, charges, partial-fill continue / stop, state machine, kill switch, report. Tests for every brief example: spread 1.98 % → reject; cap 101.505 → 101.50; ladder 100.50 → 100.60 …; margin 50k / 15k → 3 lots; MIN(4, 3) = 3; available 35k → 2 lots; 10 lots / depth 3 → slices 3, 2, …; benefit ₹2,500 > cost ₹430 → continue, ₹400 < ₹1,200 → cancel; timeout → `PARTIAL_FILL_TIMEOUT` 2 / 4; 3 @ 100.80 + 1 @ 101.10 → avg 100.875, slippage −0.125 vs ask; kill during partial → `PARTIAL_KILLED`; ask above cap → `ABORTED_SLIPPAGE_LIMIT`, never chases | `app/order_executor/*.py`, `charges.json`, `tests/test_order_executor.py` |
| **P26** ✅ | `PaperBroker` (depth-based fills, latency) + `Broker` interface + reconcile-on-restart | `app/order_executor/broker.py`, tests |
| **P27** ✅ | Runner: consume `md:icare` APPROVED (and re-entries), idempotency lock, `md:exec:active` slots, loop on `EXEC_POLL_MS`, publish `md:exec` / latest / state / `md:exec:fill`; shadow / paper modes | `run_order_executor.py`, `tests/test_order_executor_runner.py` |
| **P28** ✅ | Integration: journal opens from `md:exec:fill` in paper / live, `exec_*` fields in all modes, net-of-charges PnL; ICARE / Ranking exposure counts executing trades. Wiring: `run_all.sh/.ps1`, layers archiver (`md:exec`, `md:exec:fill`), `pipeline_health.py` (Module 14 → implemented), Streamlit "Execution" panel (working orders, slippage, rejections), client Excel row, `learn_weights.py` "execution" section, RUN.md, spec index | `run_trade_journal.py`, `app/trade_journal.py`, existing files |
| **P29** ◐ | Live path (code only, **disabled**: X-Q1; startup refuses live without a matching static IP: X-Q11): `AngelBroker` (LIMIT + DAY only; place / modify / cancel; order-update WebSocket + status fallback; trade-book reconcile; cancel-after-fill race; LPP rejection handling), Redis order-rate bucket (5 / s), static-IP / session pre-check, limit-only exit SELL path (E17), live-mode gates. Tests use a fake SmartConnect; modify-quantity behaviour is confirmed live with 1 lot | `app/order_executor/broker.py`, `run_trade_journal.py`, tests |
| **P30** ◐ | Verification: unit tests; end-to-end on Redis DB 15 with scripted quotes (full fill, partial → continue, partial → stop, timeout, slippage abort, kill mid-fill); replay of the 2026-09-21 archive in shadow mode; the earlier 207 tests stay green | — |

P25–P26 are testable without Redis. P27 produces shadow output. P28 is the first step that changes
existing behaviour, and only with `EXEC_MODE=paper`. Nothing reaches the broker until P29 **and** X-Q1.

### 7.4 Risks / notes
* **Paper fills are optimistic.** Without a queue model, a limit at the ask is assumed to fill
  up to the visible depth. Real fills will be worse. Treat shadow slippage as a lower bound.
* With 2 s ladder steps and a 10 s timeout, a limit order gets about 5 price levels. On
  illiquid stock options many entries will end `CANCELLED_TIMEOUT`. That is the intended result
  (no chasing), but it lowers the trade count.
* Depth5 comes from the tick stream and is only as fresh as the last tick for that contract.
* Charges change with regulation. `charges.json` must be checked against the broker's contract
  note before `live`.
* Live trading adds broker-side failure modes (rejections, freeze quantity, RMS blocks,
  session expiry). P29 handles the known ones. The first live sessions should use 1 lot and the
  `EXEC_LIVE_MAX_ORDER_VALUE` limit.

### 7.5 Open questions (answered 2026-10-04: X-Q1 / X-Q11 as noted, all other defaults accepted)

* **X-Q1** ✅ **Answered: yes.** Build **shadow + paper** (P23–P28, P30). The live broker code
  (P29) is written and tested against a fake SmartConnect but stays **disabled**. Live trading
  needs a separate go-ahead.
* **X-Q2** Max spread at execution: **1.0 %** (proposed) or the brief's 0.50 %?
* **X-Q3** Max slippage **0.50 %** of the ask, 10 s timeout, 2 s ladder step: OK?
* **X-Q4** When margin / depth allows fewer lots than ICARE approved: **reduce** (default)
  or reject the entry?
* **X-Q5** Completion rule: continue only if `benefit > 1.5 × additional cost`, and skip residuals
  below ₹2,000. OK?
* **X-Q6** Charges: start from Angel's published F&O tariff (₹20 per order + statutory) in
  `charges.json`, and you confirm the numbers from a contract note before live?
* **X-Q7** Price drift: reject if the ask is > **2 %** above the premium ICARE approved?
* **X-Q8** Journal PnL net of charges (gross kept separately), and `entry_premium` = actual
  average fill in paper mode?
* **X-Q9** TSL (T14): "current price" = **highest bid since entry** (stop never moves down).
  Confirm this is what you meant, not a stop that also drops when the price drops.
* **X-Q10** No new entries before **09:20**, and no new stock-option entries after **13:00 on expiry day**?
* **X-Q11** ✅ **Answered: no static IP registered yet.** Consequences:
  - `EXEC_MODE=live` is **refused at startup**: the runner exits with `LIVE_BLOCKED_NO_STATIC_IP`
    unless `EXEC_STATIC_IP` is set **and** the outgoing public IP matches it. This check comes
    in addition to the three gates in E14.
  - Nothing else changes. Market data, greeks, candles and getRMS reads are not orders and do
    not need the static IP. Shadow and paper modes run as planned.
  - Before live: rent a static IP (cloud VM / VPS with a fixed public IP, or an ISP static IP),
    register it as the primary IP in the Angel One SmartAPI app settings (a secondary IP is
    optional; IP changes are allowed at most once a week), run the trading process **from that
    machine**, and confirm the daily TOTP login works unattended.

### 7.6 Research check & expert review (web research 2026-10-04)

The plan was checked against current Indian market rules and broker documentation.
What changed in the plan, and why:

| Finding | Effect on the plan |
|---|---|
| **SEBI retail algo framework (in force 1 Apr 2026):** API / algo orders must be **limit orders** (market and IOC not allowed), must come from a registered **static IP**, need **daily 2FA**, and must stay **< 10 orders/s** for a generic algo ID | E6 is limit-only by design. E14 adds the static-IP / session check, `DAY` duration, a 5 / s order budget, and limit-only SL exits (E17). X-Q11 |
| **STT on option sales raised to 0.15 %** (from 0.10 %, Budget 2026-27, effective 1 Apr 2026) | E10 table. Round-trip cost ≈ 0.30 % of premium before spread |
| Angel One: flat **₹20 per executed order** for F&O; stamp 0.003 % (buy), SEBI ₹10/crore, GST 18 %, exchange ≈ 0.035 % (two different figures published) | E10 values; confirm the exchange rate against a contract note (X-Q6) |
| **NSE tick size for stock options = ₹0.01** when the underlying is below ₹250 (since 3 Nov 2025), ₹0.05 otherwise | E6 reads the tick from ScripMaster instead of assuming 0.05 |
| **Quantity freeze limits** (units per order, per stock; index limits revised again on 5 Oct 2026) | E8 slices by the freeze quantity, loaded daily |
| **NSE Limit Price Protection:** limit orders far from the 30-second average trade price are rejected | E6 handles `PRICE_OUT_OF_BAND` |
| SmartAPI: a partially filled order **can be modified**; order-status WebSocket and a 10 req/s individual-status API exist | E14: modify instead of cancel + new, WebSocket fills, reconcile with the trade book |
| Stock options are **physically settled**. ITM positions at expiry mean delivery and higher margin | E4 expiry-day cut-off (X-Q10). The existing 15:20 EOD exit already keeps us intraday |
| Stock-option liquidity is a fraction of index-option liquidity, and spreads are wider and change quickly | Confirms that the spread is the dominant cost (E10 example: ~₹750 spread vs ~₹224 charges per lot). Keep `EXEC_MAX_SPREAD_PCT` tight (X-Q2) |

**Expert notes (finance / market microstructure)**
* **Cost hierarchy for an option buyer:** spread crossing > adverse price move while waiting
  (signal decay) > statutory charges > brokerage. The brief focuses on brokerage for partial
  fills (§12–§15). In rupees it is the smallest item, so E9 compares the *extra per-order*
  cost with EV and uses the spread / slippage figures as the main inputs.
* **Mid-price entries carry adverse selection.** A resting buy at mid tends to fill when the
  market moves down, which is when a momentum signal is weakest. That is why E6 uses the
  tight-spread shortcut and why `missed_move_pct` is recorded: both errors (overpaying and
  missing) must be measurable.
* **Measure with implementation shortfall** (arrival mid → fill + charges + cost of missed
  quantity). It is the standard transaction-cost-analysis metric and the right training target
  for Module 19.
* **Costs belong in the EV upstream.** Ranking (R9) and ICARE compute EV before costs. Once
  P28 has data, EV should subtract the expected round-trip cost (charges ≈ 0.30 % + expected
  spread / slippage from Module 19). Otherwise small-edge trades look profitable and lose
  after costs. This is a hook for now (E15) and not changed in this plan.
* **Exits must be guaranteed even though market orders are not allowed.** This is the main
  live-trading risk the 2026 rules create, so E17 is a hard prerequisite for `live`.

Sources:
[SEBI retail algo rules 2026 (Fyers)](https://fyers.in/notice-board/new-sebi-framework-for-retail-algo-trading-from-april-01-2026/) ·
[Angel One: SmartAPI changes from 1 Apr 2026](https://www.angelone.in/news/market-updates/what-s-changing-in-angel-one-s-smartapi-access-from-april-1-2026) ·
[STT hike on F&O (ClearTax)](https://cleartax.in/s/securities-transaction-tax-stt) ·
[STT hike explained (Upstox)](https://upstox.com/news/personal-finance/tax/explained-how-the-stt-hike-on-equity-futures-and-options-affects-traders-and-investors/article-189260/) ·
[Angel One charges (Chittorgarh)](https://www.chittorgarh.com/brokerage_charges/angel-broking/14/) ·
[NSE SEBI fees / levies](https://www.nseindia.com/static/invest/first-time-investor-sebi-turnover-fees-stt-other-levies) ·
[NSE stamp duty](https://www.nseindia.com/static/invest/first-time-investor-stamp-duty-charges-taxes) ·
[Stock-option tick size revision (Angel One)](https://www.angelone.in/announcements/market/revision-in-tick-size-for-stock-options-contracts-by-nse) ·
[Freeze quantity explained (Bajaj Finserv)](https://www.bajajfinserv.in/freeze-quantity-in-options) ·
[Index freeze limits from 5 Oct 2026 (Zerodha Q&A)](https://tradingqna.com/t/nse-revises-index-f-o-quantity-freeze-limits-from-october-5-2026/198502) ·
[NSE Limit Price Protection FAQ](https://www.nseindia.com/static/trade/limit-price-protection-faqs) ·
[SmartAPI: modify partially filled order](https://smartapi.angelone.in/smartapi/forum/topic/4839/can-we-modify-partially-filled-order-using-smart-api) ·
[SmartAPI order-update WebSocket](https://smartapi.angelone.in/smartapi/forum/topic/4041/new-real-time-order-updates-via-websocket-from-tns-angelone-in-smart-order-update) ·
[SmartAPI individual order status (10 req/s)](https://smartapi.angelone.in/smartapi/forum/topic/4013/important-update-individual-order-status-api-using-unique-order-id-10-requests-second) ·
[SmartAPI rate-limit changes](https://smartapi.angelone.in/smartapi/forum/topic/4387/changes-in-api-rate-limit) ·
[Index vs stock options liquidity (Jainam)](https://www.jainam.in/blog/index-options-vs-stock-options/)

### 7.7 Implementation notes P23–P30 (decisions made while building)

**Files** — `app/order_executor/` (`config.py`, `market.py`, `pricing.py`, `quantity.py`, `charges.py`,
`fills.py`, `command.py`, `state.py`, `engine.py`, `broker.py`), `run_order_executor.py`, `charges.json`,
`tests/test_order_executor.py`, `tests/test_order_executor_runner.py`; P23 test in `tests/test_adaptive_tsl.py`.

**Deviations from the plan above**
* **Module number:** the spec index numbers the Order Executor **14** (15 = Exit Order Module).
  `pipeline_health.py` said 15; its label is fixed, and everything here now says Module 14.
* **No separate freeze-quantity / tick feed (P24):** ScripMaster already has `tick_size` (paise:
  5 = ₹0.05, 1 = ₹0.01; 3,910 stock-option contracts are on the ₹0.01 tick) and `freeze_qty` per
  contract. `scripmaster.option_specs()` reads both at startup and again each day. No
  `md:ref:freeze_qty:*` keys. If ScripMaster cannot be loaded, the fallbacks are tick 0.05 and
  `EXEC_MAX_LOTS_PER_ORDER`.
* **Depth source (P24):** `run_bidask_analyzer.py` now adds `bid_qty`, `ask_qty`, `ltp` and the
  depth5 prices / sizes to `md:bidask:latest:{TSYM}` (extra fields only; existing readers are unchanged).
* **Risk limit at execution** uses the ICARE SL **price**: risk per lot = `(cap − sl_premium) × lot`,
  so paying more for the entry can only reduce the lots.
* **Margin** = account available margin minus margin promised to other trades still executing.
  The account fallback is the paper ledger, same as ICARE.
* **Shadow mode and the journal mirror:** in shadow mode the journal opens its own position
  from `md:icare` at once. The executor ignores that one position (same contract, opened since
  the approval), so it does not reject itself as `DUPLICATE_UNDERLYING`.
* **Restart (paper):** simulated orders live in memory. After a restart, a working order counts
  as cancelled, the fills already made are kept, and the trade ends `PARTIAL_FILL_STOPPED` or
  `CANCELLED_TIMEOUT`. The ICARE payload is kept in `md:exec:icare:{trade_id}`, so the journal
  still gets `md:exec:fill`.
* **Opportunity cost (E11):** after a trade with no fill or a partial fill, `md:exec:missed` schedules
  `MISSED_MOVE` events at +5 / +15 min (mid vs the reference ask).
* **`md:exec` also carries one `REPORT` event per trade** (the final report, without the order list).
  `learn_weights.py` reads these for its execution section.
* **Exposure (E4) upstream:** `run_icare.load_positions()` adds `md:exec:active` trades as
  executing pseudo-positions when `EXEC_MODE=paper|live`. That covers ICARE and Trade Ranking,
  which share the function.
* **Journal PnL (X-Q8):** applies in **all** modes. `pnl`, `pnl_pct` and `win` are net of entry +
  exit charges, with `gross_pnl` / `gross_pnl_pct` / `charges` kept. Shadow entries use a one-order
  estimate for entry charges. Journal records written before this change are gross, so compare
  old and new buckets with care.
* **Live (P29):** `AngelBroker` sends LIMIT / DAY / NORMAL orders with `ordertag` = our order id,
  modifies through `modifyOrder`, reads fills from `orderBook()` (cumulative `filledshares` /
  `averageprice`; a cancel never assumes success), checks a per-order value limit, and uses a 5 / s
  token bucket. `run_order_executor.py` exits at startup with `LIVE_BLOCKED_NO_STATIC_IP` (or another
  `LiveGate` problem) unless every live gate passes. Not done yet: the order-update WebSocket (it
  polls the order book instead), the exit SELL path (E17), and the 1-lot live check of `modifyOrder`
  quantity semantics. These must be done before any live use.

**Verified**
* 247 unit tests (40 new, plus the T14 test); all earlier tests unchanged and green.
* Covered: every brief example (1.98 % spread reject, 101.505 → 101.50 cap, 100.50 start, 0.10
  ladder, 50k / 15k → 3 lots, 35k → 2, 10 lots / depth 3 → 3-3-3-1 slices, ₹2,500 vs ₹430 continue,
  cost > benefit stop, 4-lot fill in 2 orders → avg 100.875 / slippage −0.125 vs the ask, timeout
  2 / 4, kill switch mid-fill, ask above cap never chased, LPP rejects → abort, restart recovery).
  The runner tests on an in-memory Redis cover: ICARE message → executor → `md:exec:fill` →
  journal opens at the actual average price and lots; duplicate message executes once; kill switch;
  same underlying while executing; shadow mirror; restart; missed move; ICARE exposure; net PnL;
  learn_weights execution report.
* **Replay on real archived books** (`md_ticks_opt`, 16 / 18 / 21 Sep 2026; the archive only covers
  ~14:53–15:40, so the clock gates were switched off). 1,077 simulated approvals at random times
  on 160 contracts (all archived strikes, including far OTM):
  - 60 % rejected `SPREAD_TOO_WIDE`. The median spread at approval was 1.21 %; only 40 % were ≤ 1 %
    and **6 % were ≤ 0.5 %**. This confirms X-Q2: the brief's 0.5 % would block nearly everything.
  - Of the 435 executable approvals: 78 % `FILLED`, 4 % partial, 18 % no fill (77 timeouts,
    18 aborted above the cap; no chasing).
  - Filled trades: fill ratio 0.97, slippage vs ask −0.05 % on average (median 0, a small price
    improvement), +0.28 % vs mid (the half-spread we pay), median 3 s to fill, 1.04 orders per trade.
  - Real approvals pass SIE / ICARE liquidity filters first, so their spreads should be tighter
    than these random strikes. Paper fills are optimistic (§7.4).
  - Reproduce with `python scripts/replay_executor.py [YYYY-MM-DD]` (read-only).

**Not yet done:** a run on real Redis (the `md_redis` container is stopped) and a full-session
replay (the archive only covers the last ~40 minutes of each day).

**Before `EXEC_MODE=paper`:** run a few sessions in shadow, then check `python learn_weights.py`
→ "Module 14 execution" (status mix, slippage by spread bucket, missed move). **Before `live`:**
register a static IP (X-Q11), build the E17 exit path and the order WebSocket, check `charges.json`
against a contract note, and get a separate go-ahead (X-Q1).

---

## References
- Angel One SmartAPI docs (getRMS, margin calculator): https://smartapi.angelbroking.com/docs
- Margin Calculator batch API announcement: https://smartapi.angelone.in/smartapi/forum/topic/4010/calculate-margin-requirements-with-smartapi-s-new-margin-calculator-api
- smartapi-python (`rmsLimit()`, `placeOrder` / `modifyOrder` / `cancelOrder` / `orderBook`): https://github.com/angel-one/smartapi-python
