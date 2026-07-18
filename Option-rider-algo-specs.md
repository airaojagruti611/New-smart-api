# Stock Option Rider — Algorithm Specification

> Reference spec for the coding agent. Each module below is self-contained: objective, required inputs, calculation logic, rules/thresholds, example data, and a reference Python implementation where provided in the source design. Build modules in the order listed — later modules consume the outputs of earlier ones.

---

## 0. Module Index

| # | Module | Layer |
|---|--------|-------|
| 1 | Indicator Signals (Supertrend + EMA Cross + Pivots) | 1.1 Core Market Analysis |
| 2 | Volume Analyzer | 1.1 Core Market Analysis |
| 3 | Market Regime Detector | 1.1 Core Market Analysis |
| 4 | Bid-Ask Analyzer | 1.1 Core Market Analysis |
| 5 | Open Interest Calculations | 1.1 Core Market Analysis |
| 6 | Greeks Calculator | 1.2 Option Microstructure |
| 7 | Liquidity Score | 1.2 Option Microstructure |
| 8 | Expected Move Calculator | 1.3 Volatility Intelligence |
| 9 | Greeks Change Predictor | 1.3 Volatility Intelligence |
| 10 | Strike Price Selector | 1.4 Trade Decision Layer |
| 11 | Lot Sizing | 1.4 Trade Decision Layer |
| 12 | Probability Engine | 1.4 Trade Decision Layer |
| 13 | Trade Ranking Engine | 1.4 Trade Decision Layer |
| 14 | Order Executor / Trade Entry | 1.4 Trade Decision Layer |
| 15 | Exit Order Module | 1.4 Trade Decision Layer |
| 16 | Circuit Breaker / Kill Switch (loss cap) | 1.4 Trade Decision Layer |
| 17 | Risk Management | 1.5 Risk Layer |
| 18 | Smart Stop Loss | 1.5 Risk Layer |
| 19 | Slippage Estimator | 1.5 Risk Layer |
| 20 | Capital Allocation | 1.6 Capital Layer |
| 21 | Portfolio Exposure Monitor | 1.7 Monitoring Layer |
| 22 | Trade Journal Engine | 1.7 Monitoring Layer |
| 23 | Accumulation Phase Detector | 1.7 Monitoring Layer |

**Note:** Modules 1–8 have full design detail in the source material (reproduced below). Modules 9, 10–23 are named in the index but not yet specified in detail — flag these as TODO/backlog items for the coding agent; do not invent logic for them.

### Add-ons (backlog, not yet specified)
1. Upper circuit / lower circuit logic
2. Daily volume % gain compared with 20 SMA
3. Box breakout/breakdown from last 60-minute range
4. Volume spike
5. Sector-wise buying/selling heatmap
6. Pre-market data analyser
7. Heavy buying/selling detection in past few minutes
8. Fast price moving up/down
9. Volume breakers up/down
10. Volume increase % intraday
11. Avg sector change

### Outstanding Infrastructure Requirement
**Broker Bridge (AngelOne SmartAPI)** — explicit module still needed:
- Order placement
- Order status polling
- Rejection handling
- Lot size / freeze quantity enforcement for NSE options

---

## 1. System Architecture (Folder Structure)

```
trading_system/
│
├── config/
│   └── settings.py
│
├── data/
│   ├── market_data.py
│   ├── options_data.py
│   └── database.py
│
├── core_market/
│   ├── indicator_signals.py
│   ├── volume_analyzer.py
│   └── market_regime.py
│
├── options_microstructure/
│   ├── oi_calculations.py
│   ├── greeks_calculator.py
│   ├── imbalance_finder.py
│   ├── bid_ask_analyzer.py
│   └── liquidity_score.py
│
├── volatility/
│   ├── volatility_regime.py
│   ├── expected_move.py
│   └── gamma_exposure.py
│
├── decision_engine/
│   ├── strike_selector.py
│   ├── probability_engine.py
│   └── trade_ranking.py
│
├── risk/
│   ├── risk_management.py
│   ├── smart_stoploss.py
│   └── slippage_estimator.py
│
├── capital/
│   └── capital_allocator.py
│
├── execution/
│   └── order_executor.py
│
├── monitoring/
│   ├── portfolio_monitor.py
│   └── trade_journal.py
│
├── dashboard/
│   └── streamlit_dashboard.py
│
└── main.py
```

### End-to-end Algo Workflow

```
Fetch Market Data
      ↓
Calculate Supertrend
      ↓
Calculate EMA9 & EMA26
      ↓
Calculate Pivot Levels
      ↓
Check Higher Timeframe Trend
      ↓
Check Supertrend Alignment
      ↓
Check EMA Momentum
      ↓
Check Pivot Break
      ↓
Generate CALL / PUT Signal
      ↓
Send to Strike Selection Engine
      ↓
Risk Management
      ↓
Execute Trade
```

### Full System Integration Flow

```
Market Data
      ↓
Market Regime Detector
      ↓
Open Interest Analyzer
      ↓
Indicator Engine (Supertrend + EMA + Volume)
      ↓
Bid-Ask / Smart Money Layer
      ↓
Greeks Analyzer (Phase Detection)
      ↓
Expected Move Engine (Prediction Only)
      ↓
Trade Structuring Engine (Execution Decisions)
      ↓
Liquidity Score Gate (Entry Size / Scale-Out)
      ↓
Risk Layer Check
      ↓
Capital Allocation
      ↓
Order Execution
      ↓
Monitoring / Trade Journal
```

---

## 2. Module 1 — Indicator Signals

### 2.1 Overview
Uses three indicators to produce a directional bias score used to decide CALL or PUT trades:
- **Supertrend** (Trend Filter)
- **EMA Cross (9, 26)** (Momentum Confirmation)
- **Pivot Levels** (Entry Trigger)

### 2.2 Scoring Board

| Signal | Score |
|---|---|
| Strong Bullish | +2 |
| Bullish | +1 |
| Neutral | 0 |
| Bearish | -1 |
| Strong Bearish | -2 |

### 2.3 System Design — Three Logical Layers

```
TREND FILTER
   ↓
MOMENTUM CONFIRMATION
   ↓
ENTRY LEVEL TRIGGER
   ↓
CALL / PUT DECISION
```

| Layer | Indicator |
|---|---|
| Trend Filter | Supertrend |
| Momentum | EMA Cross |
| Entry Trigger | Pivot Levels |

### 2.4 Supertrend Module (Trend Filter)

**Purpose:** Determine primary market direction.

**Recommended Parameters**
- ATR Period = 7
- Multiplier = 1

**Calculation Concept**
```
Supertrend = Price ± (ATR × Multiplier)
```

**Output Conditions**

| Condition | Trend |
|---|---|
| Close > Supertrend | Bullish |
| Close < Supertrend | Bearish |

**Multi-Timeframe Confirmation**
```
30M Supertrend < Close
10M Supertrend < Close
5M  Supertrend < Close
1M  Supertrend < Close
```
Meaning: Strong Uptrend.

**Algo Interpretation**
```
IF majority timeframe supertrend = bullish → Bias = CALL
IF majority timeframe supertrend = bearish → Bias = PUT
```

### 2.5 EMA Cross Module (Momentum Confirmation)

**EMA Setup:** Fast EMA = 9, Slow EMA = 26

**Signal Conditions**
- **Bullish Cross:** EMA9 crosses above EMA26 → momentum turning bullish
- **Bearish Cross:** EMA9 crosses below EMA26 → momentum turning bearish

**Momentum Confirmation Logic (combine with Supertrend)**

Bullish Alignment: `Supertrend = Bullish AND EMA9 > EMA26` → **BUY CALL**
Bearish Alignment: `Supertrend = Bearish AND EMA9 < EMA26` → **BUY PUT**

### 2.6 Pivot Level Module (Entry Trigger)

```
Pivot = (High + Low + Close) / 3

Resistance:
R1 = (2 × Pivot) − Low
R2 = Pivot + (High − Low)

Support:
S1 = (2 × Pivot) − High
S2 = Pivot − (High − Low)
```

### 2.7 Entry Trigger Logic

**CALL TRADE LOGIC**

Final Rule:
```
IF Close > Supertrend
AND EMA9 > EMA26
AND Price breaks Pivot
THEN BUY CALL OPTION
```

Strong Momentum Rule:
```
IF Price breaks R1
AND EMA9 > EMA26
AND Supertrend bullish
THEN BUY CALL
```

**PUT TRADE LOGIC**

Final Rule:
```
IF Close < Supertrend
AND EMA9 < EMA26
AND Price breaks Pivot
THEN BUY PUT
```

Strong Breakdown Rule:
```
IF Price breaks S1
AND EMA9 < EMA26
AND Supertrend bearish
THEN BUY PUT
```

### 2.8 Multi-Timeframe Trend Filter (Chartink Logic)

Scanner checks:
```
Monthly Close > Previous Month
Weekly Close > Previous Week
Daily Close > Previous Day
```
→ Higher Timeframe Trend = Bullish

**Algo rule:**
```
If higher timeframe trend bullish → Only allow CALL trades
If higher timeframe trend bearish → Only allow PUT trades
```
This dramatically reduces false signals.

### 2.9 Complete Signal Matrix

| Supertrend | EMA | Price Level | Action |
|---|---|---|---|
| Bullish | EMA9 > EMA26 | Break Pivot | CALL |
| Bullish | EMA9 > EMA26 | Break R1 | Strong CALL |
| Bearish | EMA9 < EMA26 | Break Pivot | PUT |
| Bearish | EMA9 < EMA26 | Break S1 | Strong PUT |

### 2.10 Python Logic Outline

```python
def option_signal(price, supertrend, ema9, ema26, pivot):

    if price > supertrend and ema9 > ema26 and price > pivot:
        return "BUY_CALL"

    elif price < supertrend and ema9 < ema26 and price < pivot:
        return "BUY_PUT"

    else:
        return "NO_TRADE"
```

### 2.11 Connection to Strike Selection

Example output:
```
Signal: BUY_CALL
Underlying: NIFTY
Trend: Bullish
Entry Price: Pivot Break
```
Strike module then selects: **ATM or Slight OTM Call**

### 2.12 Reference Implementation — Indicator Engine (Supertrend)

```python
import pandas as pd
import numpy as np


class FastSupertrend:

    def __init__(self, df):
        """
        df must contain columns:
        open, high, low, close
        """
        self.df = df.copy()

    # -----------------------------
    # ATR Calculation (Vectorized)
    # -----------------------------
    def calculate_atr(self, period=7):

        high = self.df["high"]
        low = self.df["low"]
        close = self.df["close"]

        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(period).mean()

        self.df["atr"] = atr

        return self.df

    # -----------------------------
    # Fast Supertrend
    # -----------------------------
    def compute_supertrend(self, period=7, multiplier=1):

        self.calculate_atr(period)

        hl2 = (self.df["high"] + self.df["low"]) / 2

        self.df["upperband"] = hl2 + multiplier * self.df["atr"]
        self.df["lowerband"] = hl2 - multiplier * self.df["atr"]

        close = self.df["close"].values
        upper = self.df["upperband"].values
        lower = self.df["lowerband"].values

        supertrend = np.zeros(len(self.df))
        trend = np.ones(len(self.df))

        for i in range(1, len(self.df)):

            if close[i] > upper[i - 1]:
                trend[i] = 1

            elif close[i] < lower[i - 1]:
                trend[i] = -1

            else:
                trend[i] = trend[i - 1]

                if trend[i] == 1 and lower[i] < lower[i - 1]:
                    lower[i] = lower[i - 1]

                if trend[i] == -1 and upper[i] > upper[i - 1]:
                    upper[i] = upper[i - 1]

            supertrend[i] = lower[i] if trend[i] == 1 else upper[i]

        self.df["supertrend"] = supertrend
        self.df["trend"] = np.where(trend == 1, "bullish", "bearish")

        return self.df
```

---

## 3. Module 2 — Volume Analyzer

### 3.1 Objective
Detect buyer vs. seller dominance in each 1-minute candle and validate whether the move has real participation.

Outputs:
- Bullish Volume Tick
- Bearish Volume Tick
- Neutral / Wrong Entry Possible
- (Optional) Volume Surge Confirmation

### 3.2 Required Inputs
From 1-minute OHLCV data: `high`, `low`, `close`, `volume`
Optional: `avg_volume`

### 3.3 Core Concept
Volume is split into buyer vs. seller participation based on where price closed within the candle:
- Price closes near high → buyers dominated
- Price closes near low → sellers dominated

### 3.4 Calculation Logic

```
Step 1 — Candle Range
range = high - low   (guard against divide-by-zero)

Step 2 — Estimate Buyer Volume
buy_volume = volume * (close - low) / range

Step 3 — Estimate Seller Volume
sell_volume = volume * (high - close) / range

Step 4 — Convert to Percentage
buy_percent  = (buy_volume / volume) * 100
sell_percent = (sell_volume / volume) * 100
```

### 3.5 Imbalance Rules

**Bullish Imbalance:** `buy_percent >= 60 AND sell_percent <= 40` → buyers dominating
**Bearish Imbalance:** `sell_percent >= 60 AND buy_percent <= 40` → sellers dominating

### 3.6 Optional Volume Surge Filter

```
volume_surge = current_volume / avg_volume
Rule: volume_surge > 2 → strong liquidity and market participation
```

### 3.7 Final Signal Logic

```
Bullish Tick:
  if buy_percent >= 60: signal = "Bullish Volume"
  if buy_percent >= 60 and volume_surge > 2: signal = "Strong Bullish Volume"

Bearish Tick:
  if sell_percent >= 60: signal = "Bearish Volume"
  if sell_percent >= 60 and volume_surge > 2: signal = "Strong Bearish Volume"

No Imbalance:
  signal = "Possible Wrong Entry"   # market participation unclear, avoid trade
```

### 3.8 Reference Implementation

```python
import pandas as pd
import numpy as np


class VolumeAnalyzer:

    def __init__(self, df):
        self.df = df.copy()

    def calculate_volume_imbalance(self):

        spread = self.df["high"] - self.df["low"]
        spread = spread.replace(0, np.nan)

        self.df["buy_volume"] = (
            self.df["volume"] * (self.df["close"] - self.df["low"]) / spread
        )
        self.df["sell_volume"] = (
            self.df["volume"] * (self.df["high"] - self.df["close"]) / spread
        )

        self.df["buy_percent"] = (self.df["buy_volume"] / self.df["volume"]) * 100
        self.df["sell_percent"] = (self.df["sell_volume"] / self.df["volume"]) * 100

        return self.df

    def calculate_volume_surge(self, period=20):

        self.df["avg_volume"] = self.df["volume"].rolling(period).mean()
        self.df["volume_surge"] = self.df["volume"] / self.df["avg_volume"]

        return self.df

    def generate_signal(self):

        signals = []

        for i in range(len(self.df)):

            buy = self.df["buy_percent"].iloc[i]
            sell = self.df["sell_percent"].iloc[i]
            surge = self.df["volume_surge"].iloc[i]

            if buy >= 60 and sell <= 40:
                signals.append("Strong Bullish" if surge > 2 else "Bullish")

            elif sell >= 60 and buy <= 40:
                signals.append("Strong Bearish" if surge > 2 else "Bearish")

            else:
                signals.append("Wrong Entry Possible")

        self.df["volume_signal"] = signals

        return self.df
```

### 3.9 Example Output

| Volume | Buy% | Sell% | Surge | Signal |
|---|---|---|---|---|
| 30000 | 68% | 32% | 2.3 | Strong Bullish |
| 22000 | 63% | 37% | 1.1 | Bullish |
| 18000 | 48% | 52% | 0.9 | Wrong Entry |
| 27000 | 35% | 65% | 2.5 | Strong Bearish |

### 3.10 Integration
```
Market Data → Indicator Engine (Supertrend + EMA + Pivot) → Volume Analyzer
→ Signal Confirmation → Strike Selection → Risk Management → Order Execution
```

### 3.11 Trading Decision Examples

**Bullish Trade:** Supertrend = Bullish, EMA Cross = Bullish, Volume Signal = Bullish → **BUY CALL OPTION**
**Bearish Trade:** Supertrend = Bearish, EMA Cross = Bearish, Volume Signal = Bearish → **BUY PUT OPTION**

> **Backlog idea (not yet specified):** "Liquidity Vacuum Detector" — detects when buyers or sellers completely disappear, a major driver of fast option moves.

---

## 4. Module 3 — Market Regime Detector

### 4.1 Objective
Measure overall market direction (breadth) by analyzing how many stocks are Advancing vs. Declining, then bias capital allocation toward CALL or PUT side. This replaces news/article sentiment analysis with actual market data.

### 4.2 Required Inputs
From the scanned market universe (e.g., 220 NSE F&O stocks), per stock:
`symbol, current_price, previous_close` (or `symbol, open, close`)

### 4.3 Step-Wise Logic

**Step 1 — Determine Stock Direction**
```
price_change = current_price - previous_close
if price_change > 0 → Advance
if price_change < 0 → Decline
if price_change ≈ 0 → Neutral
```

**Step 2 — Count Advances/Declines** (example: Total=220, Advance=140, Decline=60, Neutral=20)

**Step 3 — Breadth Ratio**
```
Breadth Ratio = Advance / Decline     # e.g. 140/60 = 2.33
```

| Breadth Ratio | Market Condition |
|---|---|
| > 1.5 | Bullish regime |
| 0.7 – 1.5 | Neutral / balanced |
| < 0.7 | Bearish regime |

**Step 4 — Advance/Decline Percentage**
```
Advance % = Advance / Total Stocks    # e.g. 140/220 = 63%
Decline % = Decline / Total Stocks    # e.g. 60/220 = 27%
```

### 4.4 Market Regime Rules

**Bullish:** `Advance % ≥ 60% OR Breadth Ratio ≥ 1.5` → allocate more capital to CALL options
**Bearish:** `Decline % ≥ 60% OR Breadth Ratio ≤ 0.7` → allocate more capital to PUT options
**Neutral:** `Advance % between 40–60%` → split capital between CALL and PUT

### 4.5 Capital Allocation Output

| Market Regime | Capital Allocation |
|---|---|
| Bullish | 70% CALL / 30% PUT |
| Neutral | 50% CALL / 50% PUT |
| Bearish | 30% CALL / 70% PUT |

### 4.6 Reference Implementation

```python
import pandas as pd


class MarketRegimeDetector:

    def __init__(self, df):
        """
        df must contain:
        symbol, current_price, previous_close
        """
        self.df = df.copy()

    def classify_stocks(self):

        self.df["price_change"] = (
            self.df["current_price"] - self.df["previous_close"]
        )

        self.df["direction"] = self.df["price_change"].apply(
            lambda x: "advance" if x > 0 else ("decline" if x < 0 else "neutral")
        )

        return self.df

    def calculate_breadth(self):

        advance = (self.df["direction"] == "advance").sum()
        decline = (self.df["direction"] == "decline").sum()
        neutral = (self.df["direction"] == "neutral").sum()

        total = len(self.df)

        advance_pct = advance / total
        decline_pct = decline / total

        breadth_ratio = advance / decline if decline != 0 else float("inf")

        return {
            "advance": advance,
            "decline": decline,
            "neutral": neutral,
            "advance_pct": advance_pct,
            "decline_pct": decline_pct,
            "breadth_ratio": breadth_ratio
        }

    def detect_regime(self, stats):

        if stats["advance_pct"] >= 0.60 or stats["breadth_ratio"] >= 1.5:
            regime = "Bullish"
            capital_bias = "70% CALL / 30% PUT"

        elif stats["decline_pct"] >= 0.60 or stats["breadth_ratio"] <= 0.7:
            regime = "Bearish"
            capital_bias = "30% CALL / 70% PUT"

        else:
            regime = "Neutral"
            capital_bias = "50% CALL / 50% PUT"

        return regime, capital_bias
```

### 4.7 Example Output
```
Advance = 150, Decline = 50, Neutral = 20
Advance % = 68%, Breadth Ratio = 3.0
→ Market Regime: Bullish
→ Capital Allocation: 70% CALL / 30% PUT
```

### 4.8 Integration
```
Market Data → Market Regime Detector → Capital Allocation Engine
→ Indicator Signals (Supertrend + EMA + Volume) → Strike Selection → Execution
```
The Market Regime Detector runs **first** to guide risk exposure.

---

## 5. Module 4 — Bid-Ask Analyzer

**Core concept:** The bid-ask spread is a real-time signal feed, not just a transaction cost. This module treats the order book as a sensor array. It has 7 sub-components feeding one composite score.

### 5.1 Spread & Liquidity Detection

```
Raw spread = Ask price − Bid price
Spread %   = Raw spread ÷ Mid price × 100
Liquidity score = Σ(bid sizes, top 5 levels) + Σ(ask sizes, top 5 levels),
                  normalized 0–100 against rolling 20-period average
```

**Stock thresholds:** `<0.05%` High liquidity · `0.05–0.20%` Moderate · `>0.20%` Thin/avoid
**Option thresholds:** Normalize against the option's own 10-day average spread (not a fixed value). `1.5×` average = caution. `2×` = exit territory.

> Example: AAPL bid/ask 185.40/185.43 → spread $0.03 (0.016%) → HIGH LIQUIDITY. Same day, a deep OTM call shows $0.05/$0.25 → 400% spread relative to mid → avoid entering.

### 5.2 Smart Money Entry Detection

| Signal | Definition |
|---|---|
| **A. Size anomaly** | A bid/ask level refreshes with size > 3× the average size at that level over the last 50 ticks ("wall"/"iceberg"). |
| **B. Absorption pattern** | Ask size stays constant/grows while price doesn't rise (hidden seller); flip side: bid absorbs repeated selling without price drop = accumulation. |
| **C. Sweep detection** | A single order lifts multiple ask levels in one tick (price jumps > 2 levels instantly) — aggressive institutional buying, unusually large volume. |
| **D. Time-and-sales clustering** | Multiple round-lot prints (500/1000/2500 shares) within 200ms at the same price — algorithmic accumulation signature. |

> Example: NVDA ask at $875.00 shows 12,000 shares. Over 30 ticks, 9,400 shares trade against it but the wall remains at 12,000 → hidden refresh; smart money distributing or large seller present.

### 5.3 Direction + Support/Resistance from Bid-Ask

```
Buy pressure  = Σ(trades hitting ask) × size
Sell pressure = Σ(trades hitting bid) × size
Net delta     = Buy pressure − Sell pressure
If net delta positive and rising → directional bias UP
```

- **Support level:** price where cumulative resting bid size > 2× average bid depth across all levels.
- **Resistance level:** price where cumulative resting ask size > 2× average ask depth.
- Levels update every tick. A support level that disappears suddenly (bids pulled) is itself a bearish signal.
- **Refresh rate signal:** faster-than-average refresh = active defender of a level; slow refresh = passive/low conviction.

> Example: SPY shows 50,000 resting bid shares stacked at $510.50 across 3 refresh cycles; price dips to $510.52 and bounces → real-time S/R level.

### 5.4 Order Flow for Strike Price Selection

1. Confirm stock direction via net delta (§5.3).
2. Scan option order flow for unusual activity:
   - `Volume/OI ratio > 2.0` on a strike = unusual interest.
   - Large call sweeps (multi-leg, multi-exchange fills) on ask side = directional call buying.
   - Put/call ratio on volume (not OI) dropping sharply = rising call demand.
3. Strike selection rules:
   - Bullish net delta + call sweep detected → prefer the strike where the sweep occurred.
   - Prefer strikes where option spread% is below 1.5× its 10-day average.
   - ATM is default; go one strike OTM only if `Vol/OI > 3.0`.
   - Avoid strikes where only one side (bid or ask) is being refreshed.

> Example: AAPL $185, bullish net delta, 1,000-contract call sweep hits $187.50 across 4 exchanges, Vol/OI → 4.2, spread $1.10/$1.20 (8.7%) → this is your strike. $190 call (Vol/OI 0.3, 35% spread) → skip.

### 5.5 Entry & Exit Based on Stock Bid-Ask

**Entry — all 5 must align:**

| Signal | Condition |
|---|---|
| Spread % | Below 20-period average |
| Bid imbalance | Bid size > Ask size by > 20% at top 3 levels |
| Net delta | Positive and accelerating (last 5 ticks all positive) |
| No ask wall | No resting ask > 3× average blocking next $0.10 |
| Last print | Most recent tape print hit the ask (buy) |

**Exit — any single condition triggers:**
- Bid imbalance flips: ask size > bid size by > 30% at top 3 levels
- Net delta negative for 3 consecutive ticks
- Large ask wall (> 3× average) appears at/above current price
- Spread % jumps > 1.5× its 20-period average
- Large sweep hits the bid side (aggressive selling)

### 5.6 Option Exit When Spread Widens & Liquidity Disappears

Staged early-warning system:

| Stage | Signal |
|---|---|
| 1 | Spread drift: option spread% rises above 1.2× session average — watch, not exit |
| 2 | Bid size shrinking below 50% of session average |
| 3 | Refresh rate slows (>40% drop in ticks/sec) |
| 4 | Bid pulls one or more strikes in a single tick without stock move |
| 5 | One-sided book: asks refreshing, bids thin/stale — no buyers |

**Exit rule:** If Stage 3 + Stage 4 occur together, exit immediately at market. Do not wait for Stage 5 (already trapped by then).

### 5.7 Bid-Ask Quantity Imbalance

```
Imbalance = (Total Bid Qty − Total Ask Qty) ÷ (Total Bid Qty + Total Ask Qty)
Range: −1.0 (pure ask pressure) to +1.0 (pure bid pressure)
Threshold: > +0.30 bullish imbalance, < −0.30 bearish imbalance
```

- **Depth-weighted imbalance:** Level 1 (closest to mid) weight ×5, Level 2 ×3, Level 3 ×1.
- **Imbalance persistence:** track rolling imbalance (EMA of per-tick imbalance); one snapshot means little — persistence across 10+ ticks is a real signal.
- **Imbalance flip:** crossing zero after sustaining one direction for > 10 ticks = high-conviction directional flip.
- **Spoofing filter:** large bid/ask appearing and disappearing within 2 ticks with no trades printing = spoof — exclude from imbalance calc. Only count orders persisting > 3 consecutive ticks.
- **Layering detection:** bid sizes increasing uniformly across all 5 levels simultaneously (not from trades) = artificial depth — discount imbalance signal by 50%.

> Example: NVDA book: 25,000 bid vs 8,000 ask (top 5) → raw imbalance = +0.52; persists 15 ticks → genuine institutional bid presence. If the 25,000 bid vanishes in one tick with no trades → spoofing, reverse the read.

### 5.8 Composite Bid-Ask Score

```
Composite = (Imbalance score × 0.25)
          + (Net delta direction × 0.20)
          + (Smart money flag × 0.20)
          + (Spread health × 0.15)
          + (SR proximity × 0.10)
          + (Option flow alignment × 0.10)

Threshold: > +0.60 → Entry.  < −0.40 → Exit.
```
The option-exit module (§5.6) runs independently and **overrides** the composite — liquidity loss always wins.

---

## 6. Module 5 — Open Interest Calculations

### 6.1 Objective
Analyze OI change + Price change + Volume to determine where positions are being built, whether traders are long/short, support/resistance, and overall market positioning.

**Outputs:** Long Buildup · Short Buildup · Short Covering · Long Unwinding · Support Level · Resistance Level · Market Positioning Bias

### 6.2 Required Inputs
From option chain + price data: `symbol, strike, price, volume, open_interest, previous_open_interest, previous_price`

### 6.3 Core Concept
OI = total open contracts. OI increase = new positions created; OI decrease = positions closing. Combine **Price change + OI change** (OI alone is insufficient).

### 6.4 OI Change
```
oi_change = current_oi - previous_oi
```
| OI Change | Meaning |
|---|---|
| Positive | New positions added |
| Negative | Positions closed |

### 6.5 Smart Money Entry Detection
```
If OI_change > 0 AND Volume > avg_volume → Smart Money Participation
```
Meaning: large traders entering the market.

### 6.6 Build-Up Type (Price Change + OI Change)

| Price | OI | Build-Up Type | Meaning |
|---|---|---|---|
| ↑ | ↑ | Long Build-Up | New long positions opened — **Bullish** |
| ↓ | ↑ | Short Build-Up | New short positions opened — **Bearish** |
| ↑ | ↓ | Short Covering | Shorts closing — **Bullish (short squeeze)** |
| ↓ | ↓ | Long Unwinding | Longs exiting — **Bearish** |

### 6.7 Support/Resistance from OI

- **High Call OI (CE):** traders selling calls → **Resistance**
- **High Put OI (PE):** traders selling puts → **Support**

> Example:

| Strike | Call OI | Put OI |
|---|---|---|
| 22400 | 180000 | 60000 |
| 22300 | 70000 | 160000 |

Interpretation: 22400 = resistance, 22300 = support.

### 6.8 Max Pain Calculation

Max Pain = the strike where option sellers lose the least money. For each strike, calculate total payout to option buyers; the strike with the lowest payout = Max Pain. Often acts as an expiry magnet.

### 6.9 Market Positioning Signal

| Signal | Interpretation |
|---|---|
| Bullish Positioning | Long buildup + strong put OI |
| Bearish Positioning | Short buildup + strong call OI |
| Neutral | Balanced OI |

### 6.10 Reference Implementation

```python
import pandas as pd
import numpy as np


class OpenInterestAnalyzer:

    def __init__(self, df):
        self.df = df.copy()

    def calculate_changes(self):

        self.df["oi_change"] = (
            self.df["open_interest"] - self.df["previous_open_interest"]
        )
        self.df["price_change"] = (
            self.df["price"] - self.df["previous_price"]
        )

        return self.df

    def detect_buildup(self):

        conditions = [
            (self.df["price_change"] > 0) & (self.df["oi_change"] > 0),
            (self.df["price_change"] < 0) & (self.df["oi_change"] > 0),
            (self.df["price_change"] > 0) & (self.df["oi_change"] < 0),
            (self.df["price_change"] < 0) & (self.df["oi_change"] < 0),
        ]

        choices = [
            "Long Buildup",
            "Short Buildup",
            "Short Covering",
            "Long Unwinding",
        ]

        self.df["oi_signal"] = np.select(conditions, choices, default="Neutral")

        return self.df


def calculate_max_pain(option_chain):

    strikes = option_chain["strike"].unique()
    pain = []

    for strike in strikes:

        total_loss = 0

        for _, row in option_chain.iterrows():

            if row["type"] == "CE":
                loss = max(0, strike - row["strike"]) * row["open_interest"]
            else:
                loss = max(0, row["strike"] - strike) * row["open_interest"]

            total_loss += loss

        pain.append((strike, total_loss))

    max_pain = min(pain, key=lambda x: x[1])[0]

    return max_pain
```

### 6.11 Example Output

| Price | OI Change | Signal |
|---|---|---|
| 22420 | +8000 | Long Buildup |
| 22410 | +9000 | Short Buildup |
| 22430 | -6000 | Short Covering |
| 22400 | -4000 | Long Unwinding |

### 6.12 Integration
```
Market Data → Market Regime Detector → Open Interest Analyzer
→ Indicator Engine (Supertrend + EMA + Volume) → Strike Selection Engine
→ Risk Management → Execution Engine
```
Helps determine: market bias, support/resistance, positioning strength.

### 6.13 Example Trading Scenario
Long buildup detected + Put OI strong + Market breadth bullish → **Prefer CALL options, select strike near resistance breakout.**

---

## 7. Module 6 — Greeks Calculator (Trend Phase Engine)

### 7.1 Objective
Identify market phase using Greeks + price behavior:
- **Accumulation** → No trade
- **Markup** → Enter trade
- **Distribution** → Exit trade

Avoids: early entry, late entry, holding during reversal.

### 7.2 Required Inputs
Option chain: `delta, gamma, theta, vega, iv, ltp, oi, volume`
Underlying: `price (close)`

### 7.3 Core Concept — What Each Greek Means

| Greek | Meaning | Use |
|---|---|---|
| Delta | Direction strength | Trend confirmation |
| Gamma | Speed of move | Breakout detection |
| Theta | Time decay | Exit timing |
| Vega | Volatility | Entry/exit timing |

### 7.4 Phase Detection Logic

**🟡 Phase 1 — Accumulation**
Characteristics: low price movement, low gamma, stable delta, low/moderate IV.
```
gamma < threshold_low AND abs(price_change) small AND iv stable
```
→ Big players building positions quietly. **Action: NO TRADE**

**🟢 Phase 2 — Markup (ENTRY ZONE)**
Characteristics: price breakout, delta rising, gamma increasing, IV expanding.
```
delta >= 0.5 AND delta <= 0.8
AND gamma increasing
AND iv rising
AND price > previous resistance
```
→ Trend started, momentum building. **Action: BUY CALL (bullish) / BUY PUT (bearish mirror logic)**

**🔴 Phase 3 — Distribution (EXIT ZONE)**
Characteristics: delta stops increasing or drops, gamma peaks/falls, IV peaks then drops, theta dominates.
```
delta decreasing
OR gamma falling
OR iv dropping sharply
OR theta increasing rapidly
```
→ Trend slowing, risk increasing, profit-booking phase. **Action: EXIT TRADE**

### 7.5 Reference Implementation

```python
import pandas as pd


class GreeksAnalyzer:

    def __init__(self, df):
        """
        df must contain:
        close, delta, gamma, theta, vega, iv
        """
        self.df = df.copy()

    def detect_phase(self):

        phases = []

        for i in range(1, len(self.df)):

            delta = self.df["delta"].iloc[i]
            gamma = self.df["gamma"].iloc[i]
            theta = self.df["theta"].iloc[i]
            iv = self.df["iv"].iloc[i]

            prev_gamma = self.df["gamma"].iloc[i - 1]
            prev_delta = self.df["delta"].iloc[i - 1]
            prev_iv = self.df["iv"].iloc[i - 1]

            if gamma < prev_gamma and abs(delta - prev_delta) < 0.05:
                phases.append("Accumulation")

            elif 0.5 <= delta <= 0.8 and gamma > prev_gamma and iv > prev_iv:
                phases.append("Markup")

            elif (
                delta < prev_delta
                or gamma < prev_gamma
                or iv < prev_iv
                or theta < -abs(theta) * 0.5
            ):
                phases.append("Distribution")

            else:
                phases.append("Neutral")

        phases.insert(0, "Neutral")

        self.df["market_phase"] = phases

        return self.df

    def generate_signal(self):

        signals = []

        for i in range(len(self.df)):

            phase = self.df["market_phase"].iloc[i]

            if phase == "Markup":
                signals.append("ENTER_TRADE")

            elif phase == "Distribution":
                signals.append("EXIT_TRADE")

            else:
                signals.append("NO_ACTION")

        self.df["trade_signal"] = signals

        return self.df
```

### 7.6 Example Output

| Delta | Gamma | IV | Phase | Action |
|---|---|---|---|---|
| 0.45 | 0.02 | 12 | Accumulation | No Trade |
| 0.62 | 0.05 | 14 | Markup | Enter |
| 0.78 | 0.08 | 18 | Markup | Hold |
| 0.75 | 0.06 | 17 | Distribution | Exit |

### 7.7 Integration
```
Market Breadth → OI Flow → Volume Imbalance → Greeks Analyzer (THIS MODULE)
→ Strike Selection → Execution
```

### 7.8 Final Combined Trading Logic

**Entry:** `Market Breadth = Bullish AND Volume Imbalance = Bullish AND OI Flow = Long Build-up AND Greeks Phase = Markup` → **BUY CALL**

**Exit:** `Greeks Phase = Distribution OR Volume Weakening` → **EXIT**

---

## 8. Module 7 — Liquidity Score (Option Chain)

Solves two problems:
1. Can I get into this trade at a decent price, and how much size can I take without moving the market against myself?
2. As price moves in my favor, at which levels should I peel off partial size while keeping the rest running?

### 8.1 Sub-Module A — Entry Size Calculator

**Input 1 — Open Interest (OI):** total open contracts; your liquidity reservoir.
> Rule of thumb: never hold more than 1–2% of OI in a single strike, or exiting will move price against you.
> Example: AAPL $185 Call OI = 8,000 → safe size = 80–160 contracts max.

**Input 2 — Daily Volume:** contracts traded today (active liquidity). Key ratio = **Vol/OI**:

| Vol/OI | Meaning |
|---|---|
| < 0.1 | Stale, low interest |
| 0.1 – 0.5 | Normal, liquid enough for retail size |
| 0.5 – 1.5 | Active, good liquidity, likely directional interest |
| > 1.5 | Hot strike — aggressive positioning, watch for reversals |
| > 3.0 | Unusual — sweep or news-driven; verify against stock direction |

**Input 3 — Expected OI Change:**
```
Expected new OI = Current OI + (Today's volume × Opening ratio)
```
- Price moving away from strike → most volume likely opening (new positions)
- Price moving toward expiry on that strike → most volume likely closing
- `Vol/OI > 1.0` → assume 60–70% opening
- `Vol/OI < 0.3` → assume ~60% closing

> Example: NVDA $880 Call, OI=5,000, today's volume=4,200 (Vol/OI=0.84), price moving toward $880, opening ratio ≈ 65% → expected new OI = 5,000 + (4,200 × 0.65) = 7,730. Liquidity growing — healthy for entry up to ~150 contracts today.

**Entry Size Formula**
```
Max Safe Entry = MIN(
  OI × 0.015,                        ← OI cap (1.5% of pond)
  Average daily volume × 0.05,       ← Volume cap (5% of today's flow)
  (Bid size at top 3 levels) × 10    ← Book depth cap
)
```

Confidence multiplier:

| Vol/OI ratio | Spread % vs average | Multiplier |
|---|---|---|
| > 1.0 | < 1.0× avg | 1.0 (full size) |
| 0.5 – 1.0 | 1.0–1.5× avg | 0.7 |
| 0.2 – 0.5 | 1.5–2.0× avg | 0.4 |
| < 0.2 | > 2.0× avg | 0.2 (tiny probe only) |

```
Final entry size = Max Safe Entry × Confidence multiplier
```

### 8.2 Sub-Module B — Partial Exit (Scale-Out) Price Levels

Exit in tranches as price moves favorably (locks profit, avoids giving back gains on reversals, exits while liquidity is still good). Use all three methods below and look for where they cluster.

**Method A — OI Cluster Levels:** rank strikes above your entry by OI; top 3 = natural scale-out targets (most liquidity to sell into).
> Example: Long AAPL $185 calls. $187.50 (OI 6,200) = Target 1 (exit 30%); $190.00 (OI 9,800, big wall) = Target 2 (exit 40%); $192.50 (OI 3,100) = Target 3 (trail stop on remaining 30%).

**Method B — Gamma Exposure (GEX) Levels:** proxy = strike where OI jumped > 20% overnight (gamma building there); acts as a speed bump. Scale out **before** price reaches a high-gamma strike, since liquidity compresses as you approach it.

**Method C — Liquidity Degradation Curve:** spread widens (%) as option moves deeper ITM / nearer expiry.

| Underlying move | Modeled spread multiplier |
|---|---|
| Entry | X% (baseline) |
| +10% | X% × 1.3 |
| +20% | X% × 1.8 |
| +30% | X% × 2.5–3.0 |

Rule: begin exiting when modeled spread at current price crosses **1.5×** entry spread.

Practical table ($2.00 ATM entry):

| Option price | Expected spread | Max exit size | Action |
|---|---|---|---|
| $2.00 (entry) | $0.10 (5%) | Full position | Hold |
| $3.50 (+75%) | $0.18 (5.1%) | Full position | Optional exit T1 |
| $5.00 (+150%) | $0.35 (7%) | 70% of position | Exit 30–40% |
| $7.50 (+275%) | $0.80 (10.7%) | 40% of position | Exit another 30% |
| $10.00+ (+400%) | $1.50–2.00 (15–20%) | Thin — 20% max | Trail stop on remainder |

**Scale-Out Decision Matrix** — exit a tranche when at least 2 of 5 conditions are met:

| # | Condition |
|---|---|
| 1 | Price approaching top-3 OI strike within 0.5% |
| 2 | Option spread% risen > 1.4× session average |
| 3 | OI at next strike > 2× current strike OI (wall ahead) |
| 4 | Vol/OI on current strike dropping (participants leaving) |
| 5 | Net delta on underlying flattening (momentum fading) |

```
2 of 5 checked → exit 25% of position
3 of 5 checked → exit 40% of position
4+ of 5 checked → exit 60–70%, trail a stop on remainder
```

### 8.3 Composite Liquidity Score (0–100)

```
Liquidity Score =
   Vol/OI normalized                (30 pts max)
 + Spread % vs average, inverted    (25 pts max)
 + OI rank in chain                 (20 pts max)
 + Expected OI expansion score      (15 pts max)
 + Book depth at top 3 levels       (10 pts max)
```

| Score | Band | Action |
|---|---|---|
| 75–100 | Green | Full entry size permitted; easy scale-out available |
| 50–74 | Yellow | Reduce entry size by 40%; scale-out in 2 tranches only |
| 25–49 | Orange | Probe size only (20% of normal); exit plan set before entry |
| 0–24 | Red | Do not enter; if already in, exit immediately regardless of P&L |

---

## 9. Module 8 — Expected Move Calculator + Trade Structuring Engine

> **Design principle:** keep prediction (Expected Move Engine) fully separate from execution (Trade Structuring Engine). Merging them makes the system hard to debug, optimize, and backtest.

### 9.1 Expected Move Engine (Pure Prediction)

**Objective — ONLY predict:** Direction, Magnitude, Confidence. **No** strike, stop loss, or risk decisions here.

**Inputs:**
- Indicator Score (-2 to +2)
- Volume Score (-2 to +2)
- Bid-Ask Score (-1 to +1)
- OI Score (-2 to +2)
- Greeks (Delta, Gamma, IV trend)
- Liquidity Zones (Vacuum / Resistance)

**Step 1 — Base Score**
```
total_score = indicator + volume + bidask + oi     # range: -7 to +7
```

**Step 2 — Normalize**
```
normalized_score = total_score / 7
```

**Step 3 — Momentum Multiplier (Greeks)**
```python
multiplier = 1
if gamma_trend == "up":
    multiplier += 0.4
if iv_trend == "up":
    multiplier += 0.3
if 0.5 <= delta <= 0.8:
    multiplier += 0.3
```

**Step 4 — Liquidity Adjustment**
```python
if vacuum_zone:
    multiplier += 0.4
if strong_resistance_nearby:
    multiplier -= 0.4
```

**Step 5 — Expected Move %**
```
expected_move_pct = normalized_score * multiplier * 2   # final range ≈ -3% to +3%
```

**Step 6 — Target Price**
```
expected_move = spot * expected_move_pct
target_price = spot + expected_move
```

**Step 7 — Confidence Score**
```
confidence = abs(total_score) * 10
           + (gamma_trend == "up") * 10
           + (volume_strong) * 10        # range: 0-100
```

**Final Output**
```json
{
    "expected_move_pct": 0.018,
    "target_price": 22950,
    "direction": "bullish",
    "confidence": 78,
    "move_quality": "strong"
}
```

### 9.2 Trade Structuring Engine (Execution Layer)

Consumes the Expected Move Engine's output.

**Inputs:** Expected Move Output, Greeks Data, Option Chain Data, Premium Data, Capital, Risk Rules

**1. Strike Selection**
```python
if move_pct > 1.5:
    strike_type = "ITM"
elif move_pct > 0.5:
    strike_type = "ATM"
else:
    strike_type = "NO TRADE"
```
Delta filter: `0.5 <= delta <= 0.7`
Output: Selected Strike, Option Type (CE/PE)

**2. Stop Loss Engine**
```python
# Fixed SL
sl_pct = 0.25  # 25% of premium

# Smart SL (better)
SL = min(0.25 * premium, below_structure_level)

# Trailing SL
if profit > 0.20:
    trail = 0.10
if gamma_turns_down:
    tighten_SL_aggressively()
```

**3. Position Sizing**
```python
risk_per_trade = capital * 0.01          # 1% risk-based sizing
qty = risk_per_trade / (premium * sl_pct)
```

**4. Trade Filters — Reject Trade If:**
```
confidence < 60
or expected_move < 0.5%
or delta > 0.85
or iv too high
```

**5. Final Output**
```json
{
    "trade": true,
    "strike": 22800,
    "type": "CE",
    "entry_price": 120,
    "stop_loss": 90,
    "trailing_sl": 108,
    "target_price": 200,
    "position_size": 150,
    "risk": 1500
}
```

### 9.3 Final Execution Flow

```
Core Signals
   ↓
Expected Move Engine     (Prediction Only)
   ↓
Trade Structuring Engine (Execution Decisions)
   ↓
Risk Layer Check
   ↓
Order Execution
```

**Why the separation matters:** without it, the system is hard to debug, cannot be backtested properly, and cannot be improved incrementally. With it: prediction can be improved independently, risk can be optimized separately, and an ML layer can be plugged in later.

**Next planned module (not yet specified in detail):** **Probability Engine** (learning layer) — learns from 3 months of historical data, auto-adjusts score weights, improves expected-move accuracy over time.

---

## 10. Build Notes for the Coding Agent

- Modules 1–8 above are fully specified — implement these first, in order, since each later module consumes earlier module outputs.
- Modules 9–23 (Greeks Change Predictor through Accumulation Phase Detector) and the 11 add-ons are named only in the index — **do not invent their logic**. Flag them as backlog/TODO and confirm detailed specs before implementing.
- The Broker Bridge (AngelOne SmartAPI) is a required but unspecified infrastructure module — needs order placement, order status polling, rejection handling, and lot size/freeze quantity enforcement for NSE options.
- Keep prediction and execution logic in separate modules/files throughout the codebase (see §9), not just for the Expected Move Engine — this pattern should extend to Indicator Signals vs. Strike Selector, Greeks Analyzer vs. Trade Structuring, etc.
- Follow the folder structure in §1 for all new modules.