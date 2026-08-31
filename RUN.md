# How to Run — Angel One Market Data Pipeline

Step-by-step guide for `smartapi_new/angel_md_redis`.

The launcher starts every worker through **Module 9 (Greeks Change)** and one parquet archiver that writes Angel One ticks **and** all layer outputs into `data_lake/` (same folder layout as Angel One data).

---

## Prerequisites

- Python 3.10+
- Docker Desktop (for Redis)
- Angel One SmartAPI credentials (API key, client code, PIN, TOTP secret)

---

## Step 1 — Go to the project folder

```powershell
cd c:\Users\Dell\Downloads\smartapi-angelone-main\smartapi-angelone-main\smartapi_new\angel_md_redis
```

---

## Step 2 — Start Redis

```powershell
docker compose up -d
```

Redis listens on `localhost:6379`.

Check it:

```powershell
docker ps
```

You should see container `md_redis`.

---

## Step 3 — Create a Python virtual environment

**Windows (PowerShell):**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Linux / macOS:**

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Step 4 — Configure `.env`

Create a file named `.env` in this folder with:

```env
ANGEL_API_KEY=your_api_key
ANGEL_CLIENT_CODE=your_client_code
ANGEL_PIN=your_pin
ANGEL_TOTP_SECRET=your_totp_secret

REDIS_URL=redis://localhost:6379/0

# Optional
X_CLIENT_LOCAL_IP=127.0.0.1
X_CLIENT_PUBLIC_IP=
X_MAC_ADDRESS=
STRIKES_AROUND=0
SUBSCRIBE_MODE=SNAP_QUOTE
LOG_LEVEL=INFO
ARCHIVE_TZ=Asia/Kolkata
```

Never commit `.env` (it is gitignored).

`ARCHIVE_TZ` controls the `dt=YYYY-MM-DD` folder on disk (default UTC if unset). `run_all.ps1` / `run_all.sh` set it to `Asia/Kolkata`.

---

## Step 5 — Set symbols

Edit `symbols.txt` — one NSE symbol per line, for example:

```
RELIANCE
TCS
INFY
```

---

## Step 6 — Run the pipeline

### Option A — All services at once (Windows, recommended)

`run_all.ps1` starts Redis, every worker through Greeks Change, and the parquet archiver.

```powershell
.\run_all.ps1
```

Logs go to `logs\YYYY-MM-DD\`.  
Stop everything:

```powershell
.\stop_all.ps1
```

Restart only after `stop_all.ps1`. Do not start the old per-stream archivers (`run_archiver_all.py`, `run_archiver_candles_*.py`, `run_archiver_signals_csv.py`) at the same time — they share the same Redis consumer group as `run_archiver_layers.py`.

### Option B — All services at once (Linux / Git Bash / WSL)

`run_all.sh` starts Redis + every worker with `nohup`.

```bash
chmod +x run_all.sh
./run_all.sh
```

Logs go to `logs/YYYY-MM-DD/`.  
Stop everything:

```bash
kill $(cat logs/$(date +%F)/pids/*.pid)
```

### Option C — Manual (separate terminals)

Open a separate terminal for each process (venv activated in each). Start **producer first**.

#### Core market data (required first)

| Order | Command | What it does |
|------|---------|--------------|
| 1 | `python run_producer.py` | WebSocket ticks → `md:ticks:eq`, `md:ticks:opt` |
| 2 | `python run_greeks_only.py` | REST greeks → `md:greeks:snap` |
| 3 | `python run_joiner.py` | Joins option ticks + greeks → `md:features:opt` |
| 4 | `python run_greeks_analyzer.py` | Greeks phase → `md:greeks:phase:signal` |

#### Candles & pivots

| Order | Command | What it does |
|------|---------|--------------|
| 5 | `python run_candles_publisher.py` | Builds 1m / 1d candles |
| 6 | `python run_candles_resampler.py` | Resamples → 5m / 10m / 30m |
| 7 | `python run_daily_pivots.py` | Prev-day pivots → key `md:pivots:prevday:{SYMBOL}` + stream `md:pivots:prevday` |

#### Modules 1–7 (signals / microstructure)

| Order | Command | What it does |
|------|---------|--------------|
| 8 | `python run_htf_trend_filter.py` | HTF D/W/M trend → CALL / PUT / NEUTRAL |
| 9 | `python run_level_entry.py` | Pivot / R1 / S1 break → level entry |
| 10 | `python run_supertrend_mtf_bias.py` | Multi-TF Supertrend bias |
| 11 | `python run_ema_cross.py` | EMA9 / EMA26 cross |
| 12 | `python run_momentum_confirm.py` | Supertrend + EMA confirm |
| 13 | `python run_volume_analyzer.py` | 1m buyer/seller volume signal |
| 14 | `python run_market_regime.py` | Advance/decline regime |
| 15 | `python run_bidask_analyzer.py` | Bid-ask spread / liquidity |
| 16 | `python run_smart_money.py` | Smart-money signals |
| 17 | `python run_order_flow.py` | Order-flow direction + S/R |
| 18 | `python run_strike_flow.py` | Strike-level option flow |
| 19 | `python run_stock_entry_exit.py` | Stock bid-ask entry/exit |
| 20 | `python run_option_liquidity_exit.py` | Option liquidity exit |
| 21 | `python run_bidask_imbalance.py` | Bid-ask quantity imbalance |
| 22 | `python run_composite.py` | Composite bid-ask score |
| 23 | `python run_oi_analysis.py` | Open-interest buildup |
| 24 | `python run_liquidity_score.py` | Option liquidity 0–100 |

#### Modules 8–9 + strike inputs

| Order | Command | What it does |
|------|---------|--------------|
| 25 | `python run_entry_trigger.py` | Final CALL/PUT gate → `md:entry:trigger` |
| 26 | `python run_strike_select.py` | ATM / slight-OTM contract → `md:strike:select` |
| 27 | `python run_expected_move.py` | Module 8 expected move → `md:expected_move:signal` |
| 28 | `python run_greeks_change.py` | Module 9 greeks change → `md:greeks_change:signal` |

`run_capital_alloc.py` is also started by `run_all` (sizing). It is **not** archived (layers stop at Greeks Change).

#### Optional: API

```powershell
python run_api.py
```

- `GET /market-data/1m`
- `GET /market-data/volume`
- `WS /ws/market`
- `GET /scan/step1`
- `GET /scan/step2`

#### Parquet archiver (Redis → `data_lake/`)

One process archives Angel One ticks/greeks/candles **and** every layer through Greeks Change:

```powershell
python run_archiver_layers.py all
```

List streams:

```powershell
python run_archiver_layers.py list
```

Archive one stream only (example):

```powershell
python run_archiver_layers.py greeks_change
```

Do **not** also start `run_archiver_all.py`, `run_archiver_candles_*.py`, or `run_archiver_signals_csv.py`.

---

## Data lake layout

All parquet lands in `angel_md_redis/data_lake/` (same folder as Angel One ticks):

```
data_lake/
  stream=md_ticks_eq/dt=YYYY-MM-DD/symbol=.../part-*.parquet
  stream=md_greeks_snap/dt=YYYY-MM-DD/...
  stream=md_candles_1m/dt=YYYY-MM-DD/symbol=.../part-*.parquet
  stream=md_supertrend_bias/dt=YYYY-MM-DD/symbol=.../part-*.parquet
  stream=md_volume_signal/dt=YYYY-MM-DD/symbol=.../part-*.parquet
  stream=md_expected_move_signal/dt=YYYY-MM-DD/symbol=.../part-*.parquet
  stream=md_greeks_change_signal/dt=YYYY-MM-DD/symbol=.../part-*.parquet
  ...
```

Partitioning is by **calendar date** (`dt=`), not by process restart. Folders appear after workers emit and the archiver flushes (about every 10 seconds).

---

## Step 7 — Verify it’s working

1. Producer logs show login + websocket subscriptions.
2. Redis has streams (example with `redis-cli`):

```powershell
docker exec -it md_redis redis-cli
```

```
XLEN md:ticks:eq
XLEN md:candles:1m
XLEN md:entry:trigger
XLEN md:expected_move:signal
XLEN md:greeks_change:signal
KEYS md:greeks_change:latest:*
```

3. Check process logs under `logs/YYYY-MM-DD/` (`producer.log`, `greeks_change.log`, `arch_layers.log`).
4. Confirm parquet under `data_lake/stream=md_greeks_change_signal/`.

---

## Pipeline order (dependency map)

```
producer ──► greeks ──► joiner ──► greeks_analyzer
    │
    └──► candles_publisher ──► candles_resampler
                │
                ├──► daily_pivots ──► level_entry ──┐
                ├──► htf_trend_filter ──────────────┤
                ├──► supertrend_mtf_bias ───────────┤
                ├──► ema_cross ──► momentum_confirm ┤
                └──► volume_analyzer / regime ───────┤
                                                   ▼
                                            entry_trigger
                                                   │
                                                   ▼
                                            strike_select
                                                   │
                         expected_move (8) ───────┤
                                                   ▼
                                            greeks_change (9)
                                                   │
                                                   ▼
                                            data_lake/*.parquet
```

Bid-ask / OI / liquidity workers also run in parallel off ticks and feed Modules 7–9.

Start **producer first**; signal workers need candles/ticks flowing (market hours).

---

## Stop services

- Windows launcher: `.\stop_all.ps1`
- Manual: `Ctrl+C` in each terminal
- Redis: `docker compose down`
- `run_all.sh`: kill PIDs as shown in Step 6B

---

## Common issues

| Problem | Fix |
|--------|-----|
| Missing Angel env vars | Fill all four keys in `.env` |
| `symbols.txt not found` | Run from `angel_md_redis` folder |
| Redis connection refused | `docker compose up -d` |
| Empty signals | Wait for market hours / enough candle history |
| No `data_lake` parquet | Confirm `arch_layers` is running; wait for a flush; do not run old archivers in parallel |
| Empty `md:greeks_change:signal` | Needs `strike_select` OK events **and** `run_expected_move.py` |
| Duplicate / missing archive files | Stop old `arch_eq` / candle / CSV processes, then restart with `run_all` |
