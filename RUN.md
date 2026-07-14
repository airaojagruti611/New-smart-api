# How to Run — Angel One Market Data Pipeline

Step-by-step guide for `smartapi_new/angel_md_redis`.

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
```

Never commit `.env` (it is gitignored).

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

### Option A — All services at once (Linux / Git Bash / WSL)

`run_all.sh` starts Redis + every worker with `nohup`.

**Important:** Edit the `BASE=...` path near the top of `run_all.sh` to your machine path before running.

```bash
chmod +x run_all.sh
./run_all.sh
```

Logs go to `logs/YYYY-MM-DD/`.  
Stop everything:

```bash
kill $(cat logs/$(date +%F)/pids/*.pid)
```

### Option B — Manual (recommended on Windows)

Open a separate terminal for each process (venv activated in each).

#### Core market data (required first)

| Order | Command | What it does |
|------|---------|--------------|
| 1 | `python run_producer.py` | WebSocket ticks → `md:ticks:eq`, `md:ticks:opt` |
| 2 | `python run_greeks_only.py` | REST greeks → Redis |
| 3 | `python run_joiner.py` | Joins option ticks + greeks → `md:features:opt` |

#### Candles & pivots

| Order | Command | What it does |
|------|---------|--------------|
| 4 | `python run_candles_publisher.py` | Builds 1m / 1d candles |
| 5 | `python run_candles_resampler.py` | Resamples → 5m / 10m / 30m |
| 6 | `python run_daily_pivots.py` | Prev-day pivots → `md:pivots:prevday:{SYMBOL}` |

#### Signal / strategy workers

| Order | Command | What it does |
|------|---------|--------------|
| 7 | `python run_htf_trend_filter.py` | HTF D/W/M trend → CALL / PUT / NEUTRAL |
| 8 | `python run_level_entry.py` | Pivot / R1 / S1 break → level entry |
| 9 | `python run_supertrend_mtf_bias.py` | Multi-TF Supertrend bias |
| 10 | `python run_ema_cross.py` | EMA9 / EMA26 cross |
| 11 | `python run_momentum_confirm.py` | Supertrend + EMA confirm |
| 12 | `python run_entry_trigger.py` | Final entry: HTF + ST + EMA + level |
| 13 | `python run_strike_select.py` | Picks ATM / slight-OTM strike |

#### Optional: regime, volume, API

```powershell
python run_market_regime.py
python run_volume_analyzer.py
python run_api.py
```

API (when `run_api.py` is up):

- `GET /market-data/1m`
- `GET /market-data/volume`
- `WS /ws/market`
- `GET /scan/step1`
- `GET /scan/step2`

#### Optional: archivers (Redis → disk)

```powershell
python run_archiver_all.py eq
python run_archiver_all.py opt
python run_archiver_all.py greeks
python run_archiver_all.py features

python run_archiver_candles_1m.py
python run_archiver_candles_5m.py
python run_archiver_candles_10m.py
python run_archiver_candles_30m.py
python run_archiver_candles_1d.py

python run_archiver_signals_csv.py regime
python run_archiver_signals_csv.py volume
```

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
KEYS md:entry:trigger:latest:*
```

3. Check process logs under `logs/` if file logging is enabled.

---

## Pipeline order (dependency map)

```
producer ──► greeks ──► joiner
    │
    └──► candles_publisher ──► candles_resampler
                │
                ├──► daily_pivots ──► level_entry ──┐
                ├──► htf_trend_filter ──────────────┤
                ├──► supertrend_mtf_bias ───────────┤
                └──► ema_cross ──► momentum_confirm ┤
                                                   ▼
                                            entry_trigger
                                                   │
                                                   ▼
                                            strike_select
```

Start **producer first**; signal workers need candles/ticks flowing.

---

## Stop services

- Manual: `Ctrl+C` in each terminal.
- Redis: `docker compose down`
- `run_all.sh`: kill PIDs as shown in Step 6A.

---

## Common issues

| Problem | Fix |
|--------|-----|
| Missing Angel env vars | Fill all four keys in `.env` |
| `symbols.txt not found` | Run from `angel_md_redis` folder |
| Redis connection refused | `docker compose up -d` |
| Empty signals | Wait for market hours / enough candle history |
| `run_all.sh` path wrong | Update `BASE=` at top of the script |
```
