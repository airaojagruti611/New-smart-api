#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE"

# venv
source "$BASE/venv/bin/activate"

# timezone for dt=YYYY-MM-DD folders (optional)
export ARCHIVE_TZ="Asia/Kolkata"

# better logs
export PYTHONUNBUFFERED=1
# DEBUG = every skip/input/output; INFO = decisions + emits (default)
export LOG_LEVEL="${LOG_LEVEL:-DEBUG}"
# FileHandler off: nohup already appends stdout to logs/<date>/<name>.log
export LOG_TO_FILE="${LOG_TO_FILE:-0}"
export LOG_DIR="${LOG_DIR:-$BASE/logs}"

# start redis
docker compose up -d

DAY="$(date +%F)"
LOGDIR="$BASE/logs/$DAY"
PIDDIR="$LOGDIR/pids"
mkdir -p "$PIDDIR"

start() {
  local name="$1"; shift
  echo "Starting $name ..."
  nohup "$@" >> "$LOGDIR/$name.log" 2>&1 &
  echo $! > "$PIDDIR/$name.pid"
}

# 1) Producer: WS -> Redis (eq + opt ticks)
start "producer" python3 run_producer.py

# Angel rate-limits concurrent generateSession (same TOTP).
# Greeks waits on md:active_expiry anyway — delay is safe.
echo "Waiting 8s for producer login before greeks..."
sleep 8

# 2) Greeks: REST -> Redis (needs md:active_expiry from producer)
start "greeks" python3 run_greeks_only.py

# 3) Joiner: opt ticks + latest greeks -> features stream
start "joiner" python3 run_joiner.py

# 3a2) Greeks phase engine: Accumulation/Markup/Distribution per contract
start "greeks_phase" python3 run_greeks_analyzer.py

# 3b) Candles: ticks -> 1m/1d, then resample -> 5m/10m/30m
start "candles_pub" python3 run_candles_publisher.py
start "candles_rs"  python3 run_candles_resampler.py

# 3b2) Daily pivots: prev-day H/L/C -> md:pivots:prevday:{SYMBOL}
start "pivots" python3 run_daily_pivots.py

# 3b2b) HTF trend: Chartink D/W/M close > prev -> CALL/PUT/NEUTRAL
start "htf_trend" python3 run_htf_trend_filter.py

# 3b3) Level entry: 1m close breaks P/R1/R2 or S1/S2 -> BUY CALL / BUY PUT
start "level_entry" python3 run_level_entry.py

# 3c) Supertrend bias: multi-timeframe trend filter (CALL/PUT/NEUTRAL)
start "st_bias" python3 run_supertrend_mtf_bias.py

# 3d) EMA cross: momentum confirmation (EMA9/EMA26)
start "ema_cross" python3 run_ema_cross.py

# 3e) Momentum confirm: Supertrend AND EMA9/26 -> BUY CALL / BUY PUT
start "momentum" python3 run_momentum_confirm.py

# 3e2) Volume analyzer: 1m buyer/seller dominance -> Bullish/Bearish Volume
start "volume" python3 run_volume_analyzer.py

# 3e3) Market regime: advance/decline breadth -> CALL/PUT capital bias
start "regime" python3 run_market_regime.py

# 3e2b) Bid-ask intelligence: spread/liquidity signals (eq + opt)
start "bidask" python3 run_bidask_analyzer.py

# 3e2c) Smart money detection: wall/absorption/sweep/cluster signals (eq + opt)
start "smartmoney" python3 run_smart_money.py

# 3e2d) Order flow: direction + support/resistance from bid-ask (eq + opt)
start "orderflow" python3 run_order_flow.py

# 3e2e) Strike flow: order-flow-driven strike selection (synthesizes bidask+smartmoney+orderflow)
start "strikeflow" python3 run_strike_flow.py

# 3e2f) Stock entry/exit: bid-ask-driven entry/exit trigger (stock only)
start "stockflow" python3 run_stock_entry_exit.py

# 3e2g) Option liquidity exit: spread/liquidity withdrawal protection (opt only)
start "optexit" python3 run_option_liquidity_exit.py

# 3e2h) Bid-ask quantity imbalance: raw/weighted/persistent imbalance signal (eq + opt)
start "imbalance" python3 run_bidask_imbalance.py

# 3e2i) Composite score: synthesizes imbalance+orderflow+smartmoney+bidask+strikeflow+optexit
start "composite" python3 run_composite.py

# 3e2i2) Liquidity score: entry sizing + scale-out levels per contract
start "liquidity_score" python3 run_liquidity_score.py

# 3e2j) OI analysis: per-contract long/short buildup classification (Steps 1-4 only, see notes)
start "oi_analysis" python3 run_oi_analysis.py

# 3f) Final entry: HTF + ST + EMA + Volume + Pivot/R1/S1 break -> BUY CALL / BUY PUT
start "entry_trigger" python3 run_entry_trigger.py



# 3g) Strike select: entry signal -> ATM / slight-OTM option contract
start "strike_select" python3 run_strike_select.py

# 3h) Capital alloc: regime bias + strike -> sized CALL/PUT notional
start "capital_alloc" python3 run_capital_alloc.py

# 3i) Expected move (Module 8) + Greeks change (Module 9)
start "expected_move" python3 run_expected_move.py
start "greeks_change" python3 run_greeks_change.py

# 4) One parquet archiver: Angel One + all layer streams -> data_lake/stream=.../dt=YYYY-MM-DD/...
start "arch_layers" python3 run_archiver_layers.py all

# 5) Live Streamlit UI (http://127.0.0.1:8501) — set START_DASHBOARD=0 to skip
if [[ "${START_DASHBOARD:-1}" != "0" ]]; then
  start "dashboard" streamlit run streamlit_app.py \
    --server.port "${DASHBOARD_PORT:-8501}" \
    --server.address "${DASHBOARD_ADDR:-0.0.0.0}" \
    --server.headless true \
    --browser.gatherUsageStats false
fi

echo
echo "All started."
echo "Logs: $LOGDIR"
echo "PIDs: $PIDDIR"
echo "Dashboard: http://127.0.0.1:${DASHBOARD_PORT:-8501}"
echo "To stop everything:"
echo "  kill \$(cat $PIDDIR/*.pid)"
echo "Client Excel (any time while Redis is up):"
echo "  ./export_client_excel.sh"
echo "  python3 export_client_excel.py --symbols RELIANCE,TCS,INFY"