#!/usr/bin/env bash
set -euo pipefail

BASE="/home/jagruti/Documents/smartapi/angel_md_redis"
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

# 2) Greeks: REST -> Redis (needs md:active_expiry from producer)
start "greeks" python3 run_greeks_only.py

# 3) Joiner: opt ticks + latest greeks -> features stream
start "joiner" python3 run_joiner.py

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

# 3e2j) OI analysis: per-contract long/short buildup classification (Steps 1-4 only, see notes)
start "oi_analysis" python3 run_oi_analysis.py

# 3f) Final entry: HTF + ST + EMA + Volume + Pivot/R1/S1 break -> BUY CALL / BUY PUT
start "entry_trigger" python3 run_entry_trigger.py



# 3g) Strike select: entry signal -> ATM / slight-OTM option contract
start "strike_select" python3 run_strike_select.py

# 3h) Capital alloc: regime bias + strike -> sized CALL/PUT notional
start "capital_alloc" python3 run_capital_alloc.py

# 4) Archivers: Redis streams -> data_lake/stream=.../dt=YYYY-MM-DD/...
start "arch_eq"       python3 run_archiver_all.py eq
start "arch_opt"      python3 run_archiver_all.py opt
start "arch_greeks"   python3 run_archiver_all.py greeks
start "arch_features" python3 run_archiver_all.py features

# 4b) Candle archivers: Redis candle streams -> angel_md_data_lake/stream=.../dt=YYYY-MM-DD/...
start "arch_candles_1m"  python3 run_archiver_candles_1m.py
start "arch_candles_5m"  python3 run_archiver_candles_5m.py
start "arch_candles_10m" python3 run_archiver_candles_10m.py
start "arch_candles_30m" python3 run_archiver_candles_30m.py
start "arch_candles_1d"  python3 run_archiver_candles_1d.py

# 5) Signal archivers (CSV): regime + volume signals -> data_lake/stream=.../dt=YYYY-MM-DD/part-*.csv
start "arch_regime_csv" python3 run_archiver_signals_csv.py regime
start "arch_volume_csv" python3 run_archiver_signals_csv.py volume

echo
echo "All started."
echo "Logs: $LOGDIR"
echo "PIDs: $PIDDIR"
echo "To stop everything:"
echo "  kill \$(cat $PIDDIR/*.pid)"