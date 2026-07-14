## Angel One Market Data → Redis Streams (WS + REST Greeks)

### 1) Start Redis
docker compose up -d

### 2) Setup python venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

### 3) Configure
- Copy .env.example to .env and fill values
- Put all your symbols in symbols.txt (one per line)

### 4) Run producer (WebSocket → Redis)
python run_producer.py

Streams written:
- md:ticks:eq
- md:ticks:opt

### 5) Run greeks poller (REST → Redis)
Option A (simple): run combined WS+greeks:
python run_greeks.py

Writes:
- md:greeks:snap
and cache key:
- md:greeks:latest:{UNDERLYING}:{EXPIRY_ISO}

### 6) Run joiner (ticks + latest greeks → training stream)
python run_joiner.py

Writes:
- md:features:opt

### 7) Run 1-minute market regime detector
python run_market_regime.py

Writes:
- md:regime
- md:regime:latest

### 7b) Archive regime stream to CSV (date-partitioned)
python run_archiver_signals_csv.py regime

Writes (CSV parts):
- data_lake/stream=md_regime/dt=YYYY-MM-DD/part-*.csv

### 8) Run API endpoint (latest 1-minute market data)
python run_api.py

Endpoint:
- GET /market-data/1m
- GET /market-data/volume
- WS  /ws/market  (streams latest regime + volume snapshots)
- GET /scan/step1 (scanner step-1, defaults)
- GET /scan/step2 (scanner step-2, defaults)


docker compose -f docker-compose.yml up -d

### Volume analyzer (1-minute) + CSV archive
Run volume analyzer:
python run_volume_analyzer.py

Archive confirmed 1-minute signals to CSV:
python run_archiver_signals_csv.py volume

Writes (CSV parts):
- data_lake/stream=md_volume_signal/dt=YYYY-MM-DD/symbol=.../part-*.csv
