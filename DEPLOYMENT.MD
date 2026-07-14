# Deploy on AWS EC2 — Angel One Market Data Pipeline

Guide to run `smartapi_new/angel_md_redis` on an Ubuntu EC2 instance.

Local run steps are in [RUN.md](RUN.md). This file covers production-style deploy on EC2.

---

## 1. Choose an EC2 instance

| Setting | Recommendation |
|--------|----------------|
| AMI | Ubuntu 22.04 LTS |
| Instance type | `t3.medium` (2 vCPU, 4 GB) minimum; `t3.large` if many symbols + archivers |
| Storage | 30–50 GB gp3 (logs + data lake grow over time) |
| Region | Any; prefer one with stable outbound HTTPS |
| Key pair | Create/download a `.pem` for SSH |

**Security group (inbound):**

| Port | Source | Why |
|------|--------|-----|
| 22 | Your IP only | SSH |
| 8000 | Your IP / VPN only | Optional FastAPI (`run_api.py`) |

Do **not** open Redis `6379` to the internet. Keep Redis on localhost.

Outbound must allow HTTPS (443) for Angel SmartAPI WebSocket + REST.

---

## 2. SSH into the instance

```bash
chmod 400 your-key.pem
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
```

---

## 3. Install system packages

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip git curl ca-certificates

# Docker
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu
newgrp docker

# Docker Compose plugin (usually included with Docker Engine)
docker compose version
```

Set timezone to India (market hours / log folders):

```bash
sudo timedatectl set-timezone Asia/Kolkata
```

---

## 4. Put the code on the server

**Option A — Git clone**

```bash
cd ~
git clone <YOUR_REPO_URL> smartapi-angelone
cd smartapi-angelone/smartapi_new/angel_md_redis
```

**Option B — Upload from your PC (scp)**

```bash
# from your Windows machine (PowerShell / Git Bash)
scp -i your-key.pem -r "c:\Users\Dell\Downloads\smartapi-angelone-main\smartapi-angelone-main" ubuntu@<EC2_PUBLIC_IP>:~/
```

Then on EC2:

```bash
cd ~/smartapi-angelone-main/smartapi_new/angel_md_redis
# or whatever path you uploaded to
```

---

## 5. Python venv + dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 6. Configure `.env`

```bash
nano .env
```

```env
ANGEL_API_KEY=your_api_key
ANGEL_CLIENT_CODE=your_client_code
ANGEL_PIN=your_pin
ANGEL_TOTP_SECRET=your_totp_secret

REDIS_URL=redis://localhost:6379/0

# Optional — fill with EC2 network details if Angel requires them
X_CLIENT_LOCAL_IP=
X_CLIENT_PUBLIC_IP=
X_MAC_ADDRESS=

STRIKES_AROUND=0
SUBSCRIBE_MODE=SNAP_QUOTE
LOG_LEVEL=INFO

# If you expose the API
API_HOST=0.0.0.0
API_PORT=8000
```

```bash
chmod 600 .env
```

Never commit `.env` (it is gitignored).

Edit `symbols.txt` — one NSE symbol per line, for example:

```
RELIANCE
TCS
INFY
```

---

## 7. Start Redis

```bash
docker compose up -d
docker ps   # expect container md_redis
```

Redis listens on `localhost:6379`.

---

## 8. Fix `run_all.sh` and start the pipeline

Edit the `BASE=` line at the top of `run_all.sh` to the EC2 path, for example:

```bash
BASE="/home/ubuntu/smartapi-angelone/smartapi_new/angel_md_redis"
```

Then:

```bash
chmod +x run_all.sh
./run_all.sh
```

Logs go to `logs/YYYY-MM-DD/`.  
PIDs go to `logs/YYYY-MM-DD/pids/`.

Stop everything:

```bash
kill $(cat logs/$(date +%F)/pids/*.pid)
```

### Optional: API

```bash
source venv/bin/activate
mkdir -p logs/$(date +%F)
nohup python3 run_api.py >> logs/$(date +%F)/api.log 2>&1 &
```

Hit: `http://<EC2_PUBLIC_IP>:8000/market-data/1m` (only if the security group allows port 8000).

API endpoints:

- `GET /market-data/1m`
- `GET /market-data/volume`
- `WS /ws/market`
- `GET /scan/step1`
- `GET /scan/step2`

---

## 9. Survive reboot (recommended)

Enable Docker on boot:

```bash
sudo systemctl enable docker
```

Example systemd unit for the pipeline (`/etc/systemd/system/angel-md.service`):

```ini
[Unit]
Description=Angel MD Redis Pipeline
After=docker.service network-online.target
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
User=ubuntu
WorkingDirectory=/home/ubuntu/smartapi-angelone/smartapi_new/angel_md_redis
ExecStart=/home/ubuntu/smartapi-angelone/smartapi_new/angel_md_redis/run_all.sh
ExecStop=/bin/bash -c 'kill $(cat /home/ubuntu/smartapi-angelone/smartapi_new/angel_md_redis/logs/$(date +%%F)/pids/*.pid) || true'

[Install]
WantedBy=multi-user.target
```

Adjust `WorkingDirectory` / paths to match your deploy folder, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable angel-md
sudo systemctl start angel-md
```

---

## 10. Verify

```bash
# processes
ps aux | grep -E 'run_producer|run_entry_trigger|run_strike' | grep -v grep

# redis streams
docker exec -it md_redis redis-cli
```

Inside redis-cli:

```
XLEN md:ticks:eq
XLEN md:candles:1m
XLEN md:entry:trigger
KEYS md:entry:trigger:latest:*
```

Check logs:

```bash
tail -f logs/$(date +%F)/producer.log
tail -f logs/$(date +%F)/entry_trigger.log
```

---

## 11. Ops checklist

| Task | Tip |
|------|-----|
| Disk growth | Monitor `data_lake/`, `angel_md_data_lake/`, `logs/` |
| Restart pipeline | Kill PIDs, then `./run_all.sh` |
| Update code | `git pull`, reinstall deps if needed, restart |
| Secrets | Never commit `.env`; use `chmod 600` |
| Elastic IP | Attach one so the public IP does not change on stop/start |

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

Start **producer first**; signal workers need candles/ticks flowing (market hours).

---

## Manual start order (if not using `run_all.sh`)

Activate venv in each session, then start in this order:

| Order | Command |
|------|---------|
| 1 | `python3 run_producer.py` |
| 2 | `python3 run_greeks_only.py` |
| 3 | `python3 run_joiner.py` |
| 4 | `python3 run_candles_publisher.py` |
| 5 | `python3 run_candles_resampler.py` |
| 6 | `python3 run_daily_pivots.py` |
| 7 | `python3 run_htf_trend_filter.py` |
| 8 | `python3 run_level_entry.py` |
| 9 | `python3 run_supertrend_mtf_bias.py` |
| 10 | `python3 run_ema_cross.py` |
| 11 | `python3 run_momentum_confirm.py` |
| 12 | `python3 run_entry_trigger.py` |
| 13 | `python3 run_strike_select.py` |

Optional:

```bash
python3 run_market_regime.py
python3 run_volume_analyzer.py
python3 run_api.py
```

---

## Common EC2 issues

| Problem | Fix |
|--------|-----|
| Angel login / WS fails | Check outbound HTTPS; TOTP secret; API key |
| Redis connection refused | `docker compose up -d` |
| `run_all.sh` fails | Wrong `BASE=` path; venv not created |
| Permission denied on Docker | Log out/in after `usermod -aG docker` |
| Empty signals | Outside market hours or symbols misconfigured |
| API unreachable | Security group port 8000; `API_HOST=0.0.0.0` |
| Missing Angel env vars | Fill all four keys in `.env` |
| `symbols.txt not found` | Run from `angel_md_redis` folder |
