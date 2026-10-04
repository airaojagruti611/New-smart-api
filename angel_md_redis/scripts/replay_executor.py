"""
scripts/replay_executor.py — DECISION.md §7.7 (P30)

Replays the Order Executor (pure engine + PaperBroker) against archived REAL option
books (data_lake/stream=md_ticks_opt): random approvals per contract, clock gates
neutralised (the archive covers only the last ~40 minutes of each day). Read-only.

    python scripts/replay_executor.py            # all archived days
    python scripts/replay_executor.py 2026-09-21 # one day
"""
import glob, os, sys, json, random
import datetime as dt
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.option_pricing import IST
from app.order_executor import state as S
from app.order_executor.broker import PaperBroker
from app.order_executor.charges import load_rates
from app.order_executor.command import Context, build_command
from app.order_executor.config import ExecConfig
from app.order_executor.engine import build_report, new_state, step
from app.order_executor.market import quote_from_bidask
from app.scripmaster import load_scripmaster, option_specs

DAY = sys.argv[1] if len(sys.argv) > 1 else "*"
files = [f for f in glob.glob(f"data_lake/stream=md_ticks_opt/dt={DAY}/**/*.parquet", recursive=True) if os.path.getsize(f) > 0]
ticks = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
ticks["ts"] = pd.to_numeric(ticks["ts_recv"], errors="coerce")
ticks = ticks.dropna(subset=["ts"]).sort_values("ts")
print(f"{DAY}: {len(ticks)} option ticks, {ticks['tradingsymbol'].nunique()} contracts")
specs = option_specs(load_scripmaster(), ["TCS", "INFY", "RELIANCE"])
CFG, RATES = ExecConfig(), load_rates()
random.seed(7)

def quotes_for(g):
    out = []
    for row in g.itertuples():
        d = {"ts_ms": row.ts, "bid": row.bid, "ask": row.ask, "bid_qty": row.bid_sz, "ask_qty": row.ask_sz,
             "ask_depth5": row.ask_depth5, "ask_depth5_px": row.ask_depth5_px}
        q = quote_from_bidask(d)
        if q and q.tradable:
            out.append(q)
    return out

reports = []
for tsym, g in ticks.groupby(["tradingsymbol", ticks["ts"].map(lambda x: dt.datetime.fromtimestamp(x/1000, IST).date())]):
    tsym = tsym[0]
    qs = quotes_for(g)
    if len(qs) < 50:
        continue
    und = str(g["underlying"].iloc[0]).upper()
    spec = specs.get(str(tsym).upper()) or next((v for k, v in specs.items() if k.startswith(und)), {})
    lot = spec.get("lot_size") or 0
    if not lot:
        continue
    # 6 approvals per contract at random times between 09:30 and 15:00
    for k in range(6):
        i = random.randrange(len(qs) // 10, len(qs) - 20)
        q0 = qs[i]
        t_ist = dt.datetime.fromtimestamp(q0.ts_ms / 1000, IST)
        if t_ist.strftime("%H:%M") >= "15:29":       # archive is late-session only; stay inside market hours
            continue
        icare = {"status": "APPROVED", "ts_ms": q0.ts_ms, "symbol": str(g["underlying"].iloc[0]), "tradingsymbol": tsym,
                 "side": str(tsym)[-2:], "recommended_lots": random.choice([1, 2, 3]), "lot_size": lot,
                 "premium": q0.mid, "stop_loss_premium": q0.mid * 0.8, "max_risk_allowed": 1e7, "expected_value": 1500}
        st = new_state(build_command(icare, f"R{len(reports)}", "x", spec, CFG), q0.ts_ms)
        broker, upd, j = PaperBroker(300), [], i
        now = q0.ts_ms
        while st.status not in S.TERMINAL and now < q0.ts_ms + 20_000:
            while j + 1 < len(qs) and qs[j + 1].ts_ms <= now:
                j += 1
            q = qs[j]
            ctx = Context(hhmm="11:00", available_margin=1e7)   # clock gates neutralised: book-driven behaviour only
            st, actions, _ = step(st, q, ctx, upd, now, CFG, RATES)
            for a in actions:
                {"PLACE": lambda a: broker.place(a, now), "MODIFY": lambda a: broker.modify(a["order_id"], a["price"], a["qty"], now),
                 "CANCEL": lambda a: broker.cancel(a["order_id"], now)}[a["type"]](a)
            upd = broker.poll({tsym: q}, now + 1)
            now += 500
        r = build_report(st, RATES, "replay")
        r["spread0"] = q0.spread_pct
        r["prem0"] = q0.mid
        reports.append(r)

df = pd.DataFrame(reports)
print(f"\napprovals replayed: {len(df)}")
print(df["execution_status"].value_counts().to_string())
rej = df[df["execution_status"] == S.REJECTED_BEFORE_EXECUTION]["reject_reasons"].map(lambda x: x[0] if x else "")
print("\nfirst reject reason:\n" + rej.value_counts().to_string())
f = df[df["filled_lots"] > 0]
if not f.empty:
    print(f"\nfilled: {len(f)} | fill ratio {(f['filled_lots']/f['requested_lots']).mean():.2f} | "
          f"slippage vs ask mean {f['slippage_pct'].mean():.3f}% (median {f['slippage_pct'].median():.3f}%) | "
          f"vs mid mean {(f['slippage_vs_mid']/f['arrival_mid']*100).mean():.3f}% | "
          f"duration median {f['execution_duration_ms'].median():.0f} ms | orders/trade {f['orders_used'].mean():.2f}")
print(f"\nspread at approval: median {df['spread0'].median():.2f}%  p25 {df['spread0'].quantile(.25):.2f}%  p75 {df['spread0'].quantile(.75):.2f}%  "
      f"share <= 1%: {(df['spread0'] <= 1).mean():.0%}  share <= 0.5%: {(df['spread0'] <= 0.5).mean():.0%}")
