"""
run_fo_universe.py
──────────────────
Daily pre-market refresh of the F&O stock universe (run by run_all.sh before
the workers start; safe to run by hand any time).

  1. Downloads a fresh Angel ScripMaster (when the cached copy is not from today, IST)
  2. Builds the option-allowed stock list (app/fo_universe.py): ACTIVE / EXITING
  3. Diffs against the previous fo_universe.json -> logs ADDED / REMOVED
  4. Fetches NSE's F&O ban list (fo_secban.csv)
  5. Reconciles symbols.txt:
       no live option contracts -> REMOVED from symbols.txt (backup: symbols.txt.bak)
       EXITING (no new months)  -> warning only
       in ban period            -> warning only (ICARE rejects: fo_ban_period)

Writes:
  fo_universe.json               full universe (active / exiting / indices / ban list)
  md:universe:fo                 SET of ACTIVE stocks
  md:universe:fo:exiting         SET of EXITING stocks
  md:universe:fo:meta            JSON {as_of, counts, added, removed}
  md:fo:ban                      SET of banned stocks for the trade date (TTL FO_BAN_TTL_SEC)
  md:fo:ban:meta                 JSON {trade_date, fetched_ms, ok}

Redis down / NSE unreachable: logged, the rest still runs. A ScripMaster with
fewer than 100 option stocks is treated as broken and symbols.txt is not touched.

Flags: --dry-run  print what would change, write nothing
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import List, Optional

import redis
import requests

from app.config import BASE_DIR, SYMBOLS_FILE, load_symbols
from app.fo_universe import BAN_URL, FoUniverse, ban_trade_date, build_universe, diff, parse_ban_csv, reconcile_symbols
from app.logging_setup import setup_logger
from app.option_pricing import IST
from app.scripmaster import SCRIPMASTER_URL

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SCRIPMASTER_PATH = BASE_DIR / "OpenAPIScripMaster.json"
UNIVERSE_PATH = BASE_DIR / os.getenv("FO_UNIVERSE_FILE", "fo_universe.json")
MIN_EXPIRIES = int(os.getenv("FO_MIN_EXPIRIES", "3"))

UNIVERSE_KEY = os.getenv("FO_UNIVERSE_KEY", "md:universe:fo")
EXITING_KEY = os.getenv("FO_UNIVERSE_EXITING_KEY", "md:universe:fo:exiting")
UNIVERSE_META_KEY = os.getenv("FO_UNIVERSE_META_KEY", "md:universe:fo:meta")
BAN_KEY = os.getenv("FO_BAN_KEY", "md:fo:ban")
BAN_META_KEY = os.getenv("FO_BAN_META_KEY", "md:fo:ban:meta")
BAN_TTL_SEC = int(os.getenv("FO_BAN_TTL_SEC", str(20 * 3600)))   # gone before the next trade date

log = setup_logger("fo_universe")


def today_ist() -> dt.date:
    return dt.datetime.now(IST).date()


def load_scripmaster_rows(today: dt.date, save: bool = True) -> list:
    fresh = (SCRIPMASTER_PATH.exists()
             and dt.datetime.fromtimestamp(SCRIPMASTER_PATH.stat().st_mtime, IST).date() == today)
    if not fresh:
        log.info("DOWNLOAD ScripMaster %s", SCRIPMASTER_URL)
        r = requests.get(SCRIPMASTER_URL, timeout=120)
        r.raise_for_status()
        rows = r.json()                                   # validate before replacing the cache
        if save:
            tmp = SCRIPMASTER_PATH.with_suffix(".json.tmp")
            tmp.write_bytes(r.content)
            tmp.replace(SCRIPMASTER_PATH)
        return rows
    log.info("ScripMaster cache is from today: %s", SCRIPMASTER_PATH)
    return json.loads(SCRIPMASTER_PATH.read_text(encoding="utf-8"))


def fetch_ban_list() -> tuple[Optional[List[str]], Optional[str]]:
    """(banned, trade_date); (None, None) when NSE is unreachable."""
    try:
        r = requests.get(BAN_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*"})
        r.raise_for_status()
        return parse_ban_csv(r.text), ban_trade_date(r.text)
    except Exception as e:
        log.warning("BAN_LIST_UNAVAILABLE url=%s err=%s", BAN_URL, e)
        return None, None


def load_previous() -> Optional[dict]:
    try:
        return json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("PREVIOUS_UNREADABLE %s err=%s", UNIVERSE_PATH, e)
        return None


def write_symbols(keep: List[str]) -> None:
    backup = SYMBOLS_FILE.with_suffix(".txt.bak")
    backup.write_text(SYMBOLS_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    tmp = SYMBOLS_FILE.with_suffix(".txt.tmp")
    tmp.write_text("\n".join(keep) + "\n", encoding="utf-8")
    tmp.replace(SYMBOLS_FILE)


def publish_redis(uni: FoUniverse, meta: dict, banned: Optional[List[str]], ban_date: Optional[str]) -> None:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        p = r.pipeline()
        p.delete(UNIVERSE_KEY, EXITING_KEY)
        if uni.active:
            p.sadd(UNIVERSE_KEY, *uni.active)
        if uni.exiting:
            p.sadd(EXITING_KEY, *uni.exiting)
        p.set(UNIVERSE_META_KEY, json.dumps(meta, separators=(",", ":")))
        if banned is not None:                     # unreachable NSE: keep whatever is there (it expires)
            p.delete(BAN_KEY)
            if banned:
                p.sadd(BAN_KEY, *banned)
            p.expire(BAN_KEY, BAN_TTL_SEC)
            p.set(BAN_META_KEY, json.dumps({"trade_date": ban_date or "", "fetched_ms": int(time.time() * 1000),
                                            "count": len(banned), "ok": 1}), ex=BAN_TTL_SEC)
        p.execute()
        log.info("REDIS written %s(%d) %s(%d) %s(%s)", UNIVERSE_KEY, len(uni.active), EXITING_KEY,
                 len(uni.exiting), BAN_KEY, "unchanged" if banned is None else len(banned))
    except Exception as e:
        log.warning("REDIS_UNAVAILABLE url=%s err=%s (files still written)", REDIS_URL, e)


def main() -> int:
    ap = argparse.ArgumentParser(description="Refresh the F&O option-stock universe and reconcile symbols.txt")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing")
    args = ap.parse_args()

    today = today_ist()
    uni = build_universe(load_scripmaster_rows(today, save=not args.dry_run), today, MIN_EXPIRIES)
    log.info("UNIVERSE as_of=%s active=%d exiting=%d indices=%d",
             uni.as_of, len(uni.active), len(uni.exiting), len(uni.indices))
    if not uni.sane():
        log.error("SCRIPMASTER_SUSPECT only %d option stocks — nothing written, symbols.txt untouched",
                  len(uni.active) + len(uni.exiting))
        return 1

    for name, doc in uni.exiting.items():
        log.warning("EXITING %s live_expiries=%s (no new months listed — NSE is phasing it out)",
                    name, ",".join(doc["expiries"]))

    prev = load_previous()
    added: List[str] = []
    removed: List[str] = []
    if prev:
        prev_live = set(prev.get("active", {})) | set(prev.get("exiting", {}))
        added, removed = diff(prev_live, set(uni.active) | set(uni.exiting))
        for s in added:
            log.warning("ADDED %s (new F&O stock since %s)", s, prev.get("as_of"))
        for s in removed:
            log.warning("REMOVED %s (no live option contracts; was F&O on %s)", s, prev.get("as_of"))
    else:
        log.info("no previous %s — first run, no diff", UNIVERSE_PATH.name)

    banned, ban_date = fetch_ban_list()
    if banned is not None:
        log.info("BAN_LIST trade_date=%s count=%d %s", ban_date, len(banned), banned)

    symbols = load_symbols()
    rec = reconcile_symbols(symbols, uni, banned or [])
    for s in rec.exiting:
        log.warning("SYMBOLS_EXITING %s is in symbols.txt but NSE is phasing it out (expiries %s)",
                    s, ",".join(uni.exiting[s]["expiries"]))
    for s in rec.banned:
        log.warning("SYMBOLS_BANNED %s is in today's F&O ban period — no fresh entries (ICARE rejects)", s)
    for s in rec.removed:
        log.warning("SYMBOLS_REMOVE %s has no live option contracts%s", s, " (dry run)" if args.dry_run else "")
    if not rec.keep:
        log.error("SYMBOLS_EMPTY every symbol would be removed — symbols.txt untouched, check it by hand")

    if args.dry_run:
        log.info("DRY_RUN nothing written")
        return 0

    if rec.removed and rec.keep:
        write_symbols(rec.keep)
        log.warning("SYMBOLS_UPDATED removed=%s kept=%d backup=%s", rec.removed, len(rec.keep),
                    SYMBOLS_FILE.with_suffix(".txt.bak").name)

    doc = {
        "as_of": uni.as_of,
        "generated_ms": int(time.time() * 1000),
        "min_expiries": MIN_EXPIRIES,
        "active": uni.active,
        "exiting": uni.exiting,
        "indices": sorted(uni.indices),
        "ban": {"trade_date": ban_date, "symbols": banned} if banned is not None else None,
        "added": added,
        "removed": removed,
    }
    tmp = UNIVERSE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(doc, indent=1), encoding="utf-8")
    tmp.replace(UNIVERSE_PATH)

    meta = {"as_of": uni.as_of, "active": len(uni.active), "exiting": sorted(uni.exiting),
            "added": added, "removed": removed}
    publish_redis(uni, meta, banned, ban_date)
    log.info("DONE %s written", UNIVERSE_PATH.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
