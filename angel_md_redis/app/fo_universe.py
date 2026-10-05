"""
F&O universe: which stocks NSE currently allows option trading in.

Source: Angel ScripMaster (refreshed daily by Angel) — every NFO OPTSTK row.
NSE lists 3 monthly expiries (near / next / far) for every F&O stock. A stock
NSE is excluding gets no new far month, so it drops to 2, then 1, then 0 live
expiries while its existing contracts run out.

  ACTIVE   live expiries >= min_expiries (default 3)
  EXITING  1 .. min_expiries-1 live expiries  -> warn, no new months listed
  (absent) 0 live expiries                    -> expired / never F&O

Ban list: NSE fo_secban.csv (MWPL > 95%). No fresh positions in a banned
stock for that trade date; squaring off is allowed.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

from .scripmaster import parse_expiry

BAN_URL = "https://nsearchives.nseindia.com/content/fo/fo_secban.csv"

# A partial / broken ScripMaster download must never empty symbols.txt.
MIN_SANE_STOCKS = 100


@dataclass
class FoUniverse:
    as_of: str
    active: Dict[str, dict] = field(default_factory=dict)    # name -> {expiries, lot_size}
    exiting: Dict[str, dict] = field(default_factory=dict)
    indices: Set[str] = field(default_factory=set)            # OPTIDX underlyings

    def live(self) -> Set[str]:
        return set(self.active) | set(self.exiting) | self.indices

    def sane(self) -> bool:
        return len(self.active) + len(self.exiting) >= MIN_SANE_STOCKS


def build_universe(rows: Iterable[dict], today: dt.date, min_expiries: int = 3) -> FoUniverse:
    """rows = raw ScripMaster JSON records."""
    expiries: Dict[str, Set[dt.date]] = {}
    lots: Dict[str, str] = {}
    indices: Set[str] = set()
    for r in rows:
        if str(r.get("exch_seg") or "") != "NFO":
            continue
        it = str(r.get("instrumenttype") or "").upper()
        name = str(r.get("name") or "").strip().upper()
        if not name:
            continue
        exp = parse_expiry(r.get("expiry"))
        if exp is None or exp < today:
            continue
        if it == "OPTIDX":
            indices.add(name)
        elif it == "OPTSTK":
            expiries.setdefault(name, set()).add(exp)
            lots.setdefault(name, str(r.get("lotsize") or ""))

    uni = FoUniverse(as_of=today.isoformat(), indices=indices)
    for name in sorted(expiries):
        doc = {"expiries": [e.isoformat() for e in sorted(expiries[name])], "lot_size": lots.get(name, "")}
        (uni.active if len(expiries[name]) >= min_expiries else uni.exiting)[name] = doc
    return uni


def parse_ban_csv(text: str) -> List[str]:
    """'Securities in Ban For Trade Date 05-OCT-2026:' then '1,SAIL' rows; empty list = no bans."""
    out: List[str] = []
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 2 and parts[0].isdigit() and parts[1]:
            out.append(parts[1].upper())
    return out


def ban_trade_date(text: str) -> Optional[str]:
    """'05-OCT-2026' -> '2026-10-05' from the header line, None if absent."""
    first = text.strip().splitlines()[0] if text.strip() else ""
    tail = first.rsplit(" ", 1)[-1].rstrip(":").strip()
    try:
        return dt.datetime.strptime(tail, "%d-%b-%Y").date().isoformat()
    except ValueError:
        return None


@dataclass
class Reconcile:
    keep: List[str]
    removed: List[str]      # no live option contracts -> dropped from symbols.txt
    exiting: List[str]      # still tradable, but being phased out -> warn
    banned: List[str]       # in today's ban period -> warn (ICARE rejects)


def reconcile_symbols(symbols: List[str], uni: FoUniverse, banned: Iterable[str]) -> Reconcile:
    live = uni.live()
    ban = set(banned)
    keep = [s for s in symbols if s in live]
    return Reconcile(
        keep=keep,
        removed=[s for s in symbols if s not in live],
        exiting=[s for s in keep if s in uni.exiting],
        banned=[s for s in keep if s in ban],
    )


def diff(prev: Iterable[str], cur: Iterable[str]) -> tuple[List[str], List[str]]:
    p, c = set(prev), set(cur)
    return sorted(c - p), sorted(p - c)
