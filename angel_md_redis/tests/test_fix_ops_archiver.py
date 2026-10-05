"""
Archiver restart/PEL tests (no Redis server): an in-memory fake implements the
consumer-group semantics the archivers rely on.

  * restart with N pending entries -> exactly N archived rows (no duplicates)
  * PEL entries whose payload was trimmed by MAXLEN ((id, None), (id, {}) or a
    redis-py parse error) are ACKed + skipped, never crash the archiver
  * dt= partition uses the stream-id time when ts_recv is missing
"""
from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis  # noqa: E402

from app import archiver as parquet_mod  # noqa: E402
from app import csv_archiver as csv_mod  # noqa: E402

try:
    import pyarrow.parquet as pq  # noqa: E402
except Exception:  # pragma: no cover
    pq = None


def _key(i):
    a, b = str(i).split("-")
    return (int(a), int(b))


class FakeRedis:
    """Minimal Redis Streams + consumer groups with PEL semantics."""

    def __init__(self, trimmed_mode: str = "none"):
        # trimmed_mode: what XREADGROUP returns for a PEL entry whose message is gone
        #   "none"  -> (id, None)   "empty" -> (id, {})   "raise" -> TypeError (old redis-py)
        self.trimmed_mode = trimmed_mode
        self.streams = {}   # name -> {id: fields}
        self.groups = {}    # (stream, group) -> {"last": "0-0", "pel": {id: consumer}}
        self.seq = 0
        self.xreadgroup_calls = 0

    # -- setup helpers --
    def xadd(self, name, fields, id=None):
        s = self.streams.setdefault(name, {})
        if id is None:
            self.seq += 1
            id = f"{1767225600000 + self.seq}-0"
        s[id] = {k.encode() if isinstance(k, str) else k: str(v).encode() for k, v in fields.items()}
        return id.encode()

    def trim_first(self, name, n):
        s = self.streams[name]
        for mid in sorted(s, key=_key)[:n]:
            del s[mid]

    # -- commands used by the archivers --
    def xgroup_create(self, name, groupname, id="$", mkstream=False):
        if name not in self.streams:
            if not mkstream:
                raise redis.exceptions.ResponseError("ERR no such key")
            self.streams[name] = {}
        if (name, groupname) in self.groups:
            raise redis.exceptions.ResponseError("BUSYGROUP Consumer Group name already exists")
        self.groups[(name, groupname)] = {"last": "0-0", "pel": {}}
        return True

    def xreadgroup(self, groupname, consumername, streams, count=None, block=None, noack=False):
        self.xreadgroup_calls += 1
        out = []
        for name, sid in streams.items():
            g = self.groups[(name, groupname)]
            s = self.streams.get(name, {})
            if sid == ">":
                ids = [i for i in sorted(s, key=_key) if _key(i) > _key(g["last"])][:count]
                for i in ids:
                    g["pel"][i] = consumername
                if ids:
                    g["last"] = ids[-1]
                    out.append([name.encode(), [(i.encode(), dict(s[i])) for i in ids]])
            else:
                after = _key(sid if "-" in str(sid) else f"{sid}-0")
                ids = sorted(
                    (i for i, c in g["pel"].items() if c == consumername and _key(i) > after),
                    key=_key,
                )[:count]
                msgs = []
                for i in ids:
                    if i in s:
                        msgs.append((i.encode(), dict(s[i])))
                    elif self.trimmed_mode == "raise":
                        raise TypeError("'NoneType' object is not subscriptable")
                    elif self.trimmed_mode == "empty":
                        msgs.append((i.encode(), {}))
                    else:
                        msgs.append((i.encode(), None))
                out.append([name.encode(), msgs])  # real Redis: [[stream, []]] when nothing pending
        return out

    def xack(self, name, groupname, *ids):
        g = self.groups[(name, groupname)]
        n = 0
        for i in ids:
            i = i.decode() if isinstance(i, bytes) else str(i)
            if g["pel"].pop(i, None) is not None:
                n += 1
        return n

    def xdel(self, name, *ids):
        for i in ids:
            self.streams.get(name, {}).pop(str(i), None)

    def xpending_range(self, name, groupname, min, max, count, consumername=None, idle=None):
        g = self.groups[(name, groupname)]
        lo = None
        if min not in ("-", "0"):
            lo = _key(min.lstrip("("))
        ids = sorted(
            (i for i, c in g["pel"].items()
             if (consumername is None or c == consumername) and (lo is None or _key(i) > lo)),
            key=_key,
        )[:count]
        return [{"message_id": i.encode(), "consumer": g["pel"][i].encode(),
                 "time_since_delivered": 0, "times_delivered": 1} for i in ids]

    def xrange(self, name, min="-", max="+", count=None):
        mid = min.decode() if isinstance(min, bytes) else str(min)
        s = self.streams.get(name, {})
        return [(mid.encode(), dict(s[mid]))] if mid in s else []

    def pel_size(self, name, group):
        return len(self.groups[(name, group)]["pel"])


STREAM = "md:test:stream"
GROUP = "archive"
CONSUMER = "arch-test-1"


def _count_parquet_rows(out_dir: Path) -> int:
    return sum(pq.ParquetFile(f).metadata.num_rows for f in out_dir.rglob("*.parquet"))


def _count_csv_rows(out_dir: Path) -> int:
    n = 0
    for f in out_dir.rglob("*.csv"):
        with f.open(newline="", encoding="utf-8") as fh:
            n += sum(1 for _ in csv.DictReader(fh))
    return n


class _Base:
    """Shared scenarios; subclasses set make() and count_rows()."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "lake"

    def tearDown(self):
        self.tmp.cleanup()

    def _crash_with_pending(self, fake, n):
        """First archiver run reads n entries via '>' and dies before flushing/ACKing."""
        for k in range(n):
            fake.xadd(STREAM, {"symbol": "NIFTY" if k % 2 else "BANKNIFTY", "v": k})
        first = self.make(fake)
        while first._ingest_messages(first._xreadgroup(">")):
            pass
        self.assertEqual(fake.pel_size(STREAM, GROUP), n)

    def test_restart_with_2500_pending_yields_exactly_2500_rows(self):
        fake = FakeRedis()
        self._crash_with_pending(fake, 2500)
        second = self.make(fake)  # restart, same consumer name
        drained = second._drain_pending()
        second._flush()
        self.assertEqual(drained, 2500)
        self.assertEqual(self.count_rows(), 2500)
        self.assertEqual(fake.pel_size(STREAM, GROUP), 0)
        # cursor advanced: ~2500/300 reads + 1 empty, not an endless re-read of "0"
        self.assertLess(fake.xreadgroup_calls, 30)

    def test_run_forever_drains_pending_once_then_tails(self):
        fake = FakeRedis()
        self._crash_with_pending(fake, 2500)
        arch = self.make(fake)
        real = arch._xreadgroup

        class _Stop(Exception):
            pass

        def guarded(sid):
            if sid == ">":
                raise _Stop()
            return real(sid)

        arch._xreadgroup = guarded
        with self.assertRaises(_Stop):
            arch.run_forever()
        self.assertEqual(self.count_rows(), 2500)
        self.assertEqual(fake.pel_size(STREAM, GROUP), 0)

    def _trimmed(self, mode):
        fake = FakeRedis(trimmed_mode=mode)
        self._crash_with_pending(fake, 1000)
        fake.trim_first(STREAM, 400)  # MAXLEN trimmed the oldest 400 while they were pending
        arch = self.make(fake)
        arch._drain_pending()
        arch._flush()
        self.assertEqual(self.count_rows(), 600)
        self.assertEqual(fake.pel_size(STREAM, GROUP), 0, "trimmed entries must be ACKed")

    def test_trimmed_pel_entries_none(self):
        self._trimmed("none")

    def test_trimmed_pel_entries_empty_dict(self):
        self._trimmed("empty")

    def test_trimmed_pel_entries_parse_error_falls_back(self):
        self._trimmed("raise")

    def test_dt_partition_uses_stream_id_time_when_ts_recv_missing(self):
        fake = FakeRedis()
        # 2026-01-01 23:59:00 IST == 2026-01-01T18:29:00Z ; archive time is "today"
        fake.xadd(STREAM, {"symbol": "NIFTY", "v": 1}, id="1767292140000-0")
        arch = self.make(fake)
        arch._ingest_messages(arch._xreadgroup(">"))
        arch._flush()
        dts = {p.name for p in self.out.rglob("dt=*")}
        self.assertEqual(dts, {"dt=2026-01-01"})


@unittest.skipIf(pq is None, "pyarrow not installed")
class ParquetArchiverOpsTest(_Base, unittest.TestCase):
    def make(self, fake):
        with mock.patch.object(parquet_mod.redis, "from_url", return_value=fake):
            return parquet_mod.StreamParquetArchiver(
                stream=STREAM, group=GROUP, consumer=CONSUMER, out_dir=str(self.out),
                batch_size=700, read_count=300, flush_sec=10, partition_by_symbol=True,
            )

    def count_rows(self):
        return _count_parquet_rows(self.out)


class CsvArchiverOpsTest(_Base, unittest.TestCase):
    def make(self, fake):
        with mock.patch.object(csv_mod.redis, "from_url", return_value=fake):
            return csv_mod.StreamCsvArchiver(
                stream=STREAM, group=GROUP, consumer=CONSUMER, out_dir=str(self.out),
                batch_size=700, read_count=300, flush_sec=10, partition_by_symbol=True,
            )

    def count_rows(self):
        return _count_csv_rows(self.out)


class ArchiverLayersStreamsTest(unittest.TestCase):
    def test_capital_alloc_and_exit_request_archived(self):
        import run_archiver_layers as layers

        streams = {s for s, _c, _b in layers.STREAMS.values()}
        self.assertIn("md:capital:alloc", streams)
        self.assertIn("md:exec:exit_request", streams)
        consumers = [c for _s, c, _b in layers.STREAMS.values()]
        self.assertEqual(len(consumers), len(set(consumers)), "consumer names must be unique")
        self.assertEqual(len(streams), len(layers.STREAMS), "each stream archived once")

    def test_archiving_missing_stream_is_harmless(self):
        fake = FakeRedis()
        with mock.patch.object(parquet_mod.redis, "from_url", return_value=fake):
            arch = parquet_mod.StreamParquetArchiver(
                stream="md:exec:exit_request", group=GROUP, consumer="c",
                out_dir=tempfile.mkdtemp(),
            )
        self.assertEqual(arch._drain_pending(), 0)
        self.assertEqual(arch._ingest_messages(arch._xreadgroup(">")), 0)


if __name__ == "__main__":
    unittest.main()
