import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import redis

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise RuntimeError(
        "pyarrow is required for Parquet archiving. Install: pip install pyarrow pandas"
    ) from e

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


def _utc_date_str_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _decode(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")
    return v


def _decode_dict(d: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in d.items():
        out[str(_decode(k))] = str(_decode(v))
    return out


def _validate_tz_name(tz_name: str) -> str:
    """
    Validate IANA timezone name for ARCHIVE_TZ. Falls back to UTC if invalid.
    """
    tz_name = (tz_name or "UTC").strip()
    if tz_name.upper() == "UTC":
        return "UTC"
    if ZoneInfo is None:
        print("[ARCHIVER] zoneinfo not available; falling back to UTC")
        return "UTC"
    try:
        ZoneInfo(tz_name)  # validate
        return tz_name
    except Exception:
        print(f"[ARCHIVER] invalid ARCHIVE_TZ={tz_name!r}; falling back to UTC")
        return "UTC"


class StreamParquetArchiver:
    """
    Redis Streams -> Parquet "data lake" writer.

    Reads from a stream using a consumer group, batches messages,
    writes Parquet files to disk, then XACKs those message IDs.

    Output partitioning:
      data_lake/
        stream=md_ticks_opt/
          dt=YYYY-MM-DD/
            underlying=IOC/   (or symbol=...)
              part-<ts>-<n>.parquet
    """

    def __init__(
        self,
        stream: str,
        group: str,
        consumer: str,
        out_dir: str = "data_lake",
        batch_size: int = 8000,
        flush_sec: int = 10,
        block_ms: int = 2000,
        read_count: int = 1000,
        partition_by_symbol: bool = True,
        compression: str = "zstd",
        delete_after_ack: bool = False,
    ):
        self.stream = stream
        self.group = group
        self.consumer = consumer

        self.out_dir = Path(out_dir)
        self.batch_size = int(batch_size)
        self.flush_sec = int(flush_sec)
        self.block_ms = int(block_ms)
        self.read_count = int(read_count)
        self.partition_by_symbol = bool(partition_by_symbol)
        self.compression = compression
        self.delete_after_ack = bool(delete_after_ack)

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.r = redis.from_url(redis_url, decode_responses=False)

        self._buf_rows: List[Dict[str, Any]] = []
        self._buf_ids: List[str] = []
        self._last_flush = time.time()

        # Controls which "day" each message is assigned to (default: UTC).
        # Example: ARCHIVE_TZ=Asia/Kolkata
        self.partition_tz = _validate_tz_name(os.getenv("ARCHIVE_TZ", "UTC"))

        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_group()

    # ---------------------------
    # Redis consumer group helpers
    # ---------------------------

    def _ensure_group(self) -> None:
        """
        Create consumer group if missing. mkstream=True creates stream if absent.
        """
        try:
            self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            print(f"[ARCHIVER] created group '{self.group}' for stream '{self.stream}'")
        except redis.exceptions.ResponseError as e:
            msg = str(e)
            if "BUSYGROUP" in msg:
                # group already exists
                return
            raise

    def _xreadgroup(self, stream_id: str) -> List[Tuple[str, List[Tuple[str, Dict[bytes, bytes]]]]]:
        """
        stream_id:
          - '>' for new messages
          - '0' / '<id>' to read this consumer's pending entries (PEL) after that id
        """
        return self.r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.stream: stream_id},
            count=self.read_count,
            block=self.block_ms if stream_id == ">" else None,
        )

    # ---------------------------
    # Parquet writing
    # ---------------------------

    def _append_parquet(self, folder: Path, df: pd.DataFrame) -> None:
        folder.mkdir(parents=True, exist_ok=True)

        # atomic-ish write: write tmp then rename
        # ms timestamp + per-process sequence: two flushes in the same ms
        # (e.g. while draining a large PEL) must not overwrite each other
        ts = int(time.time() * 1000)
        self._part_seq = getattr(self, "_part_seq", 0) + 1
        name = f"part-{ts}-{os.getpid()}-{self._part_seq}"
        tmp_path = folder / f".tmp-{name}.parquet"
        final_path = folder / f"{name}.parquet"

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, tmp_path, compression=self.compression)
        tmp_path.replace(final_path)

    def _write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        df = pd.DataFrame(rows)

        # Ensure ts_recv exists and is numeric. When the producer did not set it,
        # fall back to the message's source time (stream-id ms), not archive
        # time, so a replay after midnight still lands in the right dt= folder.
        now_ms = int(time.time() * 1000)
        if "_redis_id" in df.columns:
            src_ms = pd.to_numeric(
                df["_redis_id"].astype(str).str.split("-", n=1).str[0], errors="coerce"
            )
        else:
            src_ms = pd.Series(now_ms, index=df.index)
        if "ts_recv" not in df.columns:
            df["ts_recv"] = src_ms
        df["ts_recv"] = (
            pd.to_numeric(df["ts_recv"], errors="coerce").fillna(src_ms).fillna(now_ms).astype("int64")
        )

        # Stream partition name safe for folders
        stream_folder = f"stream={self.stream.replace(':', '_')}"

        # Optional: partition by underlying/symbol
        key_col: Optional[str] = None
        if self.partition_by_symbol:
            for cand in ("underlying", "symbol"):
                if cand in df.columns:
                    key_col = cand
                    break

        # IMPORTANT: compute dt per row (so batches that span midnight land in the right folder)
        ts = pd.to_datetime(df["ts_recv"], unit="ms", utc=True)
        if self.partition_tz != "UTC":
            ts = ts.dt.tz_convert(self.partition_tz)
        df["_dt"] = ts.dt.strftime("%Y-%m-%d")

        for dt_str, part_dt in df.groupby("_dt", sort=True):
            part_dt = part_dt.drop(columns=["_dt"], errors="ignore")
            base = self.out_dir / stream_folder / f"dt={dt_str}"

            if key_col:
                for key, part_sym in part_dt.groupby(key_col, sort=False):
                    sub = base / f"{key_col}={str(key)}"
                    self._append_parquet(sub, part_sym)
            else:
                self._append_parquet(base, part_dt)

    # ---------------------------
    # Buffering + ACK
    # ---------------------------

    def _flush(self) -> None:
        if not self._buf_rows:
            self._last_flush = time.time()
            return

        # Write first; ACK only if write succeeds
        self._write_batch(self._buf_rows)

        # ACK IDs
        if self._buf_ids:
            self.r.xack(self.stream, self.group, *self._buf_ids)
            if self.delete_after_ack:
                # Optional cleanup (usually not required)
                self.r.xdel(self.stream, *self._buf_ids)

        self._buf_rows.clear()
        self._buf_ids.clear()
        self._last_flush = time.time()

    def _ingest_one(self, msg_id: Any, fields: Any) -> int:
        mid = str(_decode(msg_id))
        if not fields:
            # trimmed/deleted entry: nothing to archive, just drop it from the PEL
            self.r.xack(self.stream, self.group, mid)
            return 0
        row = _decode_dict(fields)
        row["_redis_id"] = mid
        row["_stream"] = self.stream
        self._buf_rows.append(row)
        self._buf_ids.append(mid)
        return 1

    def _ingest_messages(self, resp) -> int:
        n = 0
        for _stream_name, msgs in resp or []:
            for msg_id, fields in msgs or []:
                if msg_id is None:
                    continue
                n += self._ingest_one(msg_id, fields)
        return n

    # ---------------------------
    # Pending-entries (PEL) drain on startup
    # ---------------------------

    @staticmethod
    def _id_ms(msg_id: Any) -> Optional[int]:
        """Milliseconds part of a stream id ('1700000000000-3' -> 1700000000000)."""
        try:
            return int(str(_decode(msg_id)).split("-", 1)[0])
        except Exception:
            return None

    def _read_pending_after(self, after_id: str) -> List[Tuple[Any, Any]]:
        """
        Return up to read_count entries of this consumer's PEL with id > after_id.
        Trimmed/deleted entries come back as (id, None) or (id, {}).
        Some redis-py versions raise while parsing a nil field list; in that case
        fall back to XPENDING + XRANGE so the archiver never crash-loops.
        """
        try:
            resp = self._xreadgroup(after_id)
        except (TypeError, AttributeError, ValueError) as e:
            print(f"[ARCHIVER] {self.stream}: PEL parse error {e!r}; using XPENDING fallback")
            return self._read_pending_after_slow(after_id)
        out: List[Tuple[Any, Any]] = []
        for _stream_name, msgs in resp or []:
            out.extend(msgs or [])
        return out

    def _read_pending_after_slow(self, after_id: str) -> List[Tuple[Any, Any]]:
        lo = "-" if after_id in ("0", "0-0") else f"({after_id}"
        pend = self.r.xpending_range(
            self.stream, self.group, min=lo, max="+", count=self.read_count,
            consumername=self.consumer,
        )
        out: List[Tuple[Any, Any]] = []
        for p in pend or []:
            mid = p.get("message_id") if isinstance(p, dict) else p[0]
            got = self.r.xrange(self.stream, min=mid, max=mid, count=1)
            out.append((mid, got[0][1] if got else None))
        return out

    def _drain_pending(self) -> int:
        """
        Re-ingest this consumer's pending entries exactly once.

        XREADGROUP with an explicit id returns PEL entries with id strictly
        greater than it, so the cursor is advanced past the last returned id
        (re-reading "0" in a loop would return the same entries forever ->
        duplicated rows). Entries whose payload was trimmed by MAXLEN are
        ACKed and skipped. Returns number of rows ingested.
        """
        cursor = "0"
        total = 0
        dead_total = 0
        while True:
            entries = self._read_pending_after(cursor)
            if not entries:
                break
            last_id: Optional[str] = None
            dead: List[str] = []
            for msg_id, fields in entries:
                if msg_id is None:
                    continue
                mid = str(_decode(msg_id))
                last_id = mid
                if not fields:
                    dead.append(mid)
                    continue
                total += self._ingest_one(mid, fields)
            if dead:
                self.r.xack(self.stream, self.group, *dead)
                dead_total += len(dead)
            if len(self._buf_rows) >= self.batch_size:
                self._flush()
            if last_id is None or last_id == cursor:
                break
            cursor = last_id
        if total or dead_total:
            print(
                f"[ARCHIVER] {self.stream}: drained {total} pending entries"
                f" ({dead_total} trimmed entries acked+skipped)"
            )
        return total

    # ---------------------------
    # Main loop
    # ---------------------------

    def run_forever(self) -> None:
        print(
            f"[ARCHIVER] running stream={self.stream} group={self.group} consumer={self.consumer} "
            f"batch_size={self.batch_size} flush_sec={self.flush_sec}"
        )

        # 1) Drain pending (if any) first — each PEL entry exactly once
        self._drain_pending()
        self._flush()

        # 2) Tail new messages forever
        while True:
            resp = self._xreadgroup(">")
            if resp:
                self._ingest_messages(resp)

            # flush conditions
            if len(self._buf_rows) >= self.batch_size:
                self._flush()
            elif (time.time() - self._last_flush) >= self.flush_sec:
                self._flush()
