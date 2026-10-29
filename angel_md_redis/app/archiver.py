import os
import time
import threading
import uuid
import hashlib
import json
import sys
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis
from urllib.parse import quote

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise RuntimeError(
        "pyarrow is required for Parquet archiving. Install: pip install pyarrow"
    ) from e

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


class ArchiveDataLossError(RuntimeError):
    """A Redis pending ID exists but its source payload is no longer available."""


def _utc_date_str_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _decode(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8")
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
        max_buffer_bytes: int = 16 * 1024 * 1024,
        write_semaphore: Optional[threading.Semaphore] = None,
        status_dir: Optional[str] = None,
        claim_idle_ms: int = 60_000,
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
        self.max_buffer_bytes = int(max_buffer_bytes)
        if min(self.batch_size, self.read_count, self.max_buffer_bytes) < 1 or self.flush_sec <= 0:
            raise ValueError("Archive batch/read/buffer sizes and flush interval must be positive")
        self.write_semaphore = write_semaphore
        self.status_dir = Path(status_dir) if status_dir else None
        self.claim_idle_ms = int(claim_idle_ms)
        if self.claim_idle_ms < 0:
            raise ValueError("claim_idle_ms cannot be negative")

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.r = redis.from_url(redis_url, decode_responses=False)

        self._buf_rows: List[Dict[str, Any]] = []
        self._buf_ids: List[str] = []
        self._last_flush = time.time()
        self._buffer_bytes = 0
        self._rows_archived = 0
        self._files_written = 0
        self._last_report = 0.0
        self._stop_event = None

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
          - '0' to read pending (PEL)
        """
        return self.r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.stream: stream_id},
            count=self.read_count,
            # BLOCK 0 means wait forever in Redis. Omit BLOCK for recovery and
            # shutdown, rather than making an empty stream delay the drain.
            block=self.block_ms if stream_id == ">" and not (self._stop_event and self._stop_event.is_set()) else None,
        )

    # ---------------------------
    # Parquet writing
    # ---------------------------

    def _append_parquet(self, folder: Path, rows: List[Dict[str, Any]]) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        # Include every key, even fields that first appear after the first row.
        # Redis payload values remain strings; only the receive time is int64.
        names = sorted({key for row in rows for key in row})
        schema = pa.schema([(name, pa.int64() if name == "ts_recv" else pa.string()) for name in names])
        table = pa.Table.from_pylist(rows, schema=schema)
        ids = [row.get("_redis_id") for row in rows]
        if all(ids):
            identity = hashlib.sha256("\n".join(ids).encode("utf-8")).hexdigest()[:32]
            part = f"{ids[0].split('-')[0]}-{identity}"
        else:
            part = f"{int(time.time() * 1000)}-{uuid.uuid4().hex}"
        # The same batch replay replaces its own part instead of creating a
        # second random file. Different recovery batch boundaries remain at-least-once.
        final_path = folder / f"part-{part}.parquet"
        tmp_path = folder / f".tmp-part-{uuid.uuid4().hex}.parquet"
        try:
            with tmp_path.open("wb") as output:
                pq.write_table(table, output, compression=self.compression, write_page_checksum=True)
                output.flush()
                os.fsync(output.fileno())
            if pq.ParquetFile(tmp_path).metadata.num_rows != len(rows):
                raise RuntimeError("Parquet row count did not match the archive batch")
            tmp_path.replace(final_path)
            self._files_written = getattr(self, "_files_written", 0) + 1
        finally:
            tmp_path.unlink(missing_ok=True)

    def _write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        stream_folder = f"stream={self.stream.replace(':', '_')}"
        key_col: Optional[str] = None
        if self.partition_by_symbol:
            for cand in ("underlying", "symbol"):
                if any(row.get(cand) for row in rows):
                    key_col = cand
                    break
        zone = timezone.utc if self.partition_tz == "UTC" else ZoneInfo(self.partition_tz)
        dates: Dict[int, str] = {}
        partitions: Dict[Tuple[str, Optional[str]], List[Dict[str, Any]]] = {}
        for original in rows:
            row = {str(key): (None if value is None else str(value)) for key, value in original.items()}
            row["ts_recv"] = self._receive_ms(original)
            second = row["ts_recv"] // 1000
            if second not in dates:
                dates[second] = datetime.fromtimestamp(second, tz=zone).strftime("%Y-%m-%d")
            # Missing symbols must be retained rather than silently dropped by groupby.
            symbol = str(row.get(key_col) or "UNKNOWN") if key_col else None
            partitions.setdefault((dates[second], symbol), []).append(row)
        for (date, symbol), part in partitions.items():
            folder = self.out_dir / stream_folder / f"dt={date}"
            if key_col:
                folder = folder / f"{key_col}={quote(symbol, safe='-_.&')}"
            self._append_parquet(folder, part)

    @staticmethod
    def _receive_ms(row: Dict[str, Any]) -> int:
        # Stream IDs preserve the original receive date when replaying yesterday's backlog.
        redis_ms = str(row.get("_redis_id", "")).split("-")[0]
        for value in (row.get("ts_recv"), redis_ms):
            try:
                stamp = int(float(value))
                datetime.fromtimestamp(stamp / 1000, tz=timezone.utc)
                return stamp
            except (ValueError, TypeError, OverflowError, OSError):
                continue
        return int(time.time() * 1000)

    def _report(self, elapsed: float, last_id: Optional[str]) -> None:
        snapshot = {"stream": self.stream, "rows_archived_this_run": self._rows_archived,
                    "files_written_this_run": self._files_written, "last_flush_seconds": round(elapsed, 4),
                    "last_redis_id": last_id, "updated_at": datetime.now(timezone.utc).isoformat()}
        if self.status_dir:
            try:
                self.status_dir.mkdir(parents=True, exist_ok=True)
                path = self.status_dir / f"{self.stream.replace(':', '_')}.json"
                temp = path.with_suffix(".tmp")
                temp.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
                temp.replace(path)
            except OSError as error:
                print(f"[ARCHIVER] status update failed for {self.stream}: {error}", flush=True)
        if time.time() - self._last_report >= 30:
            print(f"[ARCHIVER] {self.stream} rows={self._rows_archived} files={self._files_written} "
                  f"flush_sec={elapsed:.3f}", flush=True)
            self._last_report = time.time()

    # ---------------------------
    # Buffering + ACK
    # ---------------------------

    def _flush(self) -> None:
        if not self._buf_rows:
            self._last_flush = time.time()
            return

        # Write first; ACK only if write succeeds
        started = time.perf_counter()
        with getattr(self, "write_semaphore", None) or nullcontext():
            self._write_batch(self._buf_rows)

        # ACK IDs
        if self._buf_ids:
            self.r.xack(self.stream, self.group, *self._buf_ids)
            if self.delete_after_ack:
                # Optional cleanup (usually not required)
                self.r.xdel(self.stream, *self._buf_ids)

        archived = len(self._buf_rows)
        last_id = self._buf_ids[-1] if self._buf_ids else None
        self._buf_rows.clear()
        self._buf_ids.clear()
        self._buffer_bytes = 0
        self._last_flush = time.time()
        self._rows_archived = getattr(self, "_rows_archived", 0) + archived
        if hasattr(self, "status_dir"):
            self._report(time.perf_counter() - started, last_id)

    def _ingest_messages(self, resp) -> int:
        n = 0
        for _stream_name, msgs in resp:
            for msg_id, fields in msgs:
                if not fields:
                    raise ArchiveDataLossError(f"{self.stream}: pending record {_decode(msg_id)} was removed from Redis before archiving")
                row = _decode_dict(fields)
                row["_redis_id"] = _decode(msg_id)
                row["_stream"] = self.stream
                row["ts_recv"] = self._receive_ms(row)
                self._buf_rows.append(row)
                self._buf_ids.append(_decode(msg_id))
                self._buffer_bytes = getattr(self, "_buffer_bytes", 0) + sys.getsizeof(row) + sum(
                    sys.getsizeof(key) + sys.getsizeof(value) for key, value in row.items())
                n += 1
                if len(self._buf_rows) >= self.batch_size or self._buffer_bytes >= getattr(self, "max_buffer_bytes", 16 * 1024 * 1024):
                    self._flush()
        return n

    # ---------------------------
    # Main loop
    # ---------------------------

    def run_forever(self, stop_event: Optional[threading.Event] = None) -> None:
        self._stop_event = stop_event
        print(
            f"[ARCHIVER] running stream={self.stream} group={self.group} consumer={self.consumer} "
            f"batch_size={self.batch_size} flush_sec={self.flush_sec}"
        )

        # 1) Drain pending (if any) first
        cursor = "0"
        while True:
            resp = self._xreadgroup(cursor)
            got = self._ingest_messages(resp) if resp else 0
            if got == 0:
                break
            # Advance the pending cursor while batching, instead of replaying
            # the same page or writing thousands of small parts during recovery.
            cursor = _decode(next(msgs[-1][0] for _, msgs in reversed(resp) if msgs))
            if (time.time() - self._last_flush) >= self.flush_sec:
                self._flush()

        self._flush()

        # Recover messages abandoned under another consumer name. The configured
        # idle threshold avoids claiming work from a recently active consumer.
        if getattr(self, "claim_idle_ms", 0):
            claim_cursor = "0-0"
            while True:
                reply = self.r.xautoclaim(self.stream, self.group, self.consumer,
                                         self.claim_idle_ms, start_id=claim_cursor, count=self.read_count)
                claim_cursor, messages = reply[:2]
                if len(reply) > 2 and reply[2]:
                    raise ArchiveDataLossError(f"{self.stream}: pending records were trimmed before recovery: {reply[2]}")
                if messages:
                    self._ingest_messages([(self.stream, messages)])
                if _decode(claim_cursor) == "0-0":
                    break
            self._flush()

        # 2) Tail new messages forever
        while True:
            resp = self._xreadgroup(">")
            if resp:
                self._ingest_messages(resp)

            # The launcher stops producers/analytics before requesting this.
            # Drain every remaining stream entry, then flush the final batch.
            if stop_event is not None and stop_event.is_set() and not self._buf_has_messages(resp):
                self._flush()
                pending = self.r.xpending(self.stream, self.group)
                if isinstance(pending, dict) and pending.get("pending", 0):
                    raise RuntimeError(f"{self.stream}: unarchived pending records remain; retrying recovery")
                return

            # flush conditions
            if len(self._buf_rows) >= self.batch_size:
                self._flush()
            elif (time.time() - self._last_flush) >= self.flush_sec:
                self._flush()

    @staticmethod
    def _buf_has_messages(resp) -> bool:
        return bool(resp and any(msgs for _, msgs in resp))
