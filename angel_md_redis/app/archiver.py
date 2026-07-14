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
          - '0' to read pending (PEL)
        """
        return self.r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.stream: stream_id},
            count=self.read_count,
            block=self.block_ms if stream_id == ">" else 0,
        )

    # ---------------------------
    # Parquet writing
    # ---------------------------

    def _append_parquet(self, folder: Path, df: pd.DataFrame) -> None:
        folder.mkdir(parents=True, exist_ok=True)

        # atomic-ish write: write tmp then rename
        ts = int(time.time() * 1000)
        tmp_path = folder / f".tmp-part-{ts}.parquet"
        final_path = folder / f"part-{ts}.parquet"

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, tmp_path, compression=self.compression)
        tmp_path.replace(final_path)

    def _write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        df = pd.DataFrame(rows)

        # Ensure ts_recv exists and is numeric
        if "ts_recv" not in df.columns:
            df["ts_recv"] = int(time.time() * 1000)
        df["ts_recv"] = pd.to_numeric(df["ts_recv"], errors="coerce").fillna(int(time.time() * 1000)).astype("int64")

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

    def _ingest_messages(self, resp) -> int:
        n = 0
        for _stream_name, msgs in resp:
            for msg_id, fields in msgs:
                row = _decode_dict(fields)
                row["_redis_id"] = _decode(msg_id)
                row["_stream"] = self.stream
                self._buf_rows.append(row)
                self._buf_ids.append(_decode(msg_id))
                n += 1
        return n

    # ---------------------------
    # Main loop
    # ---------------------------

    def run_forever(self) -> None:
        print(
            f"[ARCHIVER] running stream={self.stream} group={self.group} consumer={self.consumer} "
            f"batch_size={self.batch_size} flush_sec={self.flush_sec}"
        )

        # 1) Drain pending (if any) first
        while True:
            resp = self._xreadgroup("0")
            got = self._ingest_messages(resp) if resp else 0
            if got == 0:
                break
            if len(self._buf_rows) >= self.batch_size:
                self._flush()

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
