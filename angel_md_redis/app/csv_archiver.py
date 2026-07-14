import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


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
        print("[CSV_ARCHIVER] zoneinfo not available; falling back to UTC")
        return "UTC"
    try:
        ZoneInfo(tz_name)  # validate
        return tz_name
    except Exception:
        print(f"[CSV_ARCHIVER] invalid ARCHIVE_TZ={tz_name!r}; falling back to UTC")
        return "UTC"


class StreamCsvArchiver:
    """
    Redis Streams -> CSV "data lake" writer.

    Reads from a stream using a consumer group, buffers messages,
    writes CSV part files to disk, then XACKs those message IDs.

    Output partitioning:
      data_lake/
        stream=md_regime/
          dt=YYYY-MM-DD/
            part-<ts>.csv
        stream=md_volume_signal/
          dt=YYYY-MM-DD/
            symbol=NIFTY/
              part-<ts>.csv
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
        partition_by_symbol: bool = False,
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
        try:
            self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            print(f"[CSV_ARCHIVER] created group '{self.group}' for stream '{self.stream}'")
        except redis.exceptions.ResponseError as e:
            msg = str(e)
            if "BUSYGROUP" in msg:
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
    # CSV writing
    # ---------------------------

    def _to_dt(self, ts_ms: int) -> str:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        if self.partition_tz != "UTC":
            dt = dt.astimezone(ZoneInfo(self.partition_tz))  # type: ignore[misc]
        return dt.strftime("%Y-%m-%d")

    def _append_csv_part(self, folder: Path, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        folder.mkdir(parents=True, exist_ok=True)

        # Stable column order: metadata first, then sorted remaining keys
        meta_cols = ["ts_recv", "ts_ms", "_redis_id", "_stream"]
        keys = set()
        for r in rows:
            keys.update(r.keys())
        other_cols = sorted(k for k in keys if k not in meta_cols)
        fieldnames = [c for c in meta_cols if c in keys] + other_cols

        ts = int(time.time() * 1000)
        tmp_path = folder / f".tmp-part-{ts}.csv"
        final_path = folder / f"part-{ts}.csv"

        with tmp_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: ("" if v is None else v) for k, v in r.items()})

        tmp_path.replace(final_path)

    def _write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        # Stream partition name safe for folders
        stream_folder = f"stream={self.stream.replace(':', '_')}"

        # Optional: partition by underlying/symbol (volume signals benefit from this)
        key_col: Optional[str] = None
        if self.partition_by_symbol:
            for cand in ("underlying", "symbol"):
                if any((cand in r) for r in rows):
                    key_col = cand
                    break

        # Compute dt per row so cross-midnight batches land correctly
        by_dt: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            ts_recv = int(r.get("ts_recv") or int(time.time() * 1000))
            dt_str = self._to_dt(ts_recv)
            by_dt.setdefault(dt_str, []).append(r)

        for dt_str, part_dt in sorted(by_dt.items()):
            base = self.out_dir / stream_folder / f"dt={dt_str}"

            if key_col:
                by_key: Dict[str, List[Dict[str, Any]]] = {}
                for r in part_dt:
                    key = str(r.get(key_col) or "UNKNOWN")
                    by_key.setdefault(key, []).append(r)
                for key, part_sym in by_key.items():
                    sub = base / f"{key_col}={key}"
                    self._append_csv_part(sub, part_sym)
            else:
                self._append_csv_part(base, part_dt)

    # ---------------------------
    # Buffering + ACK
    # ---------------------------

    def _flush(self) -> None:
        if not self._buf_rows:
            self._last_flush = time.time()
            return

        # Write first; ACK only if write succeeds
        self._write_batch(self._buf_rows)

        if self._buf_ids:
            self.r.xack(self.stream, self.group, *self._buf_ids)
            if self.delete_after_ack:
                self.r.xdel(self.stream, *self._buf_ids)

        self._buf_rows.clear()
        self._buf_ids.clear()
        self._last_flush = time.time()

    def _ingest_messages(self, resp) -> int:
        n = 0
        now_ms = int(time.time() * 1000)
        for _stream_name, msgs in resp:
            for msg_id, fields in msgs:
                row = _decode_dict(fields)
                row["_redis_id"] = _decode(msg_id)
                row["_stream"] = self.stream

                # prefer producer timestamp if present; else "now"
                ts_ms = row.get("ts_ms")
                try:
                    row["ts_recv"] = int(float(ts_ms)) if ts_ms is not None else now_ms
                except Exception:
                    row["ts_recv"] = now_ms

                self._buf_rows.append(row)
                self._buf_ids.append(str(_decode(msg_id)))
                n += 1
        return n

    # ---------------------------
    # Main loop
    # ---------------------------

    def run_forever(self) -> None:
        print(
            f"[CSV_ARCHIVER] running stream={self.stream} group={self.group} consumer={self.consumer} "
            f"batch_size={self.batch_size} flush_sec={self.flush_sec} out_dir={self.out_dir}"
        )

        # 1) Drain pending first
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

            if len(self._buf_rows) >= self.batch_size:
                self._flush()
            elif (time.time() - self._last_flush) >= self.flush_sec:
                self._flush()
