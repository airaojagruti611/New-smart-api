import sys

from app.csv_archiver import StreamCsvArchiver


STREAMS = {
    # Market regime snapshots
    "regime": ("md:regime", "arch-regime-csv-1", 5000, False),
    # 1-minute confirmed volume signals (one row per symbol per closed candle)
    "volume": ("md:volume:signal", "arch-volume-csv-1", 8000, True),
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in STREAMS:
        print("Usage: python run_archiver_signals_csv.py [regime|volume]")
        raise SystemExit(1)

    key = sys.argv[1]
    stream, consumer, batch, part_by_symbol = STREAMS[key]

    StreamCsvArchiver(
        stream=stream,
        group="archive_csv",
        consumer=consumer,
        out_dir="data_lake",
        batch_size=batch,
        flush_sec=10,
        partition_by_symbol=part_by_symbol,
    ).run_forever()


if __name__ == "__main__":
    main()

