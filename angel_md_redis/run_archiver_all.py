import sys
from app.archiver import StreamParquetArchiver

STREAMS = {
    "eq": ("md:ticks:eq", "arch-eq-1", 5000),
    "opt": ("md:ticks:opt", "arch-opt-1", 8000),
    "greeks": ("md:greeks:snap", "arch-greeks-1", 2000),
    "features": ("md:features:opt", "arch-features-1", 5000),
}

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in STREAMS:
        print("Usage: python run_archiver_all.py [eq|opt|greeks|features]")
        raise SystemExit(1)

    key = sys.argv[1]
    stream, consumer, batch = STREAMS[key]

    StreamParquetArchiver(
        stream=stream,
        group="archive",
        consumer=consumer,
        out_dir="data_lake",
        batch_size=batch,
        flush_sec=10,
        partition_by_symbol=True,
    ).run_forever()

if __name__ == "__main__":
    main()
