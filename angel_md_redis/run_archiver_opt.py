from app.archiver import StreamParquetArchiver

def main():
    StreamParquetArchiver(
        stream="md:ticks:opt",
        group="archive",
        consumer="arch-opt-1",
        out_dir="data_lake",
        batch_size=8000,
        flush_sec=10,
        partition_by_symbol=True,
    ).run_forever()

if __name__ == "__main__":
    main()
