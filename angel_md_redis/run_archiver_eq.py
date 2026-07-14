from app.archiver import StreamParquetArchiver

def main():
    StreamParquetArchiver(
        stream="md:ticks:eq",
        group="archive",
        consumer="arch-eq-1",
        out_dir="data_lake",
        batch_size=5000,
        flush_sec=10,
        partition_by_symbol=True,
    ).run_forever()

if __name__ == "__main__":
    main()
