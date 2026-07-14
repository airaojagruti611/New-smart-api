from app.archiver import StreamParquetArchiver

def main():
    StreamParquetArchiver(
        stream="md:features:opt",
        group="archive",
        consumer="arch-features-1",
        out_dir="data_lake",
        batch_size=5000,
        flush_sec=10,
        partition_by_symbol=True,   # partitions by underlying/symbol
    ).run_forever()

if __name__ == "__main__":
    main()
