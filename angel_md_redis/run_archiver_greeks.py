from app.archiver import StreamParquetArchiver

def main():
    StreamParquetArchiver(
        stream="md:greeks:snap",
        group="archive",
        consumer="arch-greeks-1",
        out_dir="data_lake",
        batch_size=2000,
        flush_sec=10,
        partition_by_symbol=True,   # will partition by underlying if present
    ).run_forever()

if __name__ == "__main__":
    main()
