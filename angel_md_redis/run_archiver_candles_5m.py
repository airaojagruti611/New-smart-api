from app.archiver import StreamParquetArchiver


def main():
    StreamParquetArchiver(
        stream="md:candles:5m",
        group="archive",
        consumer="arch-candles-5m-1",
        out_dir="data_lake",
        batch_size=2000,
        flush_sec=10,
        partition_by_symbol=True,
    ).run_forever()


if __name__ == "__main__":
    main()

