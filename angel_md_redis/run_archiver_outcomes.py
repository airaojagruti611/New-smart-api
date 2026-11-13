from app.csv_archiver import StreamCsvArchiver


def main() -> None:
    StreamCsvArchiver(
        stream="md:journal:outcome",
        group="archive_csv",
        consumer="arch-outcome-csv-1",
        out_dir="data_lake",
        batch_size=2000,
        flush_sec=10,
        partition_by_symbol=False,
    ).run_forever()


if __name__ == "__main__":
    main()
