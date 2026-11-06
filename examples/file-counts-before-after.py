"""Compact six small Delta Lake appends into one file, and count files before and after."""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import delta_file_count, new_warehouse, read_cut_hour, read_delta


def main():
    with new_warehouse() as wh:
        path = str(wh.root / "delta" / "pageviews_hourly")
        for hour in range(6):
            write_deltalake(path, read_cut_hour(hour), mode="append")

        table = DeltaTable(path)
        version_before = table.version()
        print(f"files before compaction: {delta_file_count(table)}")

        metrics = table.optimize.compact()
        print(f"files added by compaction: {metrics['numFilesAdded']}")
        print(f"files removed by compaction: {metrics['numFilesRemoved']}")

        table = DeltaTable(path)
        print(f"files after compaction: {delta_file_count(table)}")
        print(f"rows after compaction: {len(read_delta(path))}")
        before = read_delta(path, version=version_before)
        print(f"rows at the version before compaction: {len(before)}")
        parquet = list((wh.root / "delta").rglob("*.parquet"))
        print(f"parquet files on disk after compaction: {len(parquet)}")


if __name__ == "__main__":
    main()
