"""Change a Delta Lake table's partition columns from day to hour, which rewrites the table.

The table holds three hourly loads with day and hour as plain columns. Delta numbers versions from 0.
"""

import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import delta_file_count, new_warehouse, read_cut_hour, read_delta, safe_error


def with_partition_columns(rows):
    """Add the day and hour columns that Delta partitions by. Delta partitions only by plain columns."""
    rows = rows.append_column("day", pc.strftime(rows["hour_utc"], format="%Y-%m-%d"))
    return rows.append_column("hour", pc.strftime(rows["hour_utc"], format="%H"))


def main():
    try:
        with new_warehouse() as wh:
            path = f"{wh.root}/delta/pageviews_hourly"
            for hour in range(3):
                write_deltalake(path, with_partition_columns(read_cut_hour(hour)), partition_by=["day"], mode="append")
            table = DeltaTable(path)
            print(f"partition columns before: {table.metadata().partition_columns}")
            print(f"version before: {table.version()}")
            print(f"data files before: {delta_file_count(table)}")

            rows = read_delta(path)
            try:
                write_deltalake(path, rows, partition_by=["hour"], mode="overwrite")
            except Exception as exc:
                print(f"change without schema mode: {safe_error(exc, wh)}")

            write_deltalake(path, rows, partition_by=["hour"], mode="overwrite", schema_mode="overwrite")
            table = DeltaTable(path)
            print(f"partition columns after: {table.metadata().partition_columns}")
            print(f"version after: {table.version()}")
            print(f"data files after: {delta_file_count(table)}")
            print(f"rows after: {read_delta(path).num_rows}")

            old = DeltaTable(path, version=2)
            print(f"partition columns in version 2: {old.metadata().partition_columns}")
            print(f"rows in version 2: {read_delta(path, version=2).num_rows}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
