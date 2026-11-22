"""Runs the delete of delete-low-views through Iceberg, Delta Lake and DuckLake and draws the rows each format wrote to remove the same rows. Copy-on-write writes the rows of the file that replaces the rewritten one; DuckLake writes a delete file that lists the removed rows."""

import datetime

import pyarrow.parquet as pq
from deltalake import DeltaTable
from pyiceberg.expressions import And, EqualTo, LessThan

from lib.figures import ACCENT, INK, MUTED, new_figure, save
from lib.lakehouse import DUCKLAKE_ALIAS, NAMESPACE, TABLE, ducklake_file_paths, load_three_formats, new_warehouse, parquet_files, read_delta, scan_arrow

LOW_VIEWS = 180  # the threshold of delete-low-views: it catches a few rows of hour 02


def main():
    with new_warehouse() as wh:
        iceberg, delta_path, lake = load_three_formats(wh, range(6))  # hours 00 to 05, one file per hour in each format
        table = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
        print("hour 02 rows before the delete:", lake.execute(f"SELECT count(*) FROM {table} WHERE hour_utc = TIMESTAMP '2024-06-03 02:00:00'").fetchall()[0][0])

        # Iceberg writes one new data file in place of the rewritten one, so the rows written are that file's rows
        ice_rows, ice_files = scan_arrow(iceberg).num_rows, set(parquet_files(iceberg))
        iceberg.delete(And(EqualTo("hour_utc", datetime.datetime(2024, 6, 3, 2)), LessThan("views", LOW_VIEWS)))
        ice_new = set(parquet_files(iceberg)) - ice_files
        iceberg_written = pq.ParquetFile(ice_new.pop()).metadata.num_rows  # the footer's count, read without loading the rows
        print("Iceberg deleted rows:", ice_rows - scan_arrow(iceberg).num_rows)
        print("Iceberg rows written by the delete:", iceberg_written)

        # Delta Lake: the new file's URI is in the log, as a file:// URI
        delta_rows, delta_files = read_delta(delta_path).num_rows, set(DeltaTable(delta_path).file_uris())
        DeltaTable(delta_path).delete(f"hour_utc = '2024-06-03T02:00:00' AND views < {LOW_VIEWS}")
        delta_new = set(DeltaTable(delta_path).file_uris()) - delta_files
        delta_written = sum(pq.ParquetFile(uri.removeprefix("file://")).metadata.num_rows for uri in delta_new)
        print("Delta Lake deleted rows:", delta_rows - read_delta(delta_path).num_rows)
        print("Delta Lake rows written by the delete:", delta_written)

        # DuckLake writes a delete file and leaves every data file in place
        lake_rows, data_files = lake.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0], ducklake_file_paths(lake, DUCKLAKE_ALIAS, TABLE)
        lake.execute(f"DELETE FROM {table} WHERE hour_utc = TIMESTAMP '2024-06-03 02:00:00' AND views < {LOW_VIEWS}")
        assert ducklake_file_paths(lake, DUCKLAKE_ALIAS, TABLE) == data_files
        delete_files = lake.execute(f"SELECT delete_file FROM ducklake_list_files('{DUCKLAKE_ALIAS}', '{TABLE}', schema => '{NAMESPACE}') "
                                    "WHERE delete_file IS NOT NULL").fetchall()
        lake_written = sum(lake.execute(f"SELECT count(*) FROM read_parquet('{row[0]}')").fetchall()[0][0] for row in delete_files)
        print("DuckLake deleted rows:", lake_rows - lake.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0])
        print("DuckLake rows written by the delete:", lake_written)

        fig, ax = new_figure(height=2.4)
        labels, values = ["DuckLake, merge-on-read", "Delta Lake, copy-on-write", "Iceberg, copy-on-write"], [lake_written, delta_written, iceberg_written]  # barh draws the first at the bottom
        bars = ax.barh(labels, values, color=[ACCENT, MUTED, INK])  # the merge-on-read format is the accent
        ax.bar_label(bars, labels=[f"{value} rows" for value in values], padding=4)  # each bar names its value
        ax.set_xlim(0, max(values) * 1.25)  # room for the value labels
        save(fig, "rows-written-to-delete")


if __name__ == "__main__":
    main()
