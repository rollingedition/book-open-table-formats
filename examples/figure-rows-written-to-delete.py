"""Runs the delete of delete-low-views through Iceberg, Delta Lake and DuckLake and draws the rows each format wrote to remove the same rows. The point rests on the rows-written lines: copy-on-write rewrites the rest of the hour 02 file, and DuckLake's delete file lists only the positions of the removed rows."""

import datetime

import pyarrow.parquet as pq
from deltalake import DeltaTable
from pyiceberg.expressions import And, EqualTo, LessThan

from lib.figures import ACCENT, MUTED, new_figure, save
from lib.lakehouse import DUCKLAKE_ALIAS, NAMESPACE, TABLE, load_three_formats, new_warehouse, parquet_files, read_delta, scan_arrow

LOW_VIEWS = 180  # the threshold of delete-low-views: it catches a few rows of hour 02


def swap(before, after):
    added, removed = after - before, before - after
    assert len(added) == 1 and len(removed) == 1  # one file in, one file out
    return [sum(pq.ParquetFile(path.removeprefix("file://")).metadata.num_rows for path in files) for files in (added, removed)]  # the footers' counts, read without loading the rows


def main():
    with new_warehouse() as wh:
        iceberg, delta_path, lake = load_three_formats(wh, range(6))  # hours 00 to 05, one file per hour in each format

        # Iceberg replaces the hour 02 file with one new file; the removed file stays on disk until its snapshots expire
        ice_rows, ice_before = scan_arrow(iceberg).num_rows, set(parquet_files(iceberg))
        iceberg.delete(And(EqualTo("hour_utc", datetime.datetime(2024, 6, 3, 2)), LessThan("views", LOW_VIEWS)))
        iceberg_written, file_rows = swap(ice_before, set(parquet_files(iceberg)))
        print("hour 02 file rows before the delete:", file_rows)
        print("Iceberg deleted rows:", ice_rows - scan_arrow(iceberg).num_rows)
        print("Iceberg rows written by the delete:", iceberg_written)

        # Delta Lake's log holds each file as a file:// URI
        delta_rows, delta_before = read_delta(delta_path).num_rows, set(DeltaTable(delta_path).file_uris())
        DeltaTable(delta_path).delete(f"hour_utc = '2024-06-03T02:00:00' AND views < {LOW_VIEWS}")
        delta_written, _ = swap(delta_before, set(DeltaTable(delta_path).file_uris()))
        print("Delta Lake deleted rows:", delta_rows - read_delta(delta_path).num_rows)
        print("Delta Lake rows written by the delete:", delta_written)

        # DuckLake writes one delete file, which lists positions
        lake_rows = lake.execute(f"SELECT count(*) FROM {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}").fetchall()[0][0]
        lake.execute(f"DELETE FROM {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE} WHERE hour_utc = TIMESTAMP '2024-06-03 02:00:00' AND views < {LOW_VIEWS}")
        [(delete_file,)] = lake.execute(f"SELECT delete_file FROM ducklake_list_files('{DUCKLAKE_ALIAS}', '{TABLE}', schema => '{NAMESPACE}') WHERE delete_file IS NOT NULL").fetchall()
        assert pq.read_schema(delete_file).names == ["file_path", "pos"]  # one row per removed position
        lake_written = lake.execute(f"SELECT count(*) FROM read_parquet('{delete_file}')").fetchall()[0][0]
        print("DuckLake deleted rows:", lake_rows - lake.execute(f"SELECT count(*) FROM {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}").fetchall()[0][0])
        print("DuckLake positions written by the delete:", lake_written)

        fig, ax = new_figure(height=2.4)
        # barh draws the first label at the bottom; each bar's label names the unit it counts
        ax.bar_label(ax.barh(["DuckLake, merge-on-read", "Delta Lake, copy-on-write", "Iceberg, copy-on-write"], [lake_written, delta_written, iceberg_written], color=[MUTED, ACCENT, ACCENT]),
                     labels=[f"{lake_written} positions", f"{delta_written} rows", f"{iceberg_written} rows"], padding=4)
        # room for the value labels, with copy-on-write the longest bar; the axis names both units, rows and positions
        ax.set(xlim=(0, iceberg_written * 1.25), xlabel="rows of data, or positions in a delete file, written")
        ax.grid(False)  # the value labels carry the values, so no grid line crosses a bar
        save(fig, "rows-written-to-delete")


if __name__ == "__main__":
    main()
