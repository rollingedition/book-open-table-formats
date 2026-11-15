"""Runs one delete through Iceberg, Delta Lake and DuckLake and prints what each format removed and rewrote. The data-file lines carry the point; the deleted-row lines beside them are for comparison."""

from datetime import datetime

from deltalake import DeltaTable
from pyiceberg.expressions import And, EqualTo, LessThan

from lib.lakehouse import DUCKLAKE_ALIAS, NAMESPACE, TABLE, ducklake_file_count, load_three_formats, new_warehouse, read_delta, safe_error, scan_arrow

HOUR_02 = datetime(2024, 6, 3, 2)
# A threshold, not a fact about the data: the cut keeps the most-viewed rows of each hour, so a small one would match nothing.
LOW_VIEWS = 180


def main():
    with new_warehouse() as wh:
        iceberg, delta_path, lake = load_three_formats(wh, range(6))  # hours 00 to 05, one commit per hour in each format
        iceberg_before = scan_arrow(iceberg).num_rows
        delta_before = read_delta(delta_path).num_rows
        table = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
        lake_before = lake.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
        files_before = ducklake_file_count(lake, DUCKLAKE_ALIAS, TABLE)  # counted before the delete, to compare with the count after

        iceberg.delete(And(EqualTo("hour_utc", HOUR_02), LessThan("views", LOW_VIEWS)))
        summary = iceberg.current_snapshot().summary  # the snapshot summary counts each data file the delete removed from the table
        print("Iceberg deleted rows:", iceberg_before - scan_arrow(iceberg).num_rows)
        print("Iceberg remaining rows:", scan_arrow(iceberg).num_rows)
        print("Iceberg data files rewritten:", summary["deleted-data-files"])

        delta = DeltaTable(delta_path)
        # the hour is written as a timestamp string inside the SQL predicate
        metrics = delta.delete(f"hour_utc = '2024-06-03T02:00:00' AND views < {LOW_VIEWS}")
        print("Delta Lake deleted rows:", delta_before - read_delta(delta_path).num_rows)
        print("Delta Lake remaining rows:", read_delta(delta_path).num_rows)
        print("Delta Lake data files rewritten:", metrics["num_removed_files"])

        lake.execute(f"DELETE FROM {table} WHERE hour_utc = TIMESTAMP '2024-06-03 02:00:00' AND views < {LOW_VIEWS}")
        lake_after = lake.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
        print("DuckLake deleted rows:", lake_before - lake_after)
        print("DuckLake remaining rows:", lake_after)
        print("DuckLake data files before:", files_before)
        print("DuckLake data files after:", ducklake_file_count(lake, DUCKLAKE_ALIAS, TABLE))
        # a delete file is listed against the data file it applies to, so counting the listed rows that have one counts the delete files
        print("DuckLake delete files written:", lake.execute(
            f"SELECT count(*) FROM ducklake_list_files('{DUCKLAKE_ALIAS}', '{TABLE}', schema => '{NAMESPACE}') "
            "WHERE delete_file IS NOT NULL").fetchall()[0][0])


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # the redacted error is what a reader sees
        print(safe_error(exc))
        raise SystemExit(1)
