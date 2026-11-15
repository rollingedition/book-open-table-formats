"""Commit the same three hours to Delta Lake and DuckLake, then read each commit back by its own number.
The prose's point that each hour gives the same row count in both formats rests on the paired row-count lines."""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import (DUCKLAKE_ALIAS, NAMESPACE, TABLE, attach_ducklake, connect_ducklake, new_warehouse,
                           read_cut_hour, read_delta, safe_error, snapshot_labels, sql_rows)

LAKE_TABLE = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
CATALOG_SNAPSHOTS = f"SELECT snapshot_id FROM ducklake_snapshots('{DUCKLAKE_ALIAS}')"
# Fetches snapshot_time, which turns into Python values through pytz; the pins do not install pytz, so this fails on purpose.
TIME_COLUMN = f"SELECT snapshot_time FROM ducklake_snapshots('{DUCKLAKE_ALIAS}')"


def main():
    with new_warehouse() as wh:
        delta_path = str(wh.root / "delta" / TABLE)
        connect_ducklake(wh.con)
        attach_ducklake(wh.con, DUCKLAKE_ALIAS, wh.root)
        wh.con.execute(f"CREATE SCHEMA {DUCKLAKE_ALIAS}.{NAMESPACE}")
        # A zero-row slice of hour 00: it gives the table its columns and types, and no rows.
        sql_rows(wh.con, f"CREATE TABLE {LAKE_TABLE} AS SELECT * FROM empty", empty=read_cut_hour(0).slice(0, 0))
        for hour in range(3):                        # hours 00, 01 and 02, one commit to each table per hour
            batch = read_cut_hour(hour)
            write_deltalake(delta_path, batch, mode="append")
            sql_rows(wh.con, f"INSERT INTO {LAKE_TABLE} SELECT * FROM batch", batch=batch)

        # Keeps only the snapshots that inserted rows, so the schema and table creations are left out.
        inserts = sorted(row[0] for row in sql_rows(wh.con, CATALOG_SNAPSHOTS + " WHERE map_contains(changes, 'tables_inserted_into')"))
        # Snapshot IDs count up with each change, so these are the snapshots made before the first insert.
        before = sql_rows(wh.con, CATALOG_SNAPSHOTS + f" WHERE snapshot_id < {inserts[0]}")
        print(f"snapshots before the first insert: {len(before)}")

        # Labels count every DuckLake snapshot from 1, setup snapshots included, not DuckLake's own numbers.
        lake_labels = snapshot_labels(sorted(row[0] for row in sql_rows(wh.con, CATALOG_SNAPSHOTS)))
        delta_versions = list(range(DeltaTable(delta_path).version() + 1))     # Delta versions run from 0 to the latest
        # zip pairs each hour's Delta version with its DuckLake insert, in order; strict fails if the counts differ.
        for hour, (delta_version, snapshot) in enumerate(zip(delta_versions, inserts, strict=True)):
            # DuckLake reads by the snapshot ID from ducklake_snapshots, never by counting the inserts.
            lake = sql_rows(wh.con, f"SELECT count(*) FROM {LAKE_TABLE} AT (VERSION => {snapshot})")[0][0]
            rows = len(read_delta(delta_path, version=delta_version))     # read_delta goes through QueryBuilder, because to_pyarrow_table() aborted at exit in some test runs at these pins.
            print(f"after hour {hour:02d}, Delta version {delta_version} rows: {rows}")
            print(f"after hour {hour:02d}, DuckLake {lake_labels[snapshot]} rows: {lake}")

        try:
            wh.con.execute(TIME_COLUMN).fetchall()
        except Exception as exc:
            print(f"select time: {safe_error(exc, wh)}")


if __name__ == "__main__":
    main()
