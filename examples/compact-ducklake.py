"""Merge six DuckLake data files, then read a snapshot from before the merge to see what the merge left behind.
The point rests on the data-file lines, and on the row count read at the earlier snapshot."""

from lib.lakehouse import (
    DUCKLAKE_ALIAS,
    NAMESPACE,
    TABLE,
    connect_ducklake,
    create_ducklake_table,
    ducklake_file_count,
    new_warehouse,
    read_cut_hour,
)


def main():
    with new_warehouse() as wh:
        lake = connect_ducklake(wh.con)
        create_ducklake_table(lake, wh.root)  # inlining is off here, so every insert writes a data file
        table = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
        for hour in range(6):
            lake.register("hour", read_cut_hour(hour))
            lake.execute(f"INSERT INTO {table} SELECT * FROM hour")
            lake.unregister("hour")
        # the newest snapshot before the merge, which the read at the end goes back to
        before = lake.execute(f"SELECT max(snapshot_id) FROM ducklake_snapshots('{DUCKLAKE_ALIAS}')").fetchall()[0][0]
        print(f"data files before merge: {ducklake_file_count(lake, DUCKLAKE_ALIAS, TABLE)}")

        # the schema is named because the table is not in main; the call returns one row
        result = lake.execute(
            f"CALL ducklake_merge_adjacent_files('{DUCKLAKE_ALIAS}', '{TABLE}', schema => '{NAMESPACE}')"
        ).fetchall()
        # the row's fields: schema, table, files merged, files created
        print(f"files merged: {result[0][2]}")
        print(f"files created: {result[0][3]}")
        print(f"data files after merge: {ducklake_file_count(lake, DUCKLAKE_ALIAS, TABLE)}")
        print(f"rows after merge: {lake.execute(f'SELECT count(*) FROM {table}').fetchall()[0][0]}")
        old = lake.execute(f"SELECT count(*) FROM {table} AT (VERSION => {before})").fetchall()[0][0]
        print(f"rows at the snapshot before merge: {old}")
        # every Parquet file under the data path, including the ones the table no longer lists
        parquet = list((wh.root / f"{DUCKLAKE_ALIAS}_data").rglob("*.parquet"))
        print(f"parquet files on disk after merge: {len(parquet)}")


if __name__ == "__main__":
    main()
