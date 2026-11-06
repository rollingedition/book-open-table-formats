"""Merge six small DuckLake data files into one, and read a snapshot from before the merge."""

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
        create_ducklake_table(lake, wh.root)
        table = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
        for hour in range(6):
            lake.register("hour", read_cut_hour(hour))
            lake.execute(f"INSERT INTO {table} SELECT * FROM hour")
            lake.unregister("hour")
        before = lake.execute(f"SELECT max(snapshot_id) FROM ducklake_snapshots('{DUCKLAKE_ALIAS}')").fetchall()[0][0]
        print(f"data files before merge: {ducklake_file_count(lake, DUCKLAKE_ALIAS, TABLE)}")

        result = lake.execute(
            f"CALL ducklake_merge_adjacent_files('{DUCKLAKE_ALIAS}', '{TABLE}', schema => '{NAMESPACE}')"
        ).fetchall()
        print(f"files merged: {result[0][2]}")
        print(f"files created: {result[0][3]}")
        print(f"data files after merge: {ducklake_file_count(lake, DUCKLAKE_ALIAS, TABLE)}")
        print(f"rows after merge: {lake.execute(f'SELECT count(*) FROM {table}').fetchall()[0][0]}")
        old = lake.execute(f"SELECT count(*) FROM {table} AT (VERSION => {before})").fetchall()[0][0]
        print(f"rows at the snapshot before merge: {old}")
        parquet = list((wh.root / f"{DUCKLAKE_ALIAS}_data").rglob("*.parquet"))
        print(f"parquet files on disk after merge: {len(parquet)}")


if __name__ == "__main__":
    main()
