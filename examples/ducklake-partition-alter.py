"""Set a DuckLake table's partitioning to hour, then load one more hour and count the files each load wrote.

DuckLake applies the new partitioning to new data only. Inlining is off, so every load is a data file.
"""

from lib.lakehouse import (DUCKLAKE_ALIAS, NAMESPACE, TABLE, connect_ducklake, create_ducklake_table, ducklake_file_count,
                           ducklake_file_paths, new_warehouse, read_cut_hour, safe_error)


def load(con, hour):
    con.register("batch", read_cut_hour(hour))
    con.execute(f"INSERT INTO {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE} SELECT * FROM batch")
    con.unregister("batch")


def main():
    try:
        with new_warehouse() as wh:
            con = connect_ducklake(wh.con)
            create_ducklake_table(con, wh.root)
            for hour in range(3):
                load(con, hour)
            print(f"data files before the change: {ducklake_file_count(con, DUCKLAKE_ALIAS, TABLE)}")
            before = ducklake_file_paths(con, DUCKLAKE_ALIAS, TABLE)

            con.execute(f"ALTER TABLE {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE} SET PARTITIONED BY (hour(hour_utc))")
            load(con, 3)
            print(f"data files after one load under hour: {ducklake_file_count(con, DUCKLAKE_ALIAS, TABLE)}")
            after = ducklake_file_paths(con, DUCKLAKE_ALIAS, TABLE)
            print(f"files kept from before the change: {len(before & after)}")

            for key, transform in con.execute(
                "SELECT partition_key_index, transform FROM __ducklake_metadata_lake.ducklake_partition_column ORDER BY 1"
            ).fetchall():
                print(f"partition key {key}: transform {transform}")
            rows = con.execute(f"SELECT count(*) FROM {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}").fetchall()
            print(f"rows in table: {rows[0][0]}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
