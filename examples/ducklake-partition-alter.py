"""Set a DuckLake table's partitioning to hour, then load one more hour and count the files before and after it.

The files-kept line is the one to check: it counts the earlier files still listed after the hour load.
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
            # the helper leaves inlining off by default, so every load below writes one data file
            create_ducklake_table(con, wh.root)
            for hour in range(3):  # hours 00 to 02, before the change
                load(con, hour)
            print(f"data files before the change: {ducklake_file_count(con, DUCKLAKE_ALIAS, TABLE)}")
            before = ducklake_file_paths(con, DUCKLAKE_ALIAS, TABLE)

            con.execute(f"ALTER TABLE {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE} SET PARTITIONED BY (hour(hour_utc))")
            load(con, 3)  # hour 03, the first load after the change
            print(f"data files after one load under hour: {ducklake_file_count(con, DUCKLAKE_ALIAS, TABLE)}")
            after = ducklake_file_paths(con, DUCKLAKE_ALIAS, TABLE)
            # the intersection is the earlier files still listed after the hour load
            print(f"files kept from before the change: {len(before & after)}")

            # ordered by key index, so the keys print in a fixed order
            for key, transform in con.execute(
                "SELECT partition_key_index, transform FROM __ducklake_metadata_lake.ducklake_partition_column ORDER BY 1"
            ).fetchall():
                print(f"partition key {key}: transform {transform}")
            rows = con.execute(f"SELECT count(*) FROM {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}").fetchall()
            print(f"rows in table: {rows[0][0]}")  # count(*) returns one row with one value
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
