"""Repeat `ducklake-delete-files` with DuckLake's default inlining, to show that the small delete writes no delete file. The point rests on the delete-file line after the small delete."""

from lib.lakehouse import DUCKLAKE_ALIAS, NAMESPACE, TABLE, connect_ducklake, create_ducklake_table, new_warehouse, read_cut_hour

LOW_VIEWS = 180  # few rows of hour 02 fall below this
VERY_LOW_VIEWS = 1000  # most of the table's rows fall below this


def delete_files(con):
    # one row per data file, so the rows with a delete file count the files a delete touched
    return con.execute(f"SELECT count(*) FROM ducklake_list_files('{DUCKLAKE_ALIAS}', '{TABLE}', schema => '{NAMESPACE}') "
                       "WHERE delete_file IS NOT NULL").fetchall()[0][0]


def main():
    with new_warehouse() as wh:
        con = connect_ducklake(wh.con)
        # None keeps DuckLake's default inlining; ducklake-delete-files passes 0 to turn it off
        create_ducklake_table(con, wh.root, inlining_row_limit=None)
        table = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
        for hour in range(6):
            con.register("hour", read_cut_hour(hour))
            con.execute(f"INSERT INTO {table} SELECT * FROM hour")
            con.unregister("hour")

        con.execute(f"DELETE FROM {table} WHERE hour_utc = TIMESTAMP '2024-06-03 02:00:00' AND views < {LOW_VIEWS}")
        print("rows left after the small delete:", con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0])
        print("delete files after the small delete:", delete_files(con))

        # no hour filter, so this delete reaches all six hours
        con.execute(f"DELETE FROM {table} WHERE views < {VERY_LOW_VIEWS}")
        print("rows left after the large delete:", con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0])
        print("delete files after the large delete:", delete_files(con))


if __name__ == "__main__":
    main()
