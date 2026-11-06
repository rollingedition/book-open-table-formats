"""Load six hours into a DuckLake table with data inlining off, then count the delete files that a small and a large delete write."""

from lib.lakehouse import DUCKLAKE_ALIAS, NAMESPACE, TABLE, connect_ducklake, create_ducklake_table, new_warehouse, read_cut_hour

LOW_VIEWS = 180
VERY_LOW_VIEWS = 1000


def delete_files(con):
    return con.execute(f"SELECT count(*) FROM ducklake_list_files('{DUCKLAKE_ALIAS}', '{TABLE}', schema => '{NAMESPACE}') "
                       "WHERE delete_file IS NOT NULL").fetchall()[0][0]


def main():
    with new_warehouse() as wh:
        con = connect_ducklake(wh.con)
        create_ducklake_table(con, wh.root, inlining_row_limit=0)
        table = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
        for hour in range(6):
            con.register("hour", read_cut_hour(hour))
            con.execute(f"INSERT INTO {table} SELECT * FROM hour")
            con.unregister("hour")

        con.execute(f"DELETE FROM {table} WHERE hour_utc = TIMESTAMP '2024-06-03 02:00:00' AND views < {LOW_VIEWS}")
        print("rows left after the small delete:", con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0])
        print("delete files after the small delete:", delete_files(con))

        con.execute(f"DELETE FROM {table} WHERE views < {VERY_LOW_VIEWS}")
        print("rows left after the large delete:", con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0])
        print("delete files after the large delete:", delete_files(con))


if __name__ == "__main__":
    main()
