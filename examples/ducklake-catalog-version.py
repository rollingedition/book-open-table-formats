"""Attach a DuckLake catalog and print its catalog version and the version of the DuckLake extension that DuckDB loaded."""

from lib.lakehouse import attach_ducklake, connect_ducklake, new_warehouse


def main():
    with new_warehouse() as wh:
        con = connect_ducklake(wh.con)
        attach_ducklake(con, "probe", wh.root)
        version = con.execute("SELECT value FROM __ducklake_metadata_probe.ducklake_metadata WHERE key = 'version'").fetchall()[0][0]
        print("ducklake catalog version:", version)
        extension = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'").fetchall()[0][0]
        print("ducklake extension version:", extension)


if __name__ == "__main__":
    main()
