"""Attach a DuckLake catalog and print its catalog version, which readers must open, and the build of the DuckLake extension that DuckDB loaded. The point rests on the first printed line."""

from lib.lakehouse import attach_ducklake, connect_ducklake, new_warehouse


def main():
    with new_warehouse() as wh:
        con = connect_ducklake(wh.con)
        # the alias also names the catalog's metadata schema, which the next line reads
        attach_ducklake(con, "probe", wh.root)
        # the version is stored as a key and value row in the metadata table; the one row, first column
        version = con.execute("SELECT value FROM __ducklake_metadata_probe.ducklake_metadata WHERE key = 'version'").fetchall()[0][0]
        print("ducklake catalog version:", version)
        # a build hash, not a release number
        extension = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'").fetchall()[0][0]
        print("ducklake extension version:", extension)


if __name__ == "__main__":
    main()
