"""Run cleanup_all against a fresh, empty DuckLake catalog that shares a data path with a live one."""

from lib.lakehouse import (DUCKLAKE_ALIAS, NAMESPACE, connect_ducklake, create_ducklake_domains,
                           new_warehouse, sql_rows)

CLEANUP = "CALL ducklake_delete_orphaned_files('fresh', cleanup_all => true{dry})"

def parquet_count(root):
    return len(list((root / f"{DUCKLAKE_ALIAS}_data").rglob("*.parquet")))

with new_warehouse() as wh:
    con = connect_ducklake(wh.con)
    create_ducklake_domains(con, wh.root)
    print("parquet files, live catalog:", parquet_count(wh.root))

    # A replacement catalog: a new, empty file that points at the same data folder.
    con.execute(f"DETACH {DUCKLAKE_ALIAS}")
    # Attached by hand: attach_ducklake would give fresh its own data folder, not the live one.
    con.execute(f"ATTACH 'ducklake:{wh.root}/fresh.ducklake' AS fresh (DATA_PATH '{wh.root}/{DUCKLAKE_ALIAS}_data/')")
    print("files the dry run lists:", len(sql_rows(con, CLEANUP.format(dry=", dry_run => true"))))
    con.execute(CLEANUP.format(dry=""))
    print("parquet files after cleanup_all:", parquet_count(wh.root))

    # The live catalog still names the table, and its data files are gone.
    con.execute(f"ATTACH 'ducklake:{wh.root}/{DUCKLAKE_ALIAS}.ducklake' AS live")
    print("count through the live catalog:", sql_rows(con, f"SELECT count(*) FROM live.{NAMESPACE}.domains")[0][0])
    try:
        print("reading the live catalog:", len(sql_rows(con, f"SELECT domain_code, language FROM live.{NAMESPACE}.domains")))
    except Exception as exc:
        print("reading the live catalog:", type(exc).__name__)
