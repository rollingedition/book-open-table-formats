"""Expire DuckLake snapshots, then clean up the files they released, in two steps."""

import shutil
from pathlib import Path

import duckdb

from lib.lakehouse import (
    DUCKLAKE_ALIAS, NAMESPACE, TABLE, connect_ducklake, create_ducklake_table,
    ducklake_file_count, ducklake_file_paths, load_ducklake_with_correction, new_warehouse,
    version_labels,
)

LAKE = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"


def listed(con, call):
    """Run a DuckLake procedure and return how many rows it lists."""
    return con.sql(f"CALL {call}").to_arrow_table().num_rows


with new_warehouse() as wh:
    con = connect_ducklake(wh.con)
    create_ducklake_table(con, wh.root)
    load_ducklake_with_correction(con)

    ids = [r[0] for r in con.execute(f"SELECT snapshot_id FROM ducklake_snapshots('{DUCKLAKE_ALIAS}') ORDER BY 1").fetchall()]
    labels = version_labels(ids, word="snapshot")
    print("snapshots before expiry:", len(ids))
    print("data files in the current snapshot:", ducklake_file_count(con, DUCKLAKE_ALIAS, TABLE))
    stray = Path(sorted(ducklake_file_paths(con, DUCKLAKE_ALIAS, TABLE))[0])
    shutil.copy(stray, stray.with_name("stray.parquet"))  # written, but no snapshot lists it

    expired = ids[:-1]  # every snapshot before the correction
    expire = f"ducklake_expire_snapshots('{DUCKLAKE_ALIAS}', versions => {expired}"
    print("expiry dry run lists:", listed(con, f"{expire}, dry_run => true)"))
    con.execute(f"CALL {expire})")
    print("remaining snapshots:", labels[ids[-1]])
    cleanup = f"ducklake_cleanup_old_files('{DUCKLAKE_ALIAS}', cleanup_all => true"
    print("cleanup dry run lists:", listed(con, f"{cleanup}, dry_run => true)"))
    print("cleanup removed:", listed(con, f"{cleanup})"))
    orphans = f"ducklake_delete_orphaned_files('{DUCKLAKE_ALIAS}', dry_run => true)"
    print("orphans listed without cleanup_all:", listed(con, orphans))
    print("rows in the current snapshot:", con.sql(f"SELECT count(*) FROM {LAKE}").fetchall()[0][0])
    try:
        con.sql(f"SELECT count(*) FROM {LAKE} AT (VERSION => {expired[-1]})").fetchall()
    except duckdb.InvalidInputException as exc:
        print(f"read {labels[expired[-1]]}: {type(exc).__name__}")
