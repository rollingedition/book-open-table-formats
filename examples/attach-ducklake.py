"""Attach a DuckLake catalog file, detach it, and attach it again from the file alone. The point rests on the
'rows after re-attach' line, and the table list shows what the catalog holds, not what the data folder holds."""

from lib.lakehouse import (DUCKLAKE_ALIAS, NAMESPACE, attach_ducklake, connect_ducklake,
                           create_ducklake_domains, new_warehouse, print_rows, sql_rows)

with new_warehouse() as wh:
    con = connect_ducklake(wh.con)
    create_ducklake_domains(con, wh.root)                # inlining is off, so the rows become Parquet files
    print("rows written:", sql_rows(con, f"SELECT count(*) FROM {DUCKLAKE_ALIAS}.{NAMESPACE}.domains")[0][0])

    # Detach, then attach with no DATA_PATH: the catalog file stores the data path.
    con.execute(f"DETACH {DUCKLAKE_ALIAS}")
    attach_ducklake(con, DUCKLAKE_ALIAS, wh.root)
    print("tables in the catalog:")
    print_rows(sql_rows(con, f"""SELECT schema_name, table_name FROM duckdb_tables()
                                 WHERE database_name = '{DUCKLAKE_ALIAS}' AND schema_name = '{NAMESPACE}'"""))
    print("rows after re-attach:", sql_rows(con, f"SELECT count(*) FROM {DUCKLAKE_ALIAS}.{NAMESPACE}.domains")[0][0])
