"""Calls set_column_metadata with a name key on a Delta Lake table with column mapping on, then reads the names back and queries
under the new name. The columns read back line and the binder error are the lines to read.
"""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import new_warehouse, read_cut_hour, read_delta, safe_error, sql_rows

with new_warehouse() as wh:
    path = f"{wh.root}/delta/pageviews_hourly"
    # column mapping on: the protocol's route to a rename, which this release does not offer as a call
    write_deltalake(path, read_cut_hour(0),
                    configuration={"delta.columnMapping.mode": "name"})
    print(f"version before the rename: {DeltaTable(path).version()}")
    # the call raises nothing and writes a commit, so the names read back below are the only check
    DeltaTable(path).alter.set_column_metadata("page_title", {"name": "article_title"})
    print("set_column_metadata returned")
    table = DeltaTable(path)
    print(f"version after the rename: {table.version()}")
    page = next(field for field in table.schema().fields if field.name == "page_title")
    # the name key the call wrote into the field's metadata, which the reader does not use for the column name
    print(f"name in page_title metadata after the rename: {page.metadata['name']}")
    print(f"columns read back: {', '.join(read_delta(path).column_names)}")
    try:
        sql_rows(wh.con, "SELECT article_title FROM t", t=read_delta(path))
    except Exception as exc:
        print(safe_error(exc, wh))
