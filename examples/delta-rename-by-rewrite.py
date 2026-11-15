"""Rename a Delta Lake column by rewriting the table, then read the old version back.
The prose's point rests on the columns before and after lines: version 2 still reads page_title, and version 3, the rewrite,
reads article_title. Older versions keep the names they were written with. Delta numbers versions from 0.
"""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import new_warehouse, read_cut_hour, read_delta

with new_warehouse() as wh:
    path = f"{wh.root}/delta/pageviews_hourly"
    for hour in range(3):
        write_deltalake(path, read_cut_hour(hour), mode="append")
    print(f"data files before the rewrite: {len(DeltaTable(path).file_uris())}")
    before = DeltaTable(path).version()  # read first, so the old version can be read back after the rewrite
    print(f"version before the rewrite: {before}")
    print(f"columns before the rewrite: {', '.join(read_delta(path, version=before).column_names)}")

    # the new names, in the table's column order: only the third one changes
    renamed = read_delta(path).rename_columns(["hour_utc", "domain_code", "article_title", "views", "response_bytes"])
    # replace the data and the schema together, in one commit, so readers of the new version see both at once
    write_deltalake(path, renamed, mode="overwrite", schema_mode="overwrite")
    print(f"data files after the rewrite: {len(DeltaTable(path).file_uris())}")
    after = DeltaTable(path).version()
    print(f"version after the rewrite: {after}")
    print(f"columns after the rewrite: {', '.join(read_delta(path).column_names)}")
    print(f"rows after the rewrite: {read_delta(path).num_rows}")
