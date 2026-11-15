"""Renames a Delta Lake column by overwriting the table with renamed data and schema, then reads the version before the rewrite back.
Compare the two columns lines: the version before the rewrite, and the version the rewrite makes.
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
