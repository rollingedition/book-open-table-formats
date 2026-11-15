"""Renames a column in an Iceberg table and reads the old files back under the new name.
Read the field ID lines, the data file lines and the non-null count, which show what the rename kept.
"""

import pyarrow as pa

from lib.lakehouse import (
    ICEBERG_ID,
    iceberg_file_count,
    new_warehouse,
    read_cut_hour,
    scan_arrow,
    snapshot_labels,
    sql_rows,
)

with new_warehouse() as wh:
    table = wh.catalog.create_table(ICEBERG_ID, schema=read_cut_hour(0).schema)
    for hour in range(3):
        table.append(read_cut_hour(hour))

    before = table.schema().find_field("page_title").field_id  # the ID is what the reader matches on, not the name
    print(f"page_title field ID before the rename: {before}")
    print(f"data files before the rename: {iceberg_file_count(table)}")
    with table.update_schema() as update:
        update.rename_column("page_title", "article_title")
    print(f"article_title field ID after the rename: {table.schema().find_field('article_title').field_id}")
    print(f"data files after the rename: {iceberg_file_count(table)}")

    rows = scan_arrow(table)
    print(f"rows: {rows.num_rows}")
    # count(column) leaves out nulls, so this counts the rows that have a value under the new name
    count = sql_rows(wh.con, "SELECT count(article_title) FROM t", t=rows)[0][0]
    print(f"article_title not null: {count}")

    labels = snapshot_labels([snapshot.snapshot_id for snapshot in table.snapshots()])
    first = table.snapshots()[0].snapshot_id  # the first snapshot, written before the rename
    print(f"{labels[first]} columns: {', '.join(scan_arrow(table, snapshot_id=first).column_names)}")
