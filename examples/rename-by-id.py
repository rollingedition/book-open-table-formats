"""Rename page_title to article_title in Iceberg, then read old files under the new name.

Iceberg keeps each column's field ID across a rename, so the three files
written under page_title are read under article_title without a rewrite.
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

    before = table.schema().find_field("page_title").field_id
    print(f"page_title field ID before the rename: {before}")
    print(f"data files before the rename: {iceberg_file_count(table)}")
    with table.update_schema() as update:
        update.rename_column("page_title", "article_title")
    print(f"article_title field ID after the rename: {table.schema().find_field('article_title').field_id}")
    print(f"data files after the rename: {iceberg_file_count(table)}")

    rows = scan_arrow(table)
    print(f"rows: {rows.num_rows}")
    count = sql_rows(wh.con, "SELECT count(article_title) FROM t", t=rows)[0][0]
    print(f"article_title not null: {count}")

    labels = snapshot_labels([snapshot.snapshot_id for snapshot in table.snapshots()])
    first = table.snapshots()[0].snapshot_id
    print(f"{labels[first]} columns: {', '.join(scan_arrow(table, snapshot_id=first).column_names)}")
