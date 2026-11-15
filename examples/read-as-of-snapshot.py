"""Read an Iceberg table as of snapshot 2 and snapshot 3, by ID, then compare the current table with snapshot 3.
The prose's answer to the owner's first question rests on the snapshot 2 views line, and the failed read on the last line."""

from lib.lakehouse import (
    ICEBERG_ID, PAGEVIEWS_SCHEMA, new_warehouse, read_cut_hour, safe_error, scan_arrow, snapshot_labels, sql_rows,
)


def main():
    with new_warehouse() as wh:
        table = wh.catalog.create_table(ICEBERG_ID, schema=PAGEVIEWS_SCHEMA)
        for hour in range(3):
            table.append(read_cut_hour(hour))

        snapshots = sorted(table.snapshots(), key=lambda snapshot: snapshot.sequence_number)
        ids = [snapshot.snapshot_id for snapshot in snapshots]
        labels = snapshot_labels(ids)
        # ids[1] is snapshot 2 and ids[2] is snapshot 3: the list is in commit order, counted from 0.
        for snapshot_id in (ids[1], ids[2]):
            arrow = scan_arrow(table, snapshot_id=snapshot_id)
            # The scan is registered in DuckDB as t, so the SQL can count and sum its rows.
            rows, views = sql_rows(wh.con, "SELECT count(*), sum(views) FROM t", t=arrow)[0]
            print(f"{labels[snapshot_id]} rows: {rows}")
            print(f"{labels[snapshot_id]} views: {views}")

        current = scan_arrow(table)                  # no snapshot_id, so this reads the current snapshot
        last = scan_arrow(table, snapshot_id=ids[2])
        print(f"current rows: {current.num_rows}")
        # equals compares every row and column, in order; scan_arrow sorts both tables by every column, so row order cannot differ.
        assert current.equals(last)

        try:
            scan_arrow(table, snapshot_id=2)         # 2 is the label's number, not an ID, so this fails on purpose
        except Exception as exc:
            print(f"read by label: {safe_error(exc, wh)}")


if __name__ == "__main__":
    main()
