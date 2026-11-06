"""Read an Iceberg table as of an earlier snapshot, by snapshot ID."""

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
        for snapshot_id in (ids[1], ids[2]):
            arrow = scan_arrow(table, snapshot_id=snapshot_id)
            rows, views = sql_rows(wh.con, "SELECT count(*), sum(views) FROM t", t=arrow)[0]
            print(f"{labels[snapshot_id]} rows: {rows}")
            print(f"{labels[snapshot_id]} views: {views}")

        current = scan_arrow(table)
        last = scan_arrow(table, snapshot_id=ids[2])
        print(f"current rows: {current.num_rows}")
        print(f"current equals snapshot 3: {current.equals(last)}")

        try:
            scan_arrow(table, snapshot_id=2)
        except Exception as exc:
            print(f"read by label: {safe_error(exc, wh)}")


if __name__ == "__main__":
    main()
