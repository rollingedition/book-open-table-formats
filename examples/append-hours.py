"""Create the Iceberg table, append hours 00 to 02 as three commits, then repeat hour 00.

The point about repeated loads rests on the last two lines: the repeat adds rows, and no error is raised.
"""

from lib.lakehouse import (
    create_iceberg_table,
    iceberg_file_count,
    new_warehouse,
    read_cut_hour,
    safe_error,
    scan_arrow,
    snapshot_labels,
)


def main():
    try:
        with new_warehouse() as wh:
            table = create_iceberg_table(wh)
            # Hours 00, 01 and 02, one commit each, in hour order.
            for hour in range(3):
                table.append(read_cut_hour(hour))

            # pyiceberg returns the snapshots in commit order, which the labels below depend on.
            snapshots = table.snapshots()
            # Snapshot IDs change from run to run, so each one prints as its position in commit order.
            labels = snapshot_labels([snap.snapshot_id for snap in snapshots])
            for snap in snapshots:
                label = labels[snap.snapshot_id]
                print(f"{label} operation: {snap.summary.operation.value}")
                # The commit's own summary holds the rows it added, not a count of the whole table.
                print(f"{label} rows added: {snap.summary['added-records']}")

            print(f"rows in table: {scan_arrow(table).num_rows}")
            # Counted from the current snapshot, not from the folder.
            print(f"data files: {iceberg_file_count(table)}")

            # Hour 00 again. An append does not check whether its rows are already in the table.
            table.append(read_cut_hour(0))
            repeat = table.current_snapshot()
            print(f"repeat of hour 00 operation: {repeat.summary.operation.value}")
            print(f"repeat of hour 00 rows added: {repeat.summary['added-records']}")
            print(f"rows in table after the repeat: {scan_arrow(table).num_rows}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
