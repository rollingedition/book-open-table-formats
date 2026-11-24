"""Commit three hours to an Iceberg table and a Delta Lake table, then list each commit with its operation and rows added.
The prose's point that all three Iceberg commits are appends rests on the operation lines for the snapshots."""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import ICEBERG_ID, PAGEVIEWS_SCHEMA, new_warehouse, read_cut_hour, snapshot_labels


def main():
    with new_warehouse() as wh:
        table = wh.catalog.create_table(ICEBERG_ID, schema=PAGEVIEWS_SCHEMA)
        delta_path = str(wh.root / "delta" / "pageviews_hourly")
        for hour in range(3):                       # hours 00, 01 and 02: one commit to each table per hour
            batch = read_cut_hour(hour)
            table.append(batch)
            write_deltalake(delta_path, batch, mode="append")

        # sequence_number orders the snapshots, and is not a count of all commits; sorting by it puts the labels in commit order.
        snapshots = sorted(table.snapshots(), key=lambda snapshot: snapshot.sequence_number)
        labels = snapshot_labels([snapshot.snapshot_id for snapshot in snapshots])
        for snapshot in snapshots:
            label = labels[snapshot.snapshot_id]
            operation = snapshot.summary.operation.value
            print(f"{label} operation: {operation}")
            # added-records counts only this commit's rows, not the table's running total.
            print(f"{label} rows added: {snapshot.summary['added-records']}")

        # Sorted by version, so the lines follow Delta's own numbering whatever order history returns.
        history = sorted(DeltaTable(delta_path).history(), key=lambda entry: entry["version"])
        for entry in history:
            rows = entry["operationMetrics"]["num_added_rows"]      # Delta's count of the rows this WRITE added
            print(f"version {entry['version']} operation: {entry['operation']}")
            print(f"version {entry['version']} rows added: {rows}")


if __name__ == "__main__":
    main()
