"""List each commit to one table: Iceberg snapshots and Delta Lake versions."""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import ICEBERG_ID, PAGEVIEWS_SCHEMA, new_warehouse, read_cut_hour, snapshot_labels


def main():
    with new_warehouse() as wh:
        table = wh.catalog.create_table(ICEBERG_ID, schema=PAGEVIEWS_SCHEMA)
        delta_path = str(wh.root / "delta" / "pageviews_hourly")
        for hour in range(3):
            batch = read_cut_hour(hour)
            table.append(batch)
            write_deltalake(delta_path, batch, mode="append")

        snapshots = sorted(table.snapshots(), key=lambda snapshot: snapshot.sequence_number)
        labels = snapshot_labels([snapshot.snapshot_id for snapshot in snapshots])
        for snapshot in snapshots:
            label = labels[snapshot.snapshot_id]
            operation = snapshot.summary.operation.value
            print(f"{label} operation: {operation}")
            print(f"{label} rows added: {snapshot.summary['added-records']}")

        history = sorted(DeltaTable(delta_path).history(), key=lambda entry: entry["version"])
        for entry in history:
            rows = entry["operationMetrics"]["num_added_rows"]
            print(f"version {entry['version']} operation: {entry['operation']}")
            print(f"version {entry['version']} rows added: {rows}")


if __name__ == "__main__":
    main()
