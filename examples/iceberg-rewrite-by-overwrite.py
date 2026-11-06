"""Rewrite an Iceberg table by overwriting it with its own scan, and count data files before and after."""

from lib.lakehouse import (
    create_iceberg_table,
    iceberg_file_count,
    new_warehouse,
    read_cut_hour,
    scan_arrow,
    snapshot_labels,
)


def main():
    with new_warehouse() as wh:
        table = create_iceberg_table(wh)
        for hour in range(6):
            table.append(read_cut_hour(hour))
        last_append = table.current_snapshot().snapshot_id
        print(f"data files before rewrite: {iceberg_file_count(table)}")

        table.overwrite(table.scan().to_arrow_batch_reader())
        print(f"data files after rewrite: {iceberg_file_count(table)}")
        print(f"rows after rewrite: {len(scan_arrow(table))}")

        snapshots = sorted(table.snapshots(), key=lambda snapshot: snapshot.sequence_number)
        labels = snapshot_labels([snapshot.snapshot_id for snapshot in snapshots])
        operations = [snapshot.summary.operation.value for snapshot in snapshots]
        print("snapshot operations in order: " + ", ".join(operations))
        print(f"{labels[last_append]} rows: {len(scan_arrow(table, snapshot_id=last_append))}")
        parquet = list((wh.root / "warehouse").rglob("*.parquet"))
        print(f"parquet files on disk after rewrite: {len(parquet)}")


if __name__ == "__main__":
    main()
