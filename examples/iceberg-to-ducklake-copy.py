"""Copy an Iceberg table into DuckLake through Arrow, partitioned from the start, then check its layout and history."""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import HourTransform

from lib.lakehouse import (copy_to_ducklake, create_iceberg_table, ducklake_files_by_partition, ducklake_partition_transforms,
                           new_warehouse, read_cut_hour, safe_error, scan_arrow, TABLE)


def main():
    try:
        with new_warehouse() as wh:
            spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=HourTransform(), name="ts_hour"))
            table = create_iceberg_table(wh, partition_spec=spec)
            for hour in range(3):
                table.append(read_cut_hour(hour))
            arrow = scan_arrow(table)
            print(f"source rows: {arrow.num_rows}")
            print(f"source snapshots: {len(table.metadata.snapshots)}")
            for field in table.spec().fields:
                print(f"source partition field: {field.name}")

            target = copy_to_ducklake(wh, arrow, partition_by="hour(hour_utc)")
            print(f"copy rows: {wh.con.execute(f'SELECT count(*) FROM {target}').fetchall()[0][0]}")
            print(f"copy partition transforms: {ducklake_partition_transforms(wh.con, table=TABLE, current=True)}")
            files = ducklake_files_by_partition(wh.con, "lake", TABLE)
            print(f"copy partition values: {[value for value, _, _ in files]}")
            print(f"copy files per partition: {[count for _, count, _ in files]}")
            print(f"copy rows per partition: {[int(rows) for _, _, rows in files]}")
            changes = wh.con.execute("SELECT changes FROM ducklake_snapshots('lake') ORDER BY snapshot_id").fetchall()
            for number, (change,) in enumerate(changes, start=1):
                print(f"copy snapshot {number}: {', '.join(sorted(change))}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
