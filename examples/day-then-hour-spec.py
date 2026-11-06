"""Partition an Iceberg table by day, then change the spec to hour and load three more hours.

Each partition spec is labelled in the order it was made. Every data file keeps the spec it was written under.
"""

from collections import Counter

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform, HourTransform

from lib.lakehouse import create_iceberg_table, new_warehouse, read_cut_hour, safe_error, scan_arrow


def main():
    try:
        with new_warehouse() as wh:
            day_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day"))
            table = create_iceberg_table(wh, partition_spec=day_spec)
            for hour in range(3):
                table.append(read_cut_hour(hour))

            with table.update_spec() as update:
                update.remove_field("ts_day")
                update.add_field("hour_utc", HourTransform(), "ts_hour")
            for hour in range(3, 6):
                table.append(read_cut_hour(hour))

            specs = sorted(table.specs())
            labels = {spec_id: f"spec {number}" for number, spec_id in enumerate(specs, start=1)}
            for spec_id in specs:
                for field in table.specs()[spec_id].fields:
                    print(f"{labels[spec_id]} field {field.name}: transform {field.transform}")

            files = Counter(table.inspect.data_files().column("spec_id").to_pylist())
            for spec_id in specs:
                print(f"{labels[spec_id]} data files: {files[spec_id]}")
            print(f"rows in table: {scan_arrow(table).num_rows}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
