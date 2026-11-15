"""Partition an Iceberg table by day, change its spec to hour, and load more hours under the new spec.

The point rests on the data file counts under each spec, which show which loads wrote under which spec.
"""

from collections import Counter

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform, HourTransform

from lib.lakehouse import create_iceberg_table, new_warehouse, read_cut_hour, safe_error, scan_arrow


def main():
    try:
        with new_warehouse() as wh:
            # source_id 1 is hour_utc, the table's first column: the day transform reads that column
            day_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day"))
            table = create_iceberg_table(wh, partition_spec=day_spec)
            for hour in range(3):  # hours 00 to 02, written under the day spec
                table.append(read_cut_hour(hour))

            # the hour field reads the same source column, so a filter on hour_utc works under both specs
            with table.update_spec() as update:
                update.remove_field("ts_day")
                update.add_field("hour_utc", HourTransform(), "ts_hour")
            for hour in range(3, 6):  # hours 03 to 05, written under the hour spec
                table.append(read_cut_hour(hour))

            # spec IDs differ on every run, so the specs are labelled by the order they were made
            specs = sorted(table.specs())
            labels = {spec_id: f"spec {number}" for number, spec_id in enumerate(specs, start=1)}
            for spec_id in specs:
                for field in table.specs()[spec_id].fields:
                    print(f"{labels[spec_id]} field {field.name}: transform {field.transform}")

            # each data file records the ID of the spec it was written under
            files = Counter(table.inspect.data_files().column("spec_id").to_pylist())
            for spec_id in specs:
                print(f"{labels[spec_id]} data files: {files[spec_id]}")
            print(f"rows in table: {scan_arrow(table).num_rows}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
