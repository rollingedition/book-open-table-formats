"""Count the files an Iceberg scan plans for two hours and for the whole table, before and after a spec change.

The filters are hour 01 of the case's day, loaded before the change, and hour 04, loaded after it.
The spec changes from day to hour between the two counts.
"""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform, HourTransform

from lib.lakehouse import create_iceberg_table, hour_filter, new_warehouse, read_cut_hour, safe_error


def plan_counts(table):
    """Return the files planned for hour 01, for hour 04 and for no filter, in that order."""
    one_hour = len(table.scan(row_filter=hour_filter(1)).plan_files())
    hour_four = len(table.scan(row_filter=hour_filter(4)).plan_files())
    everything = len(table.scan().plan_files())
    return one_hour, hour_four, everything


def main():
    try:
        with new_warehouse() as wh:
            day_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day"))
            table = create_iceberg_table(wh, partition_spec=day_spec)
            for hour in range(3):
                table.append(read_cut_hour(hour))
            one_hour, hour_four, everything = plan_counts(table)
            print(f"before the change, one-hour filter plans: {one_hour}")
            print(f"before the change, hour 04 filter plans: {hour_four}")
            print(f"before the change, no filter plans: {everything}")

            with table.update_spec() as update:
                update.remove_field("ts_day")
                update.add_field("hour_utc", HourTransform(), "ts_hour")
            for hour in range(3, 6):
                table.append(read_cut_hour(hour))
            one_hour, hour_four, everything = plan_counts(table)
            print(f"after the change, one-hour filter plans: {one_hour}")
            print(f"after the change, hour 04 filter plans: {hour_four}")
            print(f"after the change, no filter plans: {everything}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
