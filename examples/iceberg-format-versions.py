"""Create a format version 2 Iceberg table, delete rows from it, then ask pyiceberg for format version 3."""

from pyiceberg.expressions import LessThan

from lib.lakehouse import create_iceberg_table, new_warehouse, read_cut_hour, scan_arrow

LOW_VIEWS = 180


def main():
    with new_warehouse() as wh:
        table = create_iceberg_table(wh, properties={"format-version": "2"})
        table.append(read_cut_hour(0))
        print("format version:", table.metadata.format_version)

        before = scan_arrow(table).num_rows
        table.delete(LessThan("views", LOW_VIEWS))
        print("rows deleted:", before - scan_arrow(table).num_rows)
        contents = table.inspect.files().column("content").to_pylist()
        print("delete files after the delete:", sum(1 for content in contents if content != 0))
        for number, snapshot in enumerate(table.metadata.snapshots, start=1):
            print(f"snapshot {number} operation:", snapshot.summary.operation.value)

        try:
            create_iceberg_table(wh, identifier="wiki.format_three", properties={"format-version": "3"})
        except NotImplementedError as exc:
            print("format version 3 create:", type(exc).__name__)
        else:
            print("format version 3 create: created")


if __name__ == "__main__":
    main()
