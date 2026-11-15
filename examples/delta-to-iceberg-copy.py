"""Copy a Delta Lake table into Iceberg through Arrow, then compare the copy with its source.

The point rests on the delta versions and iceberg snapshots lines, which show several commits becoming one,
and on the two column listings, which show the names carried across."""

from deltalake import DeltaTable

from lib.lakehouse import (create_iceberg_table, new_warehouse, print_schema, read_delta, safe_error, scan_arrow,
                           write_delta_hours)


def main():
    try:
        with new_warehouse() as wh:
            path = write_delta_hours(wh.root, range(3))
            source = read_delta(path)
            # Delta numbers its versions from 0, so the count is the last version plus one
            print(f"delta versions: {DeltaTable(path).version() + 1}")
            print(f"delta rows: {source.num_rows}")
            print("delta columns (name, type, nullability):")
            print_schema(source.schema)

            table = create_iceberg_table(wh)
            table.append(source)
            print(f"iceberg snapshots: {len(table.metadata.snapshots)}")
            print(f"iceberg rows: {scan_arrow(table).num_rows}")
            print("iceberg columns (name, type, requirement):")
            # sorted by name, so the two listings line up row for row
            for field in sorted(table.schema().fields, key=lambda field: field.name):
                # Iceberg calls a nullable field optional, so the listing says optional where Delta says nullable
                requirement = "required" if field.required else "optional"
                print(f"{field.name}\t{field.field_type}\t{requirement}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
