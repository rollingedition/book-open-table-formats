"""Register an Iceberg table in a SQL catalog, then reach it from a second catalog object."""

from lib.lakehouse import (ICEBERG_ID, NAMESPACE, create_iceberg_table, new_warehouse,
                           open_catalog, read_cut_hour, safe_error, scan_arrow)

with new_warehouse() as wh:
    table = create_iceberg_table(wh)
    for hour in range(3):
        table.append(read_cut_hour(hour))

    # A second object with the same catalog name sees the same tables.
    same = open_catalog(wh)
    loaded = same.load_table(ICEBERG_ID)
    print("rows read through the second object:", scan_arrow(loaded).num_rows)

    # A loaded table is a snapshot of the catalog as it was when loaded.
    stale = same.load_table(ICEBERG_ID)
    table.append(read_cut_hour(3))
    print("rows in the loaded object after another append:", scan_arrow(stale).num_rows)
    print("rows after a reload:", scan_arrow(same.load_table(ICEBERG_ID)).num_rows)

    # Register the same metadata file under a new name: a pointer, not a copy.
    same.create_namespace("archive")
    same.register_table("archive.pageviews_copy", table.metadata_location)
    print("rows in archive copy:", scan_arrow(same.load_table("archive.pageviews_copy")).num_rows)

    # An object with another name does not see the namespace.
    try:
        open_catalog(wh, name="other").list_tables(NAMESPACE)
    except Exception as exc:
        print("other name:", safe_error(exc, wh))
