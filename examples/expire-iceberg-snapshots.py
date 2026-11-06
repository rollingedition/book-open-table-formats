"""Expire three Iceberg snapshots by ID, then count the data files they kept alive."""

from lib.lakehouse import (
    ICEBERG_ID,
    create_iceberg_table,
    hour_corrections,
    hour_filter,
    new_warehouse,
    orphan_data_files,
    read_cut_hour,
    scan_arrow,
    snapshot_labels,
)


def referenced_files(table):
    """Distinct data files that some retained snapshot lists. Count them; do not print them."""
    return len(set(table.inspect.all_files()["file_path"].to_pylist()))


with new_warehouse() as wh:
    table = create_iceberg_table(wh)
    for hour in (0, 1, 2):
        table.append(read_cut_hour(hour))
    table.overwrite(hour_corrections(read_cut_hour(2)), overwrite_filter=hour_filter(2))

    ids = [snapshot.snapshot_id for snapshot in table.metadata.snapshots]
    labels = snapshot_labels(ids)
    print("snapshots before expiry:", len(ids))
    print("data files referenced before expiry:", referenced_files(table))
    print("data files in the current snapshot:", len(table.inspect.files()))

    expired = ids[:3]  # the three hourly appends, by commit order
    table.maintenance.expire_snapshots().by_ids(expired).commit()
    table = wh.catalog.load_table(ICEBERG_ID)

    print("remaining snapshots:", ", ".join(labels[s.snapshot_id] for s in table.metadata.snapshots))
    print("data files referenced after expiry:", referenced_files(table))
    print("unreferenced data files left on disk:", len(orphan_data_files(table)))
    print("rows in the current snapshot:", scan_arrow(table).num_rows)
    try:
        scan_arrow(table, snapshot_id=expired[-1])
    except ValueError as exc:
        print(f"read {labels[expired[-1]]}: {type(exc).__name__}")
