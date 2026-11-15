"""Show that vacuum's default retention keeps a file that a delete has just removed, and that a read of an older version
fails once a teaching-only run removes that file. The point rests on the default-retention line and the read of version 2."""

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

from lib.lakehouse import new_warehouse, read_cut_hour, read_delta, hour_corrections

with new_warehouse() as wh:
    path = str(wh.root / "delta" / "pageviews_hourly")
    for hour in (0, 1, 2):
        write_deltalake(path, read_cut_hour(hour), mode="append")
    dt = DeltaTable(path)
    dt.delete("hour_utc = '2024-06-03T02:00:00'")  # hour 02, matched by its exact timestamp
    # hour_corrections halves the views of some page titles in hour 02, so this append holds the corrected rows
    write_deltalake(path, hour_corrections(read_cut_hour(2)), mode="append")

    dt = DeltaTable(path)
    print("current version:", dt.version())
    # the delete was seconds ago, so the default retention still keeps its file
    print("default retention, dry run lists:", len(dt.vacuum(dry_run=True)))
    try:
        dt.vacuum(retention_hours=0, dry_run=True)  # a retention below the table's minimum: refused without the flag
    except DeltaError as exc:
        print("zero retention without the flag:", type(exc).__name__)
    # Teaching run only: a zero-hour retention removes files at once. Production keeps the 168-hour default.
    print("zero retention, dry run lists:", len(dt.vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False)))
    print("zero retention, removed:", len(dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)))
    print("rows in version 0:", read_delta(path, version=0).num_rows)  # version 0 reads files the vacuum left alone
    try:
        read_delta(path, version=2)  # version 2 still lists the hour-02 file that the vacuum removed
    except pa.ArrowInvalid as exc:
        print("read version 2:", type(exc).__name__)
