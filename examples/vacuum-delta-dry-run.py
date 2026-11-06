"""Vacuum a Delta table: dry run first, then the teaching-only zero-hour run, then a read of a removed version."""

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

from lib.lakehouse import new_warehouse, read_cut_hour, read_delta, hour_corrections

with new_warehouse() as wh:
    path = str(wh.root / "delta" / "pageviews_hourly")
    for hour in (0, 1, 2):
        write_deltalake(path, read_cut_hour(hour), mode="append")
    dt = DeltaTable(path)
    dt.delete("hour_utc = '2024-06-03T02:00:00'")
    write_deltalake(path, hour_corrections(read_cut_hour(2)), mode="append")

    dt = DeltaTable(path)
    print("current version:", dt.version())
    print("default retention, dry run lists:", len(dt.vacuum(dry_run=True)))
    try:
        dt.vacuum(retention_hours=0, dry_run=True)
    except DeltaError as exc:
        print("zero retention without the flag:", type(exc).__name__)
    # Teaching run only: a zero-hour retention removes files at once. Production keeps the 168-hour default.
    print("zero retention, dry run lists:", len(dt.vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False)))
    print("zero retention, removed:", len(dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)))
    print("rows in version 0:", read_delta(path, version=0).num_rows)
    try:
        read_delta(path, version=2)
    except pa.ArrowInvalid as exc:
        print("read version 2:", type(exc).__name__)
