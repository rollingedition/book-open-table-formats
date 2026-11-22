"""Show that Delta Lake's log retention removes old versions from the log and deletes no data file, not even the old
hour-02 file that a delete removed. The point rests on the loadable-version lines and the two data-file counts."""

from lib.lakehouse import new_warehouse, read_cut_hour, hour_corrections

from pathlib import Path

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

RETENTION = "interval 0 seconds"  # teaching value only: every entry before the checkpoint is past it; production keeps 30 days
VERSIONS = range(6)  # versions 0 to 2 are the appends, 3 the delete, 4 the corrected append, 5 sets the retention


def loadable_versions(path):
    """Return the versions of the table that still load from the log."""
    found = []
    for version in VERSIONS:
        try:
            DeltaTable(path, version=version)
        except TableNotFoundError:  # the log no longer holds this version
            continue
        found.append(version)
    return found


with new_warehouse() as wh:
    path = str(wh.root / "delta" / "pageviews_hourly")
    for hour in (0, 1, 2):
        write_deltalake(path, read_cut_hour(hour), mode="append")
    DeltaTable(path).delete("hour_utc = '2024-06-03T02:00:00'")  # hour 02, matched by its exact timestamp
    write_deltalake(path, hour_corrections(read_cut_hour(2)), mode="append")
    DeltaTable(path).alter.set_table_properties({"delta.logRetentionDuration": RETENTION})

    print("versions loadable before cleanup:", len(loadable_versions(path)))
    files_before = len(list(Path(path).glob("*.parquet")))
    # cleanup_metadata deletes only log entries older than a checkpoint, so write one at the current version first
    DeltaTable(path).create_checkpoint()
    DeltaTable(path).cleanup_metadata()

    after = loadable_versions(path)
    print("versions loadable after cleanup:", len(after))
    print("current version after cleanup:", DeltaTable(path).version())
    assert after == [5]  # only the current version is still in the log
    files_after = len(list(Path(path).glob("*.parquet")))
    print("data files on disk before cleanup:", files_before)
    print("data files on disk after cleanup:", files_after)
    assert files_after == files_before  # expiry removed versions and no file, the old hour-02 file included
