"""Show that Delta Lake's log retention removes old versions from the log and deletes no data file: after cleanup_metadata,
only the current version still loads, and the same data files are on disk. The point rests on the loadable-version lines
after the cleanup and on the two data-file counts, which match."""

from lib.lakehouse import new_warehouse, read_cut_hour

import time
from pathlib import Path

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

RETENTION = "interval 1 second"  # teaching value only: production keeps the 30-day default
VERSIONS = range(4)  # versions 0 to 2 are the three appends, and version 3 sets the retention


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
    # a commit that sets the retention, so the log holds version 3 as well
    DeltaTable(path).alter.set_table_properties({"delta.logRetentionDuration": RETENTION})
    # the retention is measured back from now, so wait until the earlier commits are older than it
    time.sleep(2)

    print("versions loadable before cleanup:", len(loadable_versions(path)))
    files_before = len(list(Path(path).glob("*.parquet")))
    # cleanup_metadata removes nothing before the last checkpoint, so write one first
    DeltaTable(path).create_checkpoint()
    DeltaTable(path).cleanup_metadata()

    after = loadable_versions(path)
    print("versions loadable after cleanup:", len(after))
    print("current version after cleanup:", DeltaTable(path).version())
    assert after == [3]  # only the current version is still in the log
    files_after = len(list(Path(path).glob("*.parquet")))
    print("data files on disk before cleanup:", files_before)
    print("data files on disk after cleanup:", files_after)
    assert files_after == files_before  # expiry removed a version and no file
