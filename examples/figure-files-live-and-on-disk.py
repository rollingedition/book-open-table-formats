"""Compact six hourly appends into a Delta Lake table, and count the files each version lists and the Parquet files on disk after it.
The figure rests on the printed lines: at version 5 both counts are 6, and at the compaction's version the table lists 1 file while 7 sit on disk."""

from lib.figures import ACCENT, INK, MUTED, new_figure, save
from lib.lakehouse import delta_file_count, new_warehouse, read_cut_hour

from deltalake import DeltaTable, write_deltalake


def parquet_on_disk(root):
    """Count every Parquet file under the table's folder, including the ones no version lists any more."""
    return len(list(root.rglob("*.parquet")))


def main():
    with new_warehouse() as wh:
        root = wh.root / "delta"
        path = str(root / "pageviews_hourly")
        on_disk = []  # the Parquet files on disk after each commit, indexed by version
        for hour in range(6):
            write_deltalake(path, read_cut_hour(hour), mode="append")  # one append is one commit, so one version
            on_disk.append(parquet_on_disk(root))

        last_append = DeltaTable(path).version()
        metrics = DeltaTable(path).optimize.compact()  # a commit: it writes one file and lists it in place of the six
        compacted = DeltaTable(path).version()
        assert compacted == last_append + 1, "compaction should be the next commit after the last append"
        on_disk.append(parquet_on_disk(root))  # the replaced files are still on disk: no vacuum has run
        assert metrics["numFilesRemoved"] == 6, "the six appended files should be the ones compaction replaces"

        versions = list(range(compacted + 1))
        listed = []
        for version in versions:
            # Each version's own log lists the files that version uses, so the count is read from that version.
            listed.append(delta_file_count(DeltaTable(path, version=version)))
            print(f"files in version {version}: {listed[-1]}")
            print(f"parquet files on disk after version {version}: {on_disk[version]}")
        print(f"compaction version: {compacted}")

        fig, ax = new_figure(height=2.8)
        # The two lines lie on top of each other until the compaction: every file on disk is still listed.
        ax.plot(versions, listed, color=ACCENT, marker="o", markersize=4, label="files in the version")
        ax.plot(versions, on_disk, color=INK, linestyle="--", marker="s", markersize=3.5,
                label="parquet files on disk")
        ax.axvline(compacted, color=MUTED, linewidth=0.8, linestyle=":")
        ax.text(compacted - 0.15, 8.1, f"compaction: version {compacted}", ha="right", va="top", color=MUTED)
        ax.set_xlabel("table version")
        ax.set_ylabel("data files")
        ax.set_xticks(versions)
        ax.set_ylim(0, 8.5)
        ax.legend(loc="upper left")
        save(fig, "files-live-and-on-disk")


if __name__ == "__main__":
    main()
