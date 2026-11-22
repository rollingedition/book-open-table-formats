"""Compact six hourly appends into a Delta Lake table, and count the files each version lists and the Parquet files on disk after it.
The prose rests on the printed lines for the last append version and the compaction version."""

from lib.figures import ACCENT, INK, MUTED, new_figure, save
from lib.lakehouse import delta_file_count, new_warehouse, read_cut_hour

from deltalake import DeltaTable, write_deltalake


def main():
    with new_warehouse() as wh:
        root = wh.root / "delta"
        path = str(root / "pageviews_hourly")
        on_disk = []  # the Parquet files under the table's folder after each commit, indexed by version; no version lists them all
        for hour in range(6):
            write_deltalake(path, read_cut_hour(hour), mode="append")  # one append is one commit, so one version
            on_disk.append(len(list(root.rglob("*.parquet"))))

        metrics = DeltaTable(path).optimize.compact()  # a commit: it writes one file and lists it in place of the six
        compacted = DeltaTable(path).version()
        assert compacted == len(on_disk), "compaction should be the next commit after the last append"
        on_disk.append(len(list(root.rglob("*.parquet"))))  # the replaced files are still on disk: no vacuum has run
        assert metrics["numFilesRemoved"] == 6, "the six appended files should be the ones compaction replaces"

        last = compacted - 1
        # Each version's own log lists the files that version uses, so the count is read from that version.
        listed = [delta_file_count(DeltaTable(path, version=v)) for v in range(compacted + 1)]
        assert all(listed[v] == on_disk[v] == v + 1 for v in range(last + 1)), "each append adds one file, listed and on disk"
        print(f"last append version: {last}")
        print(f"files in the last append version: {listed[last]}")
        print(f"parquet files on disk after the last append version: {on_disk[last]}")
        print(f"compaction version: {compacted}")
        print(f"files in the compaction version: {listed[compacted]}")
        print(f"parquet files on disk after the compaction version: {on_disk[compacted]}")

        fig, ax = new_figure(height=2.8)
        versions = list(range(compacted + 1))
        ax.plot(versions, listed, color=ACCENT, linewidth=2.2, marker="o", markersize=4, zorder=3, label="files in the version")
        ax.plot(versions, on_disk, color=INK, linestyle="--", marker="s", markersize=3.5, zorder=2, label="Parquet files on disk")
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
