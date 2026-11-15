"""Upserts the corrected hour 02, with its late row, into the Iceberg table, then upserts the same batch again. The snapshot lines carry the point, in the order the commits were made."""

import pyarrow as pa

from lib.lakehouse import KEY, hour_corrections, load_three_formats, new_warehouse, read_cut_hour, safe_error, snapshot_labels


def main():
    with new_warehouse() as wh:
        iceberg, _, _ = load_three_formats(wh, range(6))
        before = {snapshot.snapshot_id for snapshot in iceberg.snapshots()}  # so the loop below lists only the snapshots this upsert adds
        # hour_corrections halves the views of the first 20 page titles in byte order, so the same titles change on every run
        corrected = hour_corrections(read_cut_hour(2))
        # A late row: a page the first delivery of hour 02 did not have.
        late = corrected.slice(0, 1).set_column(
            corrected.schema.get_field_index("page_title"),
            "page_title",
            pa.array(["Late_arrival_example"], type=pa.string()),
        )
        batch = pa.concat_tables([corrected, late])

        result = iceberg.upsert(batch, join_cols=list(KEY))  # KEY is the three key columns; a row matches when all three are equal
        # rows_updated counts only rows whose non-key values changed, so identical rows are not counted
        print("rows updated:", result.rows_updated)
        print("rows inserted:", result.rows_inserted)

        iceberg.refresh()
        labels = snapshot_labels([snapshot.snapshot_id for snapshot in iceberg.snapshots()])  # snapshot IDs change on every run, so the labels number commits
        added = [snapshot for snapshot in iceberg.snapshots() if snapshot.snapshot_id not in before]
        for snapshot in added:
            label = labels[snapshot.snapshot_id]
            print(label, snapshot.summary.operation.value)
            # a snapshot that adds or deletes no records has no such key, so the count is 0
            print(f"{label} records added:", snapshot.summary.get("added-records") or 0)
            print(f"{label} records deleted:", snapshot.summary.get("deleted-records") or 0)
        print("snapshots added:", len(added))

        again = iceberg.upsert(batch, join_cols=list(KEY))  # the same batch again, as a retry would send it
        print("rows updated on second upsert:", again.rows_updated)
        print("rows inserted on second upsert:", again.rows_inserted)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # the redacted error is what a reader sees
        print(safe_error(exc))
        raise SystemExit(1)
