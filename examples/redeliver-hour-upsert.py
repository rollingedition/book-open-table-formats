"""Upsert the corrected hour 02 into the Iceberg table on its key, then upsert it again."""

import pyarrow as pa

from lib.lakehouse import KEY, hour_corrections, load_three_formats, new_warehouse, read_cut_hour, safe_error, snapshot_labels


def main():
    with new_warehouse() as wh:
        iceberg, _, _ = load_three_formats(wh, range(6))
        before = {snapshot.snapshot_id for snapshot in iceberg.snapshots()}
        corrected = hour_corrections(read_cut_hour(2))
        # A late row: a page the first delivery of hour 02 did not have.
        late = corrected.slice(0, 1).set_column(
            corrected.schema.get_field_index("page_title"),
            "page_title",
            pa.array(["Late_arrival_example"], type=pa.string()),
        )
        batch = pa.concat_tables([corrected, late])

        result = iceberg.upsert(batch, join_cols=list(KEY))
        print("rows updated:", result.rows_updated)
        print("rows inserted:", result.rows_inserted)

        iceberg.refresh()
        labels = snapshot_labels([snapshot.snapshot_id for snapshot in iceberg.snapshots()])
        added = [snapshot for snapshot in iceberg.snapshots() if snapshot.snapshot_id not in before]
        for snapshot in added:
            label = labels[snapshot.snapshot_id]
            print(label, snapshot.summary.operation.value)
            print(f"{label} records added:", snapshot.summary.get("added-records") or 0)
            print(f"{label} records deleted:", snapshot.summary.get("deleted-records") or 0)
        print("snapshots added:", len(added))

        again = iceberg.upsert(batch, join_cols=list(KEY))
        print("rows updated on second upsert:", again.rows_updated)
        print("rows inserted on second upsert:", again.rows_inserted)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # the redacted error is what a reader sees
        print(safe_error(exc))
        raise SystemExit(1)
