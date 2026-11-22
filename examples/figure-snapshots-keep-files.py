"""Expire the three append snapshots, then draw which data files each snapshot lists. The figure rests on the printed
lines: file 3 is listed by snapshot 3 before the expiry and by no kept snapshot after it, and it stays on disk."""

from lib.figures import ACCENT, MUTED, new_figure, save
from lib.lakehouse import (ICEBERG_ID, create_iceberg_table, hour_corrections, hour_filter, new_warehouse,
                           orphan_data_files, read_cut_hour, snapshot_labels)


def listed(table, sid):
    """Data-file paths one snapshot lists. The output names them by write order instead."""
    return set(table.inspect.files(snapshot_id=sid)["file_path"].to_pylist())


with new_warehouse() as wh:
    table = create_iceberg_table(wh)
    for hour in (0, 1, 2):
        table.append(read_cut_hour(hour))
    # hour_filter(2) matches only hour 02, so the overwrite leaves hours 00 and 01 alone
    table.overwrite(hour_corrections(read_cut_hour(2)), overwrite_filter=hour_filter(2))

    ids = [s.snapshot_id for s in table.metadata.snapshots]  # commit order
    labels, names = snapshot_labels(ids), {}
    for sid in ids:  # name each file by the first snapshot that lists it, so file 1 is the first one written
        new = listed(table, sid) - set(names)
        names.update({path: f"file {len(names) + 1}" for path in new})
    before = {sid: {names[path] for path in listed(table, sid)} for sid in ids}
    print("snapshots before expiry:", len(ids))
    for sid in ids:
        print(f"{labels[sid]} lists:", ", ".join(sorted(before[sid])))
    print("data files referenced before expiry:", len(names))

    expired = ids[:3]  # the three hourly appends, by commit order
    table.maintenance.expire_snapshots().by_ids(expired).commit()
    table = wh.catalog.load_table(ICEBERG_ID)
    print("remaining snapshots:", ", ".join(labels[s.snapshot_id] for s in table.metadata.snapshots))
    referenced = set().union(*(listed(table, s.snapshot_id) for s in table.metadata.snapshots))  # what kept snapshots list
    unreferenced = sorted(set(names.values()) - {names[path] for path in referenced})
    print("data files referenced after expiry:", len(referenced))
    print("unreferenced after expiry:", ", ".join(unreferenced))
    print("unreferenced data files left on disk:", len(orphan_data_files(table)))  # counted from the folder

    # One row per snapshot, one column per file; a square marks a listed file, and the expired rows are muted.
    fig, ax = new_figure(height=3.2, sharex=False)
    for row, sid in enumerate(ids, start=1):
        xs = [col for col, name in enumerate(names.values(), start=1) if name in before[sid]]
        ax.scatter(xs, [row] * len(xs), s=90, marker="s", color=MUTED if sid in expired else ACCENT, zorder=3)
    ax.set_yticks(range(1, len(ids) + 1), [f"{labels[s]} ({'expired' if s in expired else 'kept'})" for s in ids])
    ax.set_xticks(range(1, len(names) + 1), list(names.values()))
    ax.set_ylim(len(ids) + 0.5, 0.5)  # snapshot 1 at the top, the current snapshot at the bottom
    col = list(names.values()).index(unreferenced[0]) + 1  # the one column no kept row lists
    ax.text(col, len(expired) + 1.5, "listed by no\nkept snapshot", ha="center", va="center", color=MUTED)
    save(fig, "snapshots-keep-files")
