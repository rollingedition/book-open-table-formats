"""Draw each data file of a table that moved from a day spec to an hour spec, at the hour its rows hold, and the files each one-hour filter plans.
The printed file lines, and each filter's count, are the values the figure and its prose rest on."""

import datetime

from lib.figures import ACCENT, INK, MUTED, new_figure, save
from lib.lakehouse import create_iceberg_table, hour_filter, new_warehouse, read_cut_hour, safe_error

from pyiceberg.conversions import from_bytes
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform, HourTransform
from pyiceberg.types import TimestampType

# one marker per filter, in the style the figure uses: a filled circle, a filled diamond, or an open circle
STYLE = {
    "hour 01": dict(marker="o", color=ACCENT, label="planned by the hour 01 filter"),
    "hour 04": dict(marker="D", color=INK, label="planned by the hour 04 filter"),
    "neither": dict(marker="o", facecolors="none", edgecolors=MUTED, label="planned by neither filter"),
}


def planned(table, row_filter=None):  # (hour, spec_id) for each data file a scan plans; no filter plans every file
    scan = table.scan() if row_filter is None else table.scan(row_filter=row_filter)
    epoch = datetime.datetime(1970, 1, 1)  # a timestamp bound is microseconds since the Unix epoch, which is midnight UTC
    found = []
    for task in scan.plan_files():
        low, high = (epoch + datetime.timedelta(microseconds=from_bytes(TimestampType(), bound))
                     for bound in (task.file.lower_bounds[1], task.file.upper_bounds[1]))  # field 1 is hour_utc
        assert low.hour == high.hour, "a data file holds rows from more than one hour"  # so one mark per file is true
        found.append((low.hour, task.file.spec_id))
    return sorted(found)


def hours_text(files):  # two-digit hours, or none for an empty plan
    return ", ".join(f"{hour:02d}" for hour, _ in files) or "none"


def report(table, state):  # print the files and each one-hour filter's plan; return them for the figure
    files = planned(table)
    plans = {hour: planned(table, hour_filter(hour)) for hour in (1, 4)}
    print(f"{state}, data files: {len(files)}")
    print(f"{state}, hours with a data file: {hours_text(files)}")
    for hour, plan in plans.items():
        print(f"{state}, hour {hour:02d} filter plans: {len(plan)}")
        print(f"{state}, hour {hour:02d} filter plans hours: {hours_text(plan)}")
    print(f"{state}, no filter plans: {len(files)}")
    return files, {hour: {h for h, _ in plan} for hour, plan in plans.items()}


def draw(ax, title, files, plans, rows, names):  # one mark per file, at its hour's centre on its spec's row
    for hour, spec_id in files:
        kind = "hour 01" if hour in plans[1] else "hour 04" if hour in plans[4] else "neither"
        ax.scatter(hour + 0.5, rows[spec_id], s=70, zorder=3, **STYLE[kind])
    ax.set_title(title, color=MUTED)
    ax.set_xlim(0, 6)
    ax.set_xticks([hour + 0.5 for hour in range(6)], [f"{hour:02d}" for hour in range(6)])
    ax.set_yticks(list(rows.values()), [names[spec_id] for spec_id in rows])
    ax.set_ylim(min(rows.values()) - 0.5, max(rows.values()) + 0.5)
    ax.grid(False)


def main():
    try:
        with new_warehouse() as wh:
            # source_id 1 is hour_utc, the table's first column: the day transform reads that column
            day_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day"))
            table = create_iceberg_table(wh, partition_spec=day_spec)
            for hour in range(3):  # hours 00 to 02, under the day spec
                table.append(read_cut_hour(hour))
            # every day-spec file has the same partition value, so only the hour_utc bounds can tell their hours apart
            assert len({str(task.file.partition) for task in table.scan().plan_files()}) == 1, "day partition values differ"
            before_files, before_plans = report(table, "before the change")

            with table.update_spec() as update:  # the spec change, as in the day-then-hour example
                update.remove_field("ts_day")
                update.add_field("hour_utc", HourTransform(), "ts_hour")
            for hour in range(3, 6):  # hours 03 to 05, under the hour spec
                table.append(read_cut_hour(hour))
            after_files, after_plans = report(table, "after the change")

            # labels number the specs in the order they were made, so the output names no spec ID
            spec_ids = sorted(table.specs())
            labels = {spec_id: f"spec {number}" for number, spec_id in enumerate(spec_ids, start=1)}
            names = {spec_id: f"{labels[spec_id]}: {table.specs()[spec_id].fields[0].transform}" for spec_id in spec_ids}
            for spec_id in spec_ids:
                print(f"{names[spec_id]}")
            for hour, spec_id in after_files:
                print(f"file for hour {hour:02d}: {labels[spec_id]}")

            rows = {spec_id: len(spec_ids) - number for number, spec_id in enumerate(spec_ids)}  # spec 1 is the top row
            fig, (top, bottom) = new_figure(rows=2, height=4.0)
            draw(top, "before the change", before_files, before_plans, rows, names)
            draw(bottom, "after the change", after_files, after_plans, rows, names)
            bottom.set_xlabel("hour the rows hold, UTC")
            seen = {}  # one legend entry per label, not per mark
            for ax in (top, bottom):
                seen.update(zip(*ax.get_legend_handles_labels()[::-1]))
            top.legend(list(seen.values()), list(seen.keys()), loc="center right", frameon=False)  # room on the right of the top panel
            save(fig, "files-by-spec")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
