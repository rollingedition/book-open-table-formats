"""Draw each data file of a table that moved from a day spec to an hour spec, at the hour its rows hold, and the files each one-hour filter plans.
The printed file lines, and each filter's count, are the values the figure and its prose rest on."""

import datetime

from lib.figures import ACCENT, INK, MUTED, new_figure, save
from lib.lakehouse import create_iceberg_table, hour_filter, new_warehouse, read_cut_hour, safe_error

from pyiceberg.conversions import from_bytes
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform, HourTransform
from pyiceberg.types import TimestampType


def hour_of(data_file):
    """Return the hour a data file's rows hold, from the bounds of hour_utc (field 1, the table's first column)."""
    # a timestamp bound decodes to microseconds since the Unix epoch, which is midnight UTC
    epoch = datetime.datetime(1970, 1, 1)
    low = (epoch + datetime.timedelta(microseconds=from_bytes(TimestampType(), data_file.lower_bounds[1]))).hour
    high = (epoch + datetime.timedelta(microseconds=from_bytes(TimestampType(), data_file.upper_bounds[1]))).hour
    assert low == high, "a data file holds rows from more than one hour"  # so one mark per file is true
    return low


def planned(table, row_filter=None):
    """Return (hour, spec_id) for each data file a scan plans, in hour order; with no filter, every file."""
    scan = table.scan() if row_filter is None else table.scan(row_filter=row_filter)
    return sorted((hour_of(task.file), task.file.spec_id) for task in scan.plan_files())


def hours_text(files):
    """Return the hours of a plan as two-digit labels, or none when the plan is empty."""
    return ", ".join(f"{hour:02d}" for hour, _ in files) or "none"


def report(table, state):
    """Print the data files and what each one-hour filter plans, labelled with the state of the table; return the sets."""
    files = planned(table)
    print(f"{state}, data files: {len(files)}")
    print(f"{state}, hours with a data file: {hours_text(files)}")
    plans = {}
    for hour in (1, 4):
        plans[hour] = planned(table, hour_filter(hour))
        print(f"{state}, hour {hour:02d} filter plans: {len(plans[hour])}")
        print(f"{state}, hour {hour:02d} filter plans hours: {hours_text(plans[hour])}")
    print(f"{state}, no filter plans: {len(files)}")
    return files, {hour: {h for h, _ in plan} for hour, plan in plans.items()}


def draw(ax, title, files, one_hour, four, rows, names):
    """Mark each file at the centre of its hour, on its spec's row: accent for the hour 01 filter, ink diamonds for the hour 04 filter, hollow for the rest."""
    for hour, spec_id in files:
        x, y = hour + 0.5, rows[spec_id]
        if hour in one_hour:
            ax.scatter(x, y, s=70, color=ACCENT, marker="o", zorder=3, label="planned by the hour 01 filter")
        elif hour in four:
            ax.scatter(x, y, s=70, color=INK, marker="D", zorder=3, label="planned by the hour 04 filter")
        else:
            ax.scatter(x, y, s=70, facecolors="none", edgecolors=MUTED, marker="o", zorder=3, label="not planned by either filter")
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

            # the spec change, as in the day-then-hour example
            with table.update_spec() as update:
                update.remove_field("ts_day")
                update.add_field("hour_utc", HourTransform(), "ts_hour")
            for hour in range(3, 6):  # hours 03 to 05, under the hour spec
                table.append(read_cut_hour(hour))
            after_files, after_plans = report(table, "after the change")

            # labels number the specs in the order they were made, so the output names no spec ID
            spec_ids = sorted(table.specs())
            labels = {spec_id: f"spec {number}" for number, spec_id in enumerate(spec_ids, start=1)}
            transforms = {spec_id: str(table.specs()[spec_id].fields[0].transform) for spec_id in spec_ids}
            for spec_id in spec_ids:
                print(f"{labels[spec_id]} transform: {transforms[spec_id]}")
            for hour, spec_id in after_files:
                print(f"file for hour {hour:02d}: {labels[spec_id]}")

            # the spec 2 row is empty before the change; the spec 1 row is the top row
            rows = {spec_id: len(spec_ids) - number for number, spec_id in enumerate(spec_ids)}
            names = {spec_id: f"{labels[spec_id]}: {transforms[spec_id]}" for spec_id in spec_ids}
            fig, (top, bottom) = new_figure(rows=2, height=4.0)
            draw(top, "before the change", before_files, before_plans[1], before_plans[4], rows, names)
            draw(bottom, "after the change", after_files, after_plans[1], after_plans[4], rows, names)
            bottom.set_xlabel("hour the rows hold, UTC")
            # one legend entry per label, not per mark; the top panel has room on its right, where no mark sits
            seen = {}
            for ax in (top, bottom):
                seen.update(zip(*ax.get_legend_handles_labels()[::-1]))
            top.legend(list(seen.values()), list(seen.keys()), loc="center right", frameon=False)
            save(fig, "files-by-spec")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
