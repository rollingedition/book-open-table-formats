"""Draw, for each one-hour filter after the change from a day spec to an hour spec, the one file it plans and that file's spec.
The prose rests on the lines for the hour 01 and hour 04 filters."""

from lib.figures import ACCENT, MUTED, new_figure, save
from lib.lakehouse import create_iceberg_table, hour_filter, new_warehouse, read_cut_hour, safe_error

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform, HourTransform


def main():
    try:
        with new_warehouse() as wh:
            day_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day"))
            table = create_iceberg_table(wh, partition_spec=day_spec)
            for hour in range(3):  # hours 00 to 02, under the day spec
                table.append(read_cut_hour(hour))
            with table.update_spec() as update:
                update.remove_field("ts_day")
                update.add_field("hour_utc", HourTransform(), "ts_hour")
            for hour in range(3, 6):  # hours 03 to 05, under the hour spec
                table.append(read_cut_hour(hour))

            # labels number the specs in the order they were made, so the output names no spec ID
            spec_ids = sorted(table.specs())
            labels = {spec_id: f"spec {n}" for n, spec_id in enumerate(spec_ids, start=1)}
            rows = {spec_id: -n for n, spec_id in enumerate(spec_ids, start=1)}  # plots draw y upward, so spec 1 at -1 is the top row
            fig, ax = new_figure(height=2.6)
            for hour in range(6):
                (task,) = table.scan(row_filter=hour_filter(hour)).plan_files()  # each one-hour filter plans exactly one file
                if hour in (1, 4):  # the two filters the section plans
                    print(f"hour {hour:02d} filter plans a file under: {labels[task.file.spec_id]}")
                    style = dict(color=ACCENT, marker="D", label="planned by the hour 01 or 04 filter")
                else:
                    style = dict(facecolors="none", edgecolors=MUTED, label="planned by another one-hour filter")
                ax.scatter(hour, rows[task.file.spec_id], s=70, zorder=3, **style)
            handles = dict(zip(*ax.get_legend_handles_labels()[::-1]))  # one legend entry per label, not per mark
            ax.legend(handles.values(), handles.keys(), frameon=False, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=2)
            ax.set_xticks(range(6), [f"{hour:02d}" for hour in range(6)])
            ax.set_yticks([-1, -2], ["spec 1: day", "spec 2: hour"])
            ax.set_ylim(-2.5, -0.5)
            ax.set_xlabel("hour the filter asks for, UTC")
            save(fig, "files-by-spec")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
