"""Check each copy with a sorted digest, and show that one changed value fails the check."""

from lib.lakehouse import (copy_to_ducklake, create_iceberg_table, digest_rows, new_warehouse, read_cut_hour,
                           read_delta, safe_error, scan_arrow, sql_rows, write_delta_hours)


def same(source, copy):
    return digest_rows(source) == digest_rows(copy)


def check_iceberg_to_ducklake():
    with new_warehouse() as wh:
        table = create_iceberg_table(wh)
        for hour in range(3):
            table.append(read_cut_hour(hour))
        target = copy_to_ducklake(wh, scan_arrow(table))
        source_rows = sql_rows(wh.con, "SELECT * FROM source", source=scan_arrow(table))
        copy_rows = wh.con.execute(f"SELECT * FROM {target}").fetchall()
        first = copy_rows[0]
        changed = [(*first[:3], first[3] + 1, first[4]), *copy_rows[1:]]
        print(f"iceberg to ducklake rows: {len(source_rows)} and {len(copy_rows)}")
        print(f"iceberg to ducklake match: {same(source_rows, copy_rows)}")
        print(f"iceberg to ducklake with one changed view count match: {same(source_rows, changed)}")


def check_delta_to_iceberg():
    with new_warehouse() as wh:
        path = write_delta_hours(wh.root, range(3))
        table = create_iceberg_table(wh)
        table.append(read_delta(path))
        source_rows = sql_rows(wh.con, "SELECT * FROM source", source=read_delta(path))
        copy_rows = sql_rows(wh.con, "SELECT * FROM copy", copy=scan_arrow(table))
        print(f"delta to iceberg rows: {len(source_rows)} and {len(copy_rows)}")
        print(f"delta to iceberg match: {same(source_rows, copy_rows)}")


def main():
    try:
        check_iceberg_to_ducklake()
        check_delta_to_iceberg()
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
