"""Write one timestamp with nanoseconds to each format, and read back what each format kept."""

import pyarrow as pa
from deltalake import write_deltalake
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType

from lib.lakehouse import copy_to_ducklake, new_warehouse, NAMESPACE, read_delta, safe_error, sql_rows

NANOSECONDS = pa.table({"hour_utc": pa.array([1000000500, 1000000999], pa.timestamp("ns"))})


def main():
    try:
        with new_warehouse() as wh:
            print(f"written distinct values: {len(set(NANOSECONDS['hour_utc'].cast(pa.int64()).to_pylist()))}")
            table = wh.catalog.create_table(
                f"{NAMESPACE}.precision",
                schema=Schema(NestedField(1, "hour_utc", TimestampType(), required=False)),
            )
            try:
                table.append(NANOSECONDS)
            except Exception as exc:
                print(f"iceberg append: {safe_error(exc, wh)}")

            path = str(wh.root / "precision")
            write_deltalake(path, NANOSECONDS, mode="overwrite")
            kept = read_delta(path)["hour_utc"].cast(pa.int64()).to_pylist()
            print(f"delta read back as microseconds: {kept}")
            print(f"delta distinct values read back: {len(set(kept))}")

            target = copy_to_ducklake(wh, NANOSECONDS)
            rows = sql_rows(wh.con, f"SELECT epoch_ns(hour_utc) FROM {target}")
            print(f"ducklake epoch nanoseconds read back: {[row[0] for row in rows]}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
