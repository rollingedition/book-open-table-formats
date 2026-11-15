"""Write two timestamps that differ only below a microsecond to Iceberg, Delta Lake and DuckLake, and show what each kept.

The point rests on the Iceberg error line and on the Delta and DuckLake read-back lines."""

import pyarrow as pa
from deltalake import write_deltalake
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType

from lib.lakehouse import copy_to_ducklake, new_warehouse, NAMESPACE, read_delta, safe_error, sql_rows

# nanoseconds since 1970: one second plus 500 and 999 nanoseconds, so the two values differ below a microsecond
NANOSECONDS = pa.table({"hour_utc": pa.array([1000000500, 1000000999], pa.timestamp("ns"))})


def main():
    try:
        with new_warehouse() as wh:
            # cast to int64 gives the integer behind each timestamp, so distinct values are counted exactly
            print(f"written distinct values: {len(set(NANOSECONDS['hour_utc'].cast(pa.int64()).to_pylist()))}")
            # TimestampType stores microseconds; a version 2 table has no nanosecond type to declare instead
            table = wh.catalog.create_table(
                f"{NAMESPACE}.precision",
                schema=Schema(NestedField(1, "hour_utc", TimestampType(), required=False)),
            )
            try:
                # this append raises, because the table cannot hold nanoseconds; the except prints the error
                table.append(NANOSECONDS)
            except Exception as exc:
                print(f"iceberg append: {safe_error(exc, wh)}")

            path = str(wh.root / "precision")
            write_deltalake(path, NANOSECONDS, mode="overwrite")
            # the cast reads the count in the column's unit, which Delta wrote as microseconds, so 1000000 is one second
            kept = read_delta(path)["hour_utc"].cast(pa.int64()).to_pylist()
            print(f"delta read back as microseconds: {kept}")
            print(f"delta distinct values read back: {len(set(kept))}")

            target = copy_to_ducklake(wh, NANOSECONDS)
            # epoch_ns returns nanoseconds since 1970, so a value cut to microseconds would end in three zeros
            rows = sql_rows(wh.con, f"SELECT epoch_ns(hour_utc) FROM {target}")
            print(f"ducklake epoch nanoseconds read back: {[row[0] for row in rows]}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
