"""Adds load_batch to a table that already holds three hours, in Iceberg and then in Delta Lake, and counts the nulls on each side of the add.
Read the four "with load_batch null" lines, which show the rows before the add and the rows after it in each format.
"""

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.schema import Field, PrimitiveType
from pyiceberg.types import IntegerType

from lib.lakehouse import ICEBERG_ID, delta_file_count, iceberg_file_count, new_warehouse, read_cut_hour, read_delta, split_nulls


def stamp(rows, hour):
    """Return one hourly load with load_batch set to that load's hour number."""
    # int32 is the width IntegerType stores, so the Arrow column matches the Iceberg field
    return rows.append_column("load_batch", pa.array([hour] * rows.num_rows, type=pa.int32()))


with new_warehouse() as wh:
    # the schema comes from hour 0, which has no load_batch yet
    table = wh.catalog.create_table(ICEBERG_ID, schema=read_cut_hour(0).schema)
    for hour in range(3):  # hours 00 to 02, each one commit, all written before the add
        table.append(read_cut_hour(hour))
    print(f"iceberg snapshots before the add: {len(table.snapshots())}")
    print(f"iceberg data files before the add: {iceberg_file_count(table)}")
    with table.update_schema() as update:
        update.add_column("load_batch", IntegerType())
    print(f"iceberg snapshots after the add: {len(table.snapshots())}")
    print(f"iceberg data files after the add: {iceberg_file_count(table)}")
    for hour in range(3, 6):  # hours 03 to 05, written after the add
        table.append(stamp(read_cut_hour(hour), hour))
    iceberg_rows = table.scan().to_arrow()
    print(f"iceberg rows in the table: {iceberg_rows.num_rows}")
    # the split is at 3, the first hour written after the add
    split_nulls(wh.con, iceberg_rows, "iceberg", "load_batch", 3, sides=("written before the add", "written after the add"))

    path = f"{wh.root}/delta/pageviews_hourly"
    for hour in range(3):  # hours 00 to 02, appended as three versions before the add
        write_deltalake(path, read_cut_hour(hour), mode="append")
    print(f"delta version before the add: {DeltaTable(path).version()}")
    print(f"delta data files before the add: {delta_file_count(DeltaTable(path))}")
    DeltaTable(path).alter.add_columns([Field("load_batch", PrimitiveType("integer"))])
    print(f"delta version after the add: {DeltaTable(path).version()}")
    print(f"delta data files after the add: {delta_file_count(DeltaTable(path))}")
    for hour in range(3, 6):  # hours 03 to 05, written after the add
        write_deltalake(path, stamp(read_cut_hour(hour), hour), mode="append")
    delta_rows = read_delta(path)
    print(f"delta rows in the table: {delta_rows.num_rows}")
    # the same split as the Iceberg half, at 3
    split_nulls(wh.con, delta_rows, "delta", "load_batch", 3, sides=("written before the add", "written after the add"))
