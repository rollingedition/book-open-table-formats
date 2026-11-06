"""Add load_batch after three hours of appends, in Iceberg and in Delta Lake.

load_batch holds the number of the hourly load that wrote each row. Rows written
before the add have no value for it, so they read as null. Later loads set it.
"""

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.schema import Field, PrimitiveType
from pyiceberg.types import IntegerType

from lib.lakehouse import ICEBERG_ID, delta_file_count, iceberg_file_count, new_warehouse, read_cut_hour, read_delta, split_nulls


def stamp(rows, hour):
    """Return one hourly load with load_batch set to that load's hour number."""
    return rows.append_column("load_batch", pa.array([hour] * rows.num_rows, type=pa.int32()))


with new_warehouse() as wh:
    table = wh.catalog.create_table(ICEBERG_ID, schema=read_cut_hour(0).schema)
    for hour in range(3):
        table.append(read_cut_hour(hour))
    print(f"iceberg snapshots before the add: {len(table.snapshots())}")
    print(f"iceberg data files before the add: {iceberg_file_count(table)}")
    with table.update_schema() as update:
        update.add_column("load_batch", IntegerType())
    print(f"iceberg snapshots after the add: {len(table.snapshots())}")
    print(f"iceberg data files after the add: {iceberg_file_count(table)}")
    for hour in range(3, 6):
        table.append(stamp(read_cut_hour(hour), hour))
    iceberg_rows = table.scan().to_arrow()
    print(f"iceberg rows in the table: {iceberg_rows.num_rows}")
    split_nulls(wh.con, iceberg_rows, "iceberg", "load_batch", 3, sides=("written before the add", "written after the add"))

    path = f"{wh.root}/delta/pageviews_hourly"
    for hour in range(3):
        write_deltalake(path, read_cut_hour(hour), mode="append")
    print(f"delta version before the add: {DeltaTable(path).version()}")
    print(f"delta data files before the add: {delta_file_count(DeltaTable(path))}")
    DeltaTable(path).alter.add_columns([Field("load_batch", PrimitiveType("integer"))])
    print(f"delta version after the add: {DeltaTable(path).version()}")
    print(f"delta data files after the add: {delta_file_count(DeltaTable(path))}")
    for hour in range(3, 6):
        write_deltalake(path, stamp(read_cut_hour(hour), hour), mode="append")
    delta_rows = read_delta(path)
    print(f"delta rows in the table: {delta_rows.num_rows}")
    split_nulls(wh.con, delta_rows, "delta", "load_batch", 3, sides=("written before the add", "written after the add"))
