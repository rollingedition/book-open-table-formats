"""Open a Delta Lake table by its path, with no catalog, and read it through QueryBuilder."""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import new_warehouse, read_cut_hour, read_delta

with new_warehouse() as wh:
    path = str(wh.root / "delta" / "pageviews_hourly")
    for hour in range(3):
        write_deltalake(path, read_cut_hour(hour), mode="append")

    table = DeltaTable(path)
    print("delta version:", table.version())
    print("rows at that version:", read_delta(path).num_rows)
    print("rows at version 0:", read_delta(path, version=0).num_rows)

    # The path is the only name a Delta table has here: a wrong path is a missing table.
    try:
        DeltaTable(str(wh.root / "delta" / "pageviews_typo"))
        print("missing path: opened")
    except Exception as exc:
        print("missing path:", type(exc).__name__)
