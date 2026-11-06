"""Turn on deletion vectors in a Delta Lake table, delete the rows below 180 views, and print the protocol the table declares."""

from deltalake import DeltaTable, write_deltalake

from lib.lakehouse import new_warehouse, read_cut_hour, read_delta

LOW_VIEWS = 180


def deletion_vector_files(path):
    """Count files under the table folder that are not Parquet or the Delta log."""
    return sum(1 for item in path.rglob("*")
               if item.is_file() and item.suffix != ".parquet" and "_delta_log" not in item.parts)


def main():
    with new_warehouse() as wh:
        path = wh.root / "dv_table"
        write_deltalake(str(path), read_cut_hour(0), configuration={"delta.enableDeletionVectors": "true"})
        table = DeltaTable(str(path))
        protocol = table.protocol()
        print("minimum reader version:", protocol.min_reader_version)
        print("minimum writer version:", protocol.min_writer_version)
        print("reader features:", ", ".join(sorted(protocol.reader_features)))

        before = read_delta(str(path)).num_rows
        metrics = table.delete(f"views < {LOW_VIEWS}")
        print("rows deleted:", before - read_delta(str(path)).num_rows)
        print("data files rewritten:", metrics["num_removed_files"])
        print("rows through QueryBuilder:", read_delta(str(path)).num_rows)
        print("deletion vector files:", deletion_vector_files(path))


if __name__ == "__main__":
    main()
