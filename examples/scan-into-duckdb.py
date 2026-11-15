"""Read the current Iceberg snapshot into DuckDB and check the totals.

The point that DuckDB received every row rests on the two total views lines, which the assert compares.
"""

from lib.lakehouse import (
    create_iceberg_table,
    new_warehouse,
    print_rows,
    read_cut_hour,
    safe_error,
    scan_arrow,
    sql_rows,
)


def main():
    try:
        with new_warehouse() as wh:
            table = create_iceberg_table(wh)
            for hour in range(3):
                table.append(read_cut_hour(hour))
            # The current snapshot's rows as one Arrow table, sorted by every column.
            arrow = scan_arrow(table)

            top = sql_rows(
                wh.con,
                # hour_utc is UTC, so this is hour 00 of 3 June 2024; page_title breaks ties in views.
                """
                SELECT views, page_title FROM pageviews
                WHERE hour_utc = TIMESTAMP '2024-06-03 00:00:00'
                ORDER BY views DESC, page_title LIMIT 5
                """,
                # keep_order returns the rows in the order the SQL sorted them, not sorted again.
                keep_order=True,
                pageviews=arrow,
            )
            print("top five pages for hour 00 (views, page_title):")
            print_rows(top, keep_order=True)

            # One row with one column: the sum.
            duckdb_total = sql_rows(wh.con, "SELECT sum(views) FROM pageviews", pageviews=arrow)[0][0]
            # A second route to the same total, summed in Python from the scan's own rows.
            pyiceberg_total = sum(arrow["views"].to_pylist())
            print(f"total views in DuckDB: {duckdb_total}")
            print(f"total views in the scan: {pyiceberg_total}")
            assert duckdb_total == pyiceberg_total, "DuckDB and the scan disagree on the total"
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
