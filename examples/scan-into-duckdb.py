"""Read the current Iceberg snapshot into DuckDB and check the totals."""

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
            arrow = scan_arrow(table)

            top = sql_rows(
                wh.con,
                """
                SELECT views, page_title FROM pageviews
                WHERE hour_utc = TIMESTAMP '2024-06-03 00:00:00'
                ORDER BY views DESC, page_title LIMIT 5
                """,
                keep_order=True,
                pageviews=arrow,
            )
            print("top five pages for hour 00 (views, page_title):")
            print_rows(top, keep_order=True)

            duckdb_total = sql_rows(wh.con, "SELECT sum(views) FROM pageviews", pageviews=arrow)[0][0]
            pyiceberg_total = sum(arrow["views"].to_pylist())
            print(f"total views in DuckDB: {duckdb_total}")
            print(f"total views in the scan: {pyiceberg_total}")
            print(f"totals match: {duckdb_total == pyiceberg_total}")
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
