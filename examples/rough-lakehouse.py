"""Load six hours into Iceberg, Delta Lake and DuckLake, then read each table back.

The rows and views lines print each format's totals, and the assert after them checks that the three agree.
The titles at the end come from the Iceberg table alone.
"""

from lib.lakehouse import (
    format_report,
    load_three_formats,
    new_warehouse,
    print_rows,
    safe_error,
    sql_rows,
    scan_arrow,
)

# page_title breaks ties in views, so titles with equal views always print in the same order.
TOP_THREE = """
    SELECT sum(views) AS views, page_title FROM pageviews
    GROUP BY page_title ORDER BY views DESC, page_title LIMIT 3
"""


def main():
    try:
        with new_warehouse() as wh:
            # range(6) is hours 0 to 5, the six hours of the cut, one commit per hour in each format.
            iceberg, delta_path, lake = load_three_formats(wh, range(6))
            report = format_report(wh, iceberg, delta_path, lake)

            for name, (rows, views, files) in report.items():
                print(f"{name} rows: {rows}")
                print(f"{name} views: {views}")
                print(f"{name} data files: {files}")
            # A set of (rows, views) pairs has one entry only when all three formats agree.
            totals = {(rows, views) for rows, views, _ in report.values()}
            assert len(totals) == 1, "the three formats disagree on rows or views"

            print("top three page titles over six hours (views, page_title):")
            # pageviews names the Arrow table for the SQL; keep_order prints the rows in the SQL's order, not re-sorted.
            print_rows(sql_rows(wh.con, TOP_THREE, keep_order=True, pageviews=scan_arrow(iceberg)), keep_order=True)
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
