"""Load six hours into Iceberg, Delta Lake and DuckLake, then read each table back."""

from lib.lakehouse import (
    format_report,
    load_three_formats,
    new_warehouse,
    print_rows,
    safe_error,
    sql_rows,
    scan_arrow,
)

TOP_THREE = """
    SELECT sum(views) AS views, page_title FROM pageviews
    GROUP BY page_title ORDER BY views DESC, page_title LIMIT 3
"""


def main():
    try:
        with new_warehouse() as wh:
            iceberg, delta_path, lake = load_three_formats(wh, range(6))
            report = format_report(wh, iceberg, delta_path, lake)

            for name, (rows, views, files) in report.items():
                print(f"{name} rows: {rows}")
                print(f"{name} views: {views}")
                print(f"{name} data files: {files}")
            totals = {(rows, views) for rows, views, _ in report.values()}
            print(f"totals match: {len(totals) == 1}")

            print("top three page titles over six hours (views, page_title):")
            print_rows(sql_rows(wh.con, TOP_THREE, keep_order=True, pageviews=scan_arrow(iceberg)), keep_order=True)
    except Exception as exc:
        print(safe_error(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
