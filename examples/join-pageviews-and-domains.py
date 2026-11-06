"""Join an Iceberg table, read through Arrow, with a DuckLake table in one DuckDB statement."""

from lib.lakehouse import (DUCKLAKE_ALIAS, NAMESPACE, connect_ducklake, create_iceberg_table,
                           create_ducklake_domains, new_warehouse, print_rows, read_cut_hour,
                           scan_arrow, sql_rows)

with new_warehouse() as wh:
    table = create_iceberg_table(wh)
    for hour in range(3):
        table.append(read_cut_hour(hour))
    con = connect_ducklake(wh.con)
    create_ducklake_domains(con, wh.root)

    pageviews = scan_arrow(table)
    joined = f"FROM pageviews p JOIN {DUCKLAKE_ALIAS}.{NAMESPACE}.domains d ON p.domain_code = d.domain_code"
    print("joined rows:", sql_rows(con, f"SELECT count(*) {joined}", pageviews=pageviews)[0][0])
    print("rows with no language:", sql_rows(con, f"""SELECT count(*) FROM pageviews p
        LEFT JOIN {DUCKLAKE_ALIAS}.{NAMESPACE}.domains d ON p.domain_code = d.domain_code
        WHERE d.domain_code IS NULL""", pageviews=pageviews)[0][0])
    print("top three by views:")
    print_rows(sql_rows(con, f"""SELECT p.domain_code, d.language, p.page_title, p.views {joined}
        ORDER BY p.views DESC, p.domain_code, p.page_title, p.hour_utc LIMIT 3""", pageviews=pageviews, keep_order=True),
        keep_order=True)
