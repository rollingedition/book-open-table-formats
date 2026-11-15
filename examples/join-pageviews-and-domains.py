"""Join an Iceberg table, read through Arrow, with a DuckLake table in one DuckDB statement. The point rests on the
'rows with no language' line, and the top-three block shows which rows the join returns first."""

from lib.lakehouse import (DUCKLAKE_ALIAS, NAMESPACE, connect_ducklake, create_iceberg_table,
                           create_ducklake_domains, new_warehouse, print_rows, read_cut_hour,
                           scan_arrow, sql_rows)

with new_warehouse() as wh:
    table = create_iceberg_table(wh)
    for hour in range(3):                                # hours 0, 1 and 2 of the cut
        table.append(read_cut_hour(hour))
    con = connect_ducklake(wh.con)
    create_ducklake_domains(con, wh.root)

    pageviews = scan_arrow(table)                        # read now: the join sees the table as it is at this line
    # sql_rows registers pageviews under that keyword name, so the SQL can refer to it as pageviews
    joined = f"FROM pageviews p JOIN {DUCKLAKE_ALIAS}.{NAMESPACE}.domains d ON p.domain_code = d.domain_code"
    print("joined rows:", sql_rows(con, f"SELECT count(*) {joined}", pageviews=pageviews)[0][0])
    # the left join keeps every pageview, and this condition counts the ones that found no domain row
    print("rows with no language:", sql_rows(con, f"""SELECT count(*) FROM pageviews p
        LEFT JOIN {DUCKLAKE_ALIAS}.{NAMESPACE}.domains d ON p.domain_code = d.domain_code
        WHERE d.domain_code IS NULL""", pageviews=pageviews)[0][0])
    print("top three by views:")
    print_rows(sql_rows(con, f"""SELECT p.domain_code, d.language, p.page_title, p.views {joined}
        ORDER BY p.views DESC, p.domain_code, p.page_title, p.hour_utc LIMIT 3""", pageviews=pageviews, keep_order=True),
        keep_order=True)
