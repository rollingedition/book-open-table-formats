"""MERGE a changed language label into the domains table in DuckLake, then run the same MERGE again."""

import pyarrow as pa

from lib.lakehouse import attach_ducklake, connect_ducklake, print_rows, read_domains, new_warehouse, safe_error

MERGE = """
MERGE INTO lake.wiki.domains AS t
USING labels AS s
ON t.domain_code = s.domain_code
WHEN MATCHED AND t.language <> s.language THEN UPDATE SET language = s.language
WHEN NOT MATCHED THEN INSERT (domain_code, language) VALUES (s.domain_code, s.language)
"""


def main():
    with new_warehouse() as wh:
        lake = connect_ducklake(wh.con)
        attach_ducklake(lake, "lake", wh.root, inlining_row_limit=0)
        lake.execute("CREATE SCHEMA lake.wiki")
        lake.execute("CREATE TABLE lake.wiki.domains (domain_code VARCHAR, language VARCHAR)")
        lake.register("dims", read_domains())
        lake.execute("INSERT INTO lake.wiki.domains SELECT * FROM dims")

        lake.register("labels", pa.table({"domain_code": ["fr"], "language": ["French (France)"]}))
        print("rows changed by the merge:", lake.execute(MERGE).fetchall()[0][0])
        print("rows changed by the same merge again:", lake.execute(MERGE).fetchall()[0][0])
        print_rows(lake.execute("SELECT domain_code, language FROM lake.wiki.domains").fetchall())


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # the redacted error is what a reader sees
        print(safe_error(exc))
        raise SystemExit(1)
