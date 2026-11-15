"""Merges the corrected hour 02 and its late row into the DuckLake copy with a SQL MERGE, then runs the same MERGE again. The matched-row line splits the changed count into inserts and updates; the second changed-row line is the rerun."""

import pyarrow as pa

from lib.lakehouse import hour_corrections, load_three_formats, new_warehouse, read_cut_hour, safe_error

# The WHEN MATCHED condition is what keeps a rerun from counting matched rows that already agree.
MERGE = """
MERGE INTO lake.wiki.pageviews_hourly AS t
USING batch AS s
ON t.hour_utc = s.hour_utc AND t.domain_code = s.domain_code AND t.page_title = s.page_title
WHEN MATCHED AND t.views <> s.views THEN UPDATE SET views = s.views
WHEN NOT MATCHED THEN INSERT (hour_utc, domain_code, page_title, views, response_bytes)
    VALUES (s.hour_utc, s.domain_code, s.page_title, s.views, s.response_bytes)
"""


def main():
    with new_warehouse() as wh:
        _, _, lake = load_three_formats(wh, range(6))
        corrected = hour_corrections(read_cut_hour(2))
        # a copy of the hour's first row, with a page title the table never held
        late = corrected.slice(0, 1).set_column(
            corrected.schema.get_field_index("page_title"),
            "page_title",
            pa.array(["Late_arrival_example"], type=pa.string()),
        )
        lake.register("batch", pa.concat_tables([corrected, late]))
        print("source rows:", lake.execute("SELECT count(*) FROM batch").fetchall()[0][0])
        # the join counts source rows whose key the table already holds, which splits the changes into inserts and updates
        matched = lake.execute(
            "SELECT count(*) FROM lake.wiki.pageviews_hourly AS t JOIN batch AS s "
            "ON t.hour_utc = s.hour_utc AND t.domain_code = s.domain_code AND t.page_title = s.page_title"
        ).fetchall()[0][0]
        print("source rows matched:", matched)
        # DuckDB returns one count for a MERGE, with updates and inserts added together
        print("rows changed by the merge:", lake.execute(MERGE).fetchall()[0][0])
        print("rows changed by the same merge again:", lake.execute(MERGE).fetchall()[0][0])
        print("rows in table:", lake.execute("SELECT count(*) FROM lake.wiki.pageviews_hourly").fetchall()[0][0])


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # the redacted error is what a reader sees
        print(safe_error(exc))
        raise SystemExit(1)
