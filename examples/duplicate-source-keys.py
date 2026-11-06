"""Show what an upsert or a MERGE does when the batch has two rows for one key."""

import pyarrow as pa

from lib.lakehouse import KEY, hour_corrections, load_three_formats, new_warehouse, read_cut_hour, safe_error, scan_arrow

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
        iceberg, _, lake = load_three_formats(wh, range(6))
        corrected = hour_corrections(read_cut_hour(2))
        # A corrected row, sent twice: the batch holds two rows with one key.
        before = read_cut_hour(2)["views"].to_pylist()
        changed = [i for i, after in enumerate(corrected["views"].to_pylist()) if after != before[i]]
        doubled = pa.concat_tables([corrected, corrected.slice(changed[0], 1)])

        try:
            iceberg.upsert(doubled, join_cols=list(KEY))
            print("Iceberg upsert: no error")
        except Exception as exc:
            print("Iceberg upsert:", safe_error(exc, wh))
        print("Iceberg rows after the failed upsert:", scan_arrow(iceberg).num_rows)

        lake.register("batch", doubled)
        try:
            lake.execute(MERGE)
            print("DuckLake merge: no error")
        except Exception as exc:
            print("DuckLake merge:", safe_error(exc, wh))
        print("DuckLake rows after the failed merge:", lake.execute("SELECT count(*) FROM lake.wiki.pageviews_hourly").fetchall()[0][0])


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # the redacted error is what a reader sees
        print(safe_error(exc))
        raise SystemExit(1)
