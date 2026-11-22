"""Register a second name for a table's metadata, then commit through each name in turn. The point rests on the
'archive rows after a commit through wiki' line: the registered name keeps the rows it read before that commit."""

from lib.lakehouse import ICEBERG_ID, create_iceberg_table, new_warehouse, open_catalog, parquet_files, read_cut_hour, scan_arrow

ARCHIVE_ID = "archive.pageviews_copy"


def rows_by_name(catalog):
    """Return the row counts that the two names read now, each through a freshly loaded table."""
    wiki = scan_arrow(catalog.load_table(ICEBERG_ID)).num_rows
    archive = scan_arrow(catalog.load_table(ARCHIVE_ID)).num_rows
    return wiki, archive


with new_warehouse() as wh:
    table = create_iceberg_table(wh)
    table.append(read_cut_hour(0))
    table.append(read_cut_hour(1))

    # register_table adds a catalog row that points at the metadata file as it is now.
    catalog = open_catalog(wh)
    catalog.create_namespace("archive")
    catalog.register_table(ARCHIVE_ID, table.metadata_location)
    wiki, archive = rows_by_name(catalog)
    print("wiki rows at registration:", wiki)
    print("archive rows at registration:", archive)

    # The first commit goes through the original name, so only that name's pointer moves.
    table.append(read_cut_hour(2))
    wiki_after, archive_after = rows_by_name(catalog)
    print("wiki rows after a commit through wiki:", wiki_after)
    print("archive rows after a commit through wiki:", archive_after)
    assert wiki_after > wiki
    assert archive_after == archive

    # Two commits go through the registered name, so its count differs from the original's. Each load is fresh,
    # because a loaded table keeps the state it had at load time.
    for hour in (3, 4):
        catalog.load_table(ARCHIVE_ID).append(read_cut_hour(hour))
    wiki_again, archive_again = rows_by_name(catalog)
    print("wiki rows after two commits through archive:", wiki_again)
    print("archive rows after two commits through archive:", archive_again)
    print("data files both names list:", len(set(parquet_files(catalog.load_table(ICEBERG_ID))) & set(parquet_files(catalog.load_table(ARCHIVE_ID)))))
    assert wiki_again == wiki_after
    assert archive_again > archive_after
