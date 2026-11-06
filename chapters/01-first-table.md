---
chapter_id: first-table
---

# Build a first table, end to end

By the end of this chapter you can commit batches of rows to a table, read the table back in DuckDB, and check the totals. This book is for data and analytics engineers who write SQL and Python and have to decide how their lakehouse tables are stored, changed, cleaned up, shared and moved between formats. The book builds one result: a pageviews table that you store, change, read as of an earlier snapshot, clean up, register in a catalog and move between formats. This chapter builds a rough version of it, one table written in three table formats (Apache Iceberg, Delta Lake and DuckLake) and read back into DuckDB, so you can compare the totals. Each later chapter makes one step of the result solid:

- Chapter 2 reads the table as it was at an earlier snapshot.
- Chapter 3 adds and renames columns without rewriting files.
- Chapter 4 changes the partitioning without rewriting old files.
- Chapter 5 corrects rows with deletes, updates and merges.
- Chapter 6 compacts the small files that hourly appends leave.
- Chapter 7 expires snapshots and removes files that no snapshot reaches.
- Chapter 8 registers tables in a catalog and queries them from DuckDB.
- Chapter 9 moves a table between formats and checks the copy.
- Chapter 10 chooses a format from what your tools support.

You know SQL, Python and Parquet, and you know what a partition is. This chapter does not explain them.

The book follows one case throughout: an hourly pageviews table, which we call the pageviews table. Each row says how many times one public Wikipedia article was viewed in one hour, in one language edition. The rows cover six hours of 3 June 2024, from 00:00 to 05:59 UTC. They come from the [Wikimedia pageviews dumps](https://dumps.wikimedia.org/other/pageviews/2024/2024-06/), which the Wikimedia project releases under CC0. The book keeps the 500 most-viewed titles in each hour, across the English, German, French and Japanese editions, after special pages are removed. The day is fixed and in the past, so the dumps for it will not change, and the data in the repository will not go stale. [The data folder's README](../examples/data/README.md) gives the source, the licence and the exact cut.

The data is messy on purpose. Titles carry underscores and non-Latin scripts, and the counts are skewed: the top title in hour 00 has far more views than the title at position 500. Every row in the cut has a `response_bytes` of 0, as the [data folder's README](../examples/data/README.md) records. The book keeps the column as the cut gives it, so it is 0 throughout. These values stay in the data, because a table format has to store them as they are.

## What a table format adds to files {#what-a-table-format-adds}

A folder of Parquet files can hold a table, but the folder does not say which files belong to the table. Anything that reads the folder sees every file in it, including a file a writer left half-finished. A table format fixes this with metadata: a record of the table's schema and of the data files that make up the table at each point in its history. A change to the table is a commit, which writes new metadata in a single step. That step is the one change that moves the table from one state to the next.

Each table format has its own metadata and its own words for these ideas. Iceberg calls the state after a commit a snapshot. Delta Lake numbers its versions. DuckLake records its snapshots in a [catalog file](#create-catalog-and-table). This chapter writes all three, so you can see the same rows in each.

Where the metadata sits differs too. An Iceberg table keeps a metadata file that names its current snapshot. In format version 2, which pyiceberg writes, each snapshot points to a manifest list, the manifest list names manifests, and each manifest names data files along with their statistics. The [Iceberg spec](https://iceberg.apache.org/spec/) defines each of those layers. A Delta Lake table keeps an ordered log of commit files in a `_delta_log` folder beside its data, as the [Delta Lake protocol](https://raw.githubusercontent.com/delta-io/delta/v4.4.1/PROTOCOL.md) describes. A DuckLake table keeps its metadata in the catalog file that you attach, and writes its data files into a folder beside that file.

The example below loads six hours into Iceberg, Delta Lake and DuckLake, one hour at a time, then reads each table back and compares the totals.

[Example: rough-lakehouse](../examples/rough-lakehouse.py)

Each format reports the same row count and the same view total. Each format also wrote one data file per hour:

- Iceberg: {{out:rough-lakehouse:iceberg rows}} rows, {{out:rough-lakehouse:iceberg views}} views, {{out:rough-lakehouse:iceberg data files}} data files.
- Delta Lake: {{out:rough-lakehouse:delta rows}} rows, {{out:rough-lakehouse:delta views}} views, {{out:rough-lakehouse:delta data files}} data files.
- DuckLake: {{out:rough-lakehouse:ducklake rows}} rows, {{out:rough-lakehouse:ducklake views}} views, {{out:rough-lakehouse:ducklake data files}} data files.

The last check line, `totals match: {{out:rough-lakehouse:totals match}}`, says the three totals are equal. The example also prints the three most-viewed titles over the six hours, read from the Iceberg table alone: `Main_Page`, `メインページ` and `Wikipedia:Hauptseite`, the main pages of the English, Japanese and German editions.

The example prints no snapshot IDs, version numbers or file names, since they change from run to run. [The section on appending each hour](#append-each-hour) explains what it prints instead.

DuckLake needs one extra step on your own machine. Run `INSTALL ducklake` and then `LOAD ducklake` in DuckDB. The book's runs instead load a copy of DuckDB's official, signed `ducklake` extension from a file in the repository, so they run without a network connection. DuckLake also has an inlining default that matters for the file counts: by default, it keeps inserts of 10 rows or fewer in its catalog rather than in a Parquet file. The [DuckLake data inlining page](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining) states the default: "Data inlining is enabled by default with a row limit of 10." The example sets the limit to 0, so every insert writes a file, and the file counts mean the same thing in every chapter.

### One load loop, three commit styles {#one-load-loop}

The helper `load_three_formats()` in `examples/lib/lakehouse.py` holds the loop that [rough-lakehouse](#what-a-table-format-adds) calls. The loop writes each hour to all three tables, and each line that writes is one commit in one format:

```python
for hour in hours:
    rows = read_cut_hour(hour)
    iceberg.append(rows)
    write_deltalake(delta_path, rows, mode="append")
    lake.register("hour", rows)
    lake.execute(f"INSERT INTO {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE} SELECT * FROM hour")
    lake.unregister("hour")
```

`read_cut_hour` returns one hour of the cut as an Arrow table, with the same five columns in every format. The Iceberg append creates a snapshot. The Delta append creates a new version, and it creates the table on the first call, when the path does not yet hold one. The DuckLake insert writes a Parquet file and records a snapshot in the catalog file. The three calls differ, and the table each one writes is the same six hours of rows. The loop itself checks nothing. It runs the six loads in order and stops if one of them raises an error. The checks come afterwards, in the example, where each format's totals are read back and compared.

### A commit names the table's files {#files-metadata-and-commit}

The [Apache Iceberg table spec](https://iceberg.apache.org/spec/) defines a snapshot in one sentence: "A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table." A snapshot therefore does two things. It names the data files that the table holds at that point, and it records the operation that made it. The spec lists the kinds of operation. For an append, the operation is `append`, which the spec describes as "Only data files were added and no files were removed."

The pageviews table makes this concrete. Each hour you load becomes a data file, and each load is one commit. After the first commit the table holds hour 00's file and nothing else. After the second commit it holds two files, and so on. A data file is never edited. A commit adds files, and the new snapshot names the files the table holds after the commit.

The table's count of data files comes from its snapshot, not from its folder. [rough-lakehouse](#what-a-table-format-adds) prints the number of data files each format reports for the table. That number is what the table says it holds, and a listing of the folder can show a different number.

**Working rule:** Ask the table what it holds, through its current snapshot, and not by listing its folder. The folder holds every file a writer put there, and the snapshot says which of those files the table reads.

## Why each format is taught through a Python library {#why-python-libraries}

The [rough-lakehouse](#what-a-table-format-adds) example wrote each format through a different library, and this section says why. The book writes Apache Iceberg and Delta Lake with the Python libraries that a reader will use to run them: pyiceberg, at version {{version:pyiceberg}}, and deltalake, at version {{version:deltalake}}. DuckDB, at version {{version:duckdb}}, reads what those libraries write. It reads an Iceberg table as an Arrow table from a scan, and it reads a Delta table as an Arrow table from a query that deltalake runs. DuckLake is the exception. DuckDB runs DuckLake itself, through its `ducklake` extension, and attaches the table's catalog file. The book does not use DuckDB's `iceberg` or `delta` extensions, and no example runs them.

| Format | Written by | Read by DuckDB through |
| --- | --- | --- |
| Apache Iceberg | pyiceberg, {{version:pyiceberg}} | Arrow table from a scan |
| Delta Lake | deltalake, {{version:deltalake}} | Arrow table from deltalake's `QueryBuilder` |
| DuckLake | DuckDB's `ducklake` extension, on DuckDB {{version:duckdb}} | `ATTACH` to a catalog file |

The choice has a cost. A reader who runs Iceberg through Spark or Trino meets more of the format there, with features this book does not cover. The book names those places and does not teach those engines. The gain is fewer moving parts: each format's own library writes it, and DuckDB reads the output. Each library's version is pinned, and the chapters name the pinned library version, not the version of the format's specification. The pins live in `examples/requirements.txt`, which lists every package with one exact version per line. A reader who installs from that file gets the libraries this chapter ran against. Use the Python version that `examples/.python-version` names, because pip does not select one.

**Working rule:** Write and read each table with the library whose version you pin, and say which version your reads use. Reach for an engine's own extension only when you have measured what the library cannot do for you.

## Land three hours in a table {#land-three-hours}

The [rough-lakehouse](#what-a-table-format-adds) example showed the whole loop, so this section slows it down. Landing a batch takes two steps: create the table, then commit each batch to it. The pageviews table lands with hours 00, 01 and 02 first, so each hour is one commit. That example loads hours 03 to 05 the same way, after the first three. The examples in the rest of this section keep to the Iceberg table, because it has the most to show.

### Create the table in a catalog {#create-catalog-and-table}

An Iceberg table needs a catalog, which is the registry that maps a table name to the table's current metadata. A Delta Lake table is found by its path, with no catalog, and a DuckLake table lives in its catalog file. The book uses pyiceberg's SQL catalog on SQLite for the Iceberg table. The catalog is one file, and the table's data files go in a warehouse folder beside it. The catalog holds pointers and metadata, not rows. Inside the catalog file, a table name such as `wiki.pageviews_hourly` maps to the location of the table's current metadata file. A commit writes a new metadata file and moves that mapping, so anyone who asks the catalog for the table gets the metadata that the most recent commit wrote. The helper `new_warehouse()` in `examples/lib/lakehouse.py` makes a temporary catalog and warehouse for each run, and removes them when the run ends. Because SQLite is a file and needs no server, the whole catalog runs on a laptop with the network off.

The table has five columns, and its schema says what each one holds:

```python
Schema(
    NestedField(1, "hour_utc", TimestampType(), required=False),
    NestedField(2, "domain_code", StringType(), required=False),
    NestedField(3, "page_title", StringType(), required=False),
    NestedField(4, "views", LongType(), required=False),
    NestedField(5, "response_bytes", LongType(), required=False),
)
```

The `hour_utc` column holds a timestamp without a time zone, in UTC, and the two counts are 64-bit integers. The helper `create_iceberg_table()` creates the table in the catalog, unpartitioned. Chapter 4 adds a partition spec to it.

**Working rule:** Write the schema out in code, with the type each column really has, before the first commit. The types are then decided on purpose, and not by whichever batch happens to arrive first.

### Append each hour as a commit {#append-each-hour}

Each append is one commit. The example below creates the table, appends hours 00, 01 and 02 in order, and then lists the snapshots in commit order with their operation and the rows each commit added.

[Example: append-hours](../examples/append-hours.py)

The first hour has {{out:append-hours:snapshot 1 rows added}} rows. The second and third have {{out:append-hours:snapshot 2 rows added}} each, so hour 00 is one row short. The data's privacy check removed one row from hour 00, as the data README explains. The three commits add {{out:append-hours:rows in table}} rows in total, across {{out:append-hours:data files}} data files, one for each commit. Each snapshot's operation is `{{out:append-hours:snapshot 1 operation}}`, the append operation the spec defines.

Each commit also writes a summary, and pyiceberg returns the snapshots in commit order, which is how the example reads the operation and the rows added. The summary is what the table records about its own commits, so it is the first place to look when a count is wrong.

The example prints `snapshot 1`, `snapshot 2` and `snapshot 3` in place of the snapshot IDs. An Iceberg snapshot ID is a long integer that the spec requires to be unique, and pyiceberg chooses it when the commit is made, so it changes from run to run. The example maps each ID to its position in commit order, so the printed output is the same on every run.

**Working rule:** Make one commit per batch you can name, and compare the rows each commit added with the count you expect from its source before you make the next commit. A wrong count is simpler to trace while its commit is the most recent one, and harder once more commits sit on top of it.

## Query the table with DuckDB {#query-with-duckdb}

With the table in place, you want to ask it questions. DuckDB runs SQL, but an Iceberg table is not a DuckDB table. The book reads the table's current snapshot as an Arrow table, which pyiceberg returns from a scan, and registers that Arrow table with DuckDB under the name `pageviews`. DuckDB then runs ordinary SQL over it, with no Iceberg extension. The scan is the table's own reading of the snapshot. It lists the data files that the snapshot names, reads them, and hands over only the rows they hold. DuckDB sees exactly those rows and nothing that the snapshot does not name.

The example below appends the three hours and asks two questions: the five most-viewed titles in hour 00, and the total views across the three hours. It checks the total against the sum that the scan itself gives.

[Example: scan-into-duckdb](../examples/scan-into-duckdb.py)

The list holds the five titles with the most views in hour 00, and the most-viewed title comes first. The query orders the rows by views, largest first, with the title as the tie-breaker, and the example prints them in that order. Each line shows the views first, then the title, separated by a tab. The total is {{out:scan-into-duckdb:total views in DuckDB}} views in DuckDB and {{out:scan-into-duckdb:total views in the scan}} views in the scan, and `totals match: {{out:scan-into-duckdb:totals match}}`.

A total that matches the scan's sum shows that DuckDB received every row the scan returned. It does not check the rows themselves, and it does not check the top-five query. A check on a total costs one line, so it is worth running after every change to a query. Checking the rows against their source is the job of the later chapters, which compare each change against the data they came from.

**Working rule:** Read an Iceberg table into DuckDB as the Arrow table from a scan, and compare a total with the scan's own sum, to confirm that DuckDB received every row. Then check each query's result against a source you can compare it with.

## What goes wrong in production {#first-table-production-failures}

The first chapter's path is short, and it hides four problems that you will meet in production.

**A repeated load adds the same rows again.** An append adds the data files it is given and removes none, as the spec's definition of the operation says. Nothing in the table keeps a second copy of hour 00 out. A commit that carries a second copy of hour 00 looks like any other commit. The [append-hours](#append-each-hour) example shows it: the repeated commit's operation is `{{out:append-hours:repeat of hour 00 operation}}`, it adds {{out:append-hours:repeat of hour 00 rows added}} rows, the same count as the first copy, and the table then holds {{out:append-hours:rows in table after the repeat}} rows, with no error. The only record that the load was meant to run once is the count you wrote down before it ran. Chapter 5 shows how to correct rows that were loaded wrongly.

**The folder holds more than the table.** A count of the files in the warehouse folder answers a different question from the count of the table's files. The folder holds every file a writer put there, and a snapshot defines what the table holds. A tool that counts Parquet files in the folder reports a number that the table never promised. Chapters 6 and 7 deal with files that the table no longer reaches.

**Snapshot IDs, versions and snapshot numbers are different kinds of number.** Iceberg IDs are 64-bit values. Delta Lake versions count from 0 in commit order. DuckLake numbers its snapshots as well, and it records a snapshot each time it creates a schema or a table, as well as each time it inserts rows. A script that stores one format's number and reads it as another format's version reads the wrong state.

**A small DuckLake insert writes no file.** By default, DuckLake keeps inserts of 10 rows or fewer in its catalog, so a file count of zero after a small insert is the expected result, not a failure. The examples in this book set the row limit to 0. A reader who copies an insert into their own catalog should know the default first.

**Working rule:** Write down the row count each commit should add, next to the load that makes it, and compare that count with the commit's summary after every load. The table does not check for a repeated load. A count you wrote down in advance is the check that does.

## Decisions this chapter made {#first-table-decisions}

- **Three examples in this chapter.** `rough-lakehouse` does the whole result in three formats, so each later chapter changes one part of a result the reader has already run. The other two, `append-hours` and `scan-into-duckdb`, build the Iceberg table one commit at a time and read it in DuckDB.
- **Hours 00 to 02 first, then 03 to 05.** The Iceberg examples land three hours and the rough example lands six. The totals in this chapter cover all six hours.
- **Unpartitioned tables.** Chapter 1 creates tables without a partition spec. Chapter 4 changes the partitioning. The pyiceberg-core package is pinned beside pyiceberg, because the day and hour transforms in chapter 4 need it.
- **Inlining off in the examples.** DuckLake examples set `DATA_INLINING_ROW_LIMIT` to 0, so each insert writes a Parquet file and the file counts mean the same thing wherever the book prints them.
- **Delta tables are read through deltalake's `QueryBuilder` only.** The reading helper in `examples/lib/lakehouse.py` takes that path, and each later chapter that reads a Delta table uses the same helper.
- **Real data, not a stand-in.** The examples read the cut of the six public dumps that [the data README](../examples/data/README.md) describes. The book ships no generated data for this chapter.
- **SQLite for the Iceberg catalog.** A SQLite file needs no server, so the Iceberg examples run on a laptop with the network off. The examples also turn off DuckDB's automatic extension download, so a run that needs the network fails instead of fetching.
- **Labels stand in for IDs.** The examples print `snapshot 1`, `snapshot 2` and so on in place of snapshot IDs, so the printed output does not change from run to run.
