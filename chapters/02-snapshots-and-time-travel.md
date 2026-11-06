---
chapter_id: snapshots-and-time-travel
---

# Read a table as it was at an earlier snapshot

By the end of this chapter you can read a table as it was after an earlier commit, in each of the three formats.

At the end you can list a table's commits, read one of them by the number its format uses, and compare the numbers across formats without mixing them up.

This chapter follows the book's running case, the hourly pageviews table `wiki.pageviews_hourly`. A dashboard total for the table changed after the third hour landed. The owner asks two questions: what the total was before that commit, and whether the table changed or only the query did. Answering them needs the table's history, and the ability to read an old state of the table. The table has five columns: `hour_utc`, `domain_code`, `page_title`, `views` and `response_bytes`. The dashboard sums `views`. The examples in this chapter build their own copy of the table the way [chapter 1](#land-three-hours) does: they create it and append hours 00, 01 and 02 as three commits. Hour 00 has {{out:snapshot-history:snapshot 1 rows added}} rows, and each of the other two hours has {{out:snapshot-history:snapshot 2 rows added}}.

## Every commit is a snapshot {#every-commit-is-a-snapshot}

[Chapter 1](#append-each-hour) lands the three hours as three commits. Each commit leaves a snapshot behind, and that is what you read later.

A commit changes the table's metadata in one atomic step, so a reader sees the table either before the commit or after it, never in between. The [Iceberg spec](https://iceberg.apache.org/spec/) describes each change as "an atomic swap" of metadata files. In Apache Iceberg, each commit creates a snapshot, which the [Iceberg table spec](https://iceberg.apache.org/spec/) defines as "the state of a table at some time," used "to access the complete set of data files in the table." A snapshot has a unique ID, a creation time, and a summary. The summary has a required field, `operation`, which is one of `append`, `replace`, `overwrite` or `delete`. An `append` added data files and removed none. A later commit adds a snapshot and leaves the earlier ones alone, which is what lets the dashboard owner ask about a state that is no longer the newest.

The example below lists the three snapshots in commit order, with each one's operation and rows added.

[Example: snapshot-history](../examples/snapshot-history.py)

The first commit added {{out:snapshot-history:snapshot 1 rows added}} rows, which is hour 00. Hour 00 has one row fewer than the others because the data cut dropped one row from it, as the [data README](../examples/data/README.md) explains. The second and third each added {{out:snapshot-history:snapshot 2 rows added}} rows. All three commits used `append`, so none of them rewrote or removed a file. The dashboard owner's question starts here: the table has three states to choose from, and the history names them.

pyiceberg gives each snapshot a long integer ID. We do not print those IDs. The example maps them, in commit order, to the labels "snapshot 1", "snapshot 2" and "snapshot 3" with `snapshot_labels`, and prints only the labels.

The same three hours went to a Delta Lake table through `write_deltalake`, one write per hour. Delta Lake calls each commit a version, and its history lists the operation and the rows written for each one. Delta numbers its versions from 0, so the first write is version 0. The book prints Delta's own numbers, so the example sorts the history by version and prints "version 0", "version 1" and "version 2". Each Delta write is a `WRITE` operation that adds the same rows as the matching Iceberg snapshot: {{out:snapshot-history:version 0 rows added}} rows for version 0, then {{out:snapshot-history:version 1 rows added}} and {{out:snapshot-history:version 2 rows added}}.

The history answers the first half of the owner's question. It says which commit changed the table and how many rows that commit added. An `append` only adds rows, so it cannot change a total for hours that are already in the table. If the total for an hour moved, the history must name an `overwrite`, a `delete` or a `replace` as the cause, and you then read the snapshots on either side of it. The history does not say what a query did, and that part of the question needs the reads in the next section.

**Working rule:** Before you read or change a table, list its history in the format you use, and use that list as the only source of commit numbers. Sort the list by commit order, label Iceberg snapshots with `snapshot_labels` before you print them, and print Delta versions as Delta numbers them. Read the operation with each commit. An `append` only adds files, so it cannot have removed rows, while an `overwrite` or `delete` can.

### What a snapshot points to {#what-a-snapshot-points-to}

A snapshot holds no rows. It points to the files that hold them. In format versions 2 and 3, a snapshot records a manifest list, and that is where its files are found. The spec's definition says the snapshot is the entry point to the complete set of data files in the table, so a reader starts from the snapshot and follows it to the files.

That makes a snapshot a set of pointers, and the consequence matters for every section that follows. A snapshot reads only while the files it names still exist. Time travel is therefore not a backup. Time travel reads files that the table still keeps, and the table keeps them while some snapshot names them. [Chapter 7](#snapshots-keep-files-alive) removes the old snapshots and the files only they named, which ends the time travel to those states.

The commit's operation shows what sharing is possible. The spec defines `append` as the operation where "only data files were added and no files were removed." So snapshot 3 names the files of hour 00 again, because the third commit did not rewrite them. Several snapshots can name the same file, and a commit costs only the files it wrote plus the pointers to them.

The row counts in [Snapshots in pyiceberg](#snapshots-in-pyiceberg) show the pointers at work. Snapshot 2 reads back {{out:read-as-of-snapshot:snapshot 2 rows}} rows, which is the {{out:snapshot-history:snapshot 1 rows added}} rows of hour 00 plus the {{out:snapshot-history:snapshot 2 rows added}} of hour 01, and nothing from hour 02. Snapshot 3 reads all three hours. Each snapshot reads the rows that its commits had made visible by then, and no more. The gap between the two counts is the rows that the third commit added, {{out:snapshot-history:snapshot 3 rows added}} of them, so the history and the reads agree.

**Working rule:** Treat an old snapshot as readable only while its files exist. Before you remove files, decide which snapshots you keep, and expire the others first. A query over an old snapshot that fails after a cleanup is a missing file. The fix is to find that file in a copy of the table, not to read a different snapshot in its place.

## Read an earlier snapshot {#read-an-earlier-snapshot}

Reading an earlier snapshot means naming it when you scan the table. Each format has its own way to name one. Iceberg takes a snapshot ID. DuckLake takes a snapshot number in `AT (VERSION => n)`. Delta Lake takes a version number. The three numbers do not count the same things.

The owner's first question is what the total was before the third hour landed. The answer is the state after the second commit, snapshot 2.

### Snapshots in pyiceberg {#snapshots-in-pyiceberg}

In pyiceberg {{version:pyiceberg}}, `scan(snapshot_id=...)` reads the table as of that snapshot, and a scan with no snapshot ID reads the current snapshot. The example takes the IDs in commit order, reads snapshots 2 and 3 by ID, and sums the views in each, so the case's totals come from the table's own history. It also reads the current table and compares it with snapshot 3.

[Example: read-as-of-snapshot](../examples/read-as-of-snapshot.py)

Snapshot 2 has {{out:read-as-of-snapshot:snapshot 2 rows}} rows and {{out:read-as-of-snapshot:snapshot 2 views}} views in total. That total is the answer to the owner's first question: what the table held before the third commit. Snapshot 3 has {{out:read-as-of-snapshot:snapshot 3 rows}} rows and {{out:read-as-of-snapshot:snapshot 3 views}} views. The current table gives {{out:read-as-of-snapshot:current rows}} rows, and the example prints `{{out:read-as-of-snapshot:current equals snapshot 3}}` for the comparison. Reading the table with no ID gives the same rows as its newest snapshot, and the example compares the rows themselves, not only their counts.

The views column answers the owner's second question. Snapshot 3 has more views than snapshot 2, and the difference is the views that hour 02 added. The total changed because the third commit added rows, not because the dashboard's query changed. Two reads of the same table show that without any change to the query.

A reader who has the label "snapshot 2" in mind might pass the number 2 as the ID. The call fails, and that failure is the safe outcome:

`{{out:read-as-of-snapshot:read by label}}`

The error names the number it was given, which is not a snapshot ID, and nothing in the table changed. The reader then knows to look the ID up. The labels exist for the prose, and the IDs for the scan.

The example gets each ID from `table.snapshots()`, and it maps each ID to its label with `snapshot_labels`, which takes the IDs in commit order. Nothing in the example computes an ID from a count, and it prints only labels, never IDs.

**Working rule:** Get snapshot IDs from `table.snapshots()` and pass the ID you looked up, never a label's number. Map the IDs to labels in commit order before you print anything. When the question is the state after a commit, find that commit in the history first, and read the snapshot it created.

### Read a commit by its number in each format {#versions-in-ducklake-and-delta}

Delta Lake reads an earlier version with `DeltaTable(path, version=n)`. The version is the commit's position in the table's history, counted from 0, so version 0 is the first write. The helper `read_delta` takes the path and an optional version and returns the rows as a sorted Arrow table. In this book, every Delta read goes through deltalake's `QueryBuilder`, which the helper calls for you. The [decisions section](#snapshot-decisions) gives the reason.

DuckLake keeps a snapshot for each change to the catalog, and the change includes creating a schema and creating a table. DuckLake numbers its snapshots from 0 as well, so the first insert in a new catalog is not number 1. The [DuckLake time travel page](https://ducklake.select/docs/stable/duckdb/usage/time_travel) shows the clause for reading one, `SELECT * FROM tbl AT (VERSION => 3);`, and says the snapshot to query can be specified by timestamp or "explicitly using a snapshot identifier". The book uses the identifier. You read the number from `ducklake_snapshots`, not from your own count of inserts.

The three-version example writes the same three hours to a Delta table and a DuckLake table, one commit per hour in each format. It reads each commit back by its number and prints both row counts. It also counts the DuckLake snapshots before the first insert, and shows one failure.

[Example: three-version-numbers](../examples/three-version-numbers.py)

Before the first insert, the DuckLake catalog holds {{out:three-version-numbers:snapshots before the first insert}} snapshots: the default schema that DuckLake creates on attach, the `wiki` schema and the table. This book labels DuckLake's snapshots from 1, setup included, so the first insert is "snapshot 4", which is DuckLake's own number 3 plus one. A reader who counts inserts from 1 gets the wrong label.

After hour 00, Delta version 0 has {{out:three-version-numbers:after hour 00, Delta version 0 rows}} rows and DuckLake snapshot 4 has {{out:three-version-numbers:after hour 00, DuckLake snapshot 4 rows}}. After hour 01, Delta version 1 has {{out:three-version-numbers:after hour 01, Delta version 1 rows}} rows and DuckLake snapshot 5 has {{out:three-version-numbers:after hour 01, DuckLake snapshot 5 rows}}. After hour 02, which is the full table, Delta version 2 has {{out:three-version-numbers:after hour 02, Delta version 2 rows}} rows and DuckLake snapshot 6 has {{out:three-version-numbers:after hour 02, DuckLake snapshot 6 rows}}. The hour is what makes the rows comparable, because the formats do not share a numbering.

The table below sets the three numbers side by side.

| Format | Number that names a commit | Which changes it counts | How you read it |
| --- | --- | --- | --- |
| Iceberg | Snapshot ID, a long integer | Each commit to the table | `scan(snapshot_id=...)` |
| Delta Lake | Version, counted from 0 | Each commit to the table | `DeltaTable(path, version=n)` |
| DuckLake | Snapshot number, counted from 0 (the book's label is one higher) | Every change to the catalog, including schema and table creation | `AT (VERSION => n)` |

**Working rule:** Compare formats by the hour each commit loaded, never by raw number. Print Iceberg and DuckLake snapshots with their labels, print Delta versions as Delta numbers them, and bind the counts to them. Read Delta only through `read_delta`, and read DuckLake only with numbers that come from `ducklake_snapshots`. If a table in one format gets commits that the other does not, the labels stop lining up, and the history of each format becomes the only reliable record.

## What goes wrong in production {#snapshot-production-failures}

The failures in this chapter are the ones a reader is likely to meet first. The first two appear in this chapter's examples. The third and fourth are argued from the documentation and the book's rules, and the fifth and sixth lean on [chapter 1](#what-a-table-format-adds) and [chapter 7](#snapshots-keep-files-alive). The first is the number that is not an ID. A failed scan is a safe failure, because the error says what was wrong. Worse failures come from reading a number that is valid in one format and means something else in another, or from a query that runs and returns something unexpected. The [read-as-of-snapshot example](#snapshots-in-pyiceberg) shows the first kind: the ID guess fails loudly, and the error text names the value that was passed.

The second failure comes from a query that fetches a time column. The three-version example includes a select of `snapshot_time` from `ducklake_snapshots`, and the call fails:

`{{out:three-version-numbers:select time}}`

The message says the pytz module is missing. The pinned environment does not install it. DuckDB's Python client needs pytz to turn a `TIMESTAMPTZ` column such as `snapshot_time` into Python values. The example avoids the problem by selecting only `snapshot_id` and filtering on `changes`, so it never fetches `snapshot_time`. To read the time anyway, cast it with `snapshot_time::VARCHAR`. Leave out the column or cast it; we would rather not add a package the book does not pin.

The third failure is a timestamp read. DuckLake accepts a timestamp in `AT (TIMESTAMP => ...)`, and Iceberg snapshots have a creation time. A timestamp picks a state by the clock, and the clock is the time when each commit ran. In production that is a good way to ask for "the table as it was at nine in the morning". An example would have to take a clock time from the run, which changes on every run, so the book uses IDs and version numbers instead. Choose the timestamp form when you need a point in wall-clock time and you have a record of when each commit ran. Choose the number form when you need the same answer every time.

The fourth failure is a number that drifts. Two formats can hold the same table, and the same label can then name different states. The book's example commits the same hours in the same order, so the labels agree. A production table that receives a repair in one format and not the other will not agree. Even with identical histories, the raw numbers do not line up across formats, because DuckLake's snapshot numbers also count schema and table creation. A dashboard that reads "version 4" from both formats will show different totals, so compare commits by their labels. The check is to compare the row counts for the label in both formats before you trust either number. The three-version example does that.

The fifth failure is about where DuckLake puts small writes. The [data inlining page](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining) says "Data inlining is enabled by default with a row limit of 10." A small insert can therefore stay in the catalog and create no Parquet file, so a file count taken after it reads low. The example's batches have 500 rows, which is over that limit, so under the documented default each of its inserts is written as a file. A count of data files is only meaningful once you know the row limit, and [chapter 1](#what-a-table-format-adds) names the default limit.

The sixth failure is the one this chapter warns about most. An old snapshot shows the table as it was written, and only while the files it names still exist. Nothing fails at the cleanup itself. The error appears when a reader asks for a snapshot whose files are gone. [Chapter 7](#snapshots-keep-files-alive) shows which files a cleanup removes.

**Working rule:** Read the error text before you change the query. It names the value, the module or the version that failed, and that is the fastest route to the fix. Compare each count in one format with the same label in the other format before you trust either one. Keep the snapshot your question needs readable until the question is answered, and expire only the snapshots that no question still needs. Record the label and the format's name with every number you report, so that a reader of the report can reproduce it.

## Decisions this chapter made {#snapshot-decisions}

- **Labels, not IDs.** The examples print labels such as "snapshot 2" for Iceberg and DuckLake. The Iceberg IDs and the DuckLake snapshot numbers never reach the output. Delta prints its version numbers, which count from 0 and are the numbers a reader passes to `DeltaTable`. A reader can rerun an example and compare its text line by line, and a diff shows a change in the data or the library, never a change in a random ID. The labels are mapped in commit order, so they keep their meaning when a library returns its history in another order.
- **IDs and version numbers, not timestamps.** A timestamp read depends on the clock and on when each commit ran. We read by ID or number, so that a reader gets the same rows from the same commit whenever the example runs. A production reader who needs wall-clock time should use the timestamp form and keep a record of when each commit ran.
- **Delta reads through `read_delta` only.** The helper reads through deltalake's `QueryBuilder`. At these pins, a process that reads Delta into Arrow with `to_pyarrow_table()` aborted at exit in some of our test runs. We fix the read path rather than work around the abort, so no example calls `os._exit` or hides the exit status.
- **DuckLake through the helpers only.** The examples load DuckLake with `connect_ducklake` and attach it with `attach_ducklake`. We never set `allow_unsigned_extensions`. The book's runs load DuckDB's official, signed DuckLake extension from the vendored file in the repository, after the helper checks both hashes against `SOURCE.toml`, so an offline run needs no download. Only on a reader's own machine, when that vendored file is absent, does `connect_ducklake` run `INSTALL ducklake`.
- **Narrow queries on the catalog.** The examples read `ducklake_snapshots` by named column and leave out `snapshot_time`, because that column needs a module the pins do not install. We would rather name the columns we need than add a dependency that the pins do not record.
- **Labels counted from one for Iceberg and DuckLake, Delta's own numbers for Delta.** Iceberg and DuckLake snapshots get "snapshot N", counted from 1 over every snapshot the format records. For DuckLake that includes the three setup snapshots, so DuckLake's own number is N minus 1. Delta prints its real version, counted from 0. Each label says only which commit it is in its own table. It never says that two formats hold the same state, and the example checks that by comparing row counts.
- **Failures printed, not raised.** Two examples end with a call that fails on purpose, and the example catches the error and prints it. The failure is shown because a reader will meet it, and the error text is printed in full so the reader can match it against their own output.
- **Read the history before the state.** The examples list each format's commits, and only then read a commit by its number. A chapter reader gets the operation and the row count for each commit before any scan, so a count that looks wrong can be checked against the commit that produced it.
