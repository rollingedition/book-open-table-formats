---
description: "Teaches Apache Iceberg (pyiceberg), Delta Lake (deltalake) and DuckLake (DuckDB) on one hourly pageviews table: creating and appending tables, reading earlier snapshots, adding and renaming columns, changing partitions, deleting, upserting and merging rows, compacting small files, expiring snapshots and cleaning up files, registering tables in catalogs, copying a table to another format, and choosing a format. Use when you create or append to an Iceberg, Delta Lake or DuckLake table, read an older snapshot or version, add or rename a column, change partitioning, delete or merge corrected rows, compact small files, expire snapshots or vacuum old files, find a table in a catalog, move a table to another format, or choose a format that several tools must read."
---

## Create a table and append batches

**When:** You need a new Iceberg, Delta Lake or DuckLake table, or a new batch must go into one that exists.

1. Write and read each table with the library version your project pins. [Python libraries](#why-python-libraries)
2. Write the schema in code, with each column's real type, before the first commit. [Create the table](#create-catalog-and-table)
3. Make one commit per batch, using the format's own write: an Iceberg append, a Delta append or a DuckLake insert. [One load loop](#one-load-loop)
4. Before each commit, write down the row count it should add. Compare that count with the one the commit reports. [Append each hour](#append-each-hour)
5. Before you count DuckLake files, know the inlining default. Small inserts stay in the catalog unless the row limit is 0. [Production failures](#first-table-production-failures)

**Check:** Run [the example](examples/rough-lakehouse.py). The last check line should read totals match: {{out:rough-lakehouse:totals match}}.

**Pitfalls:**
- A repeated load adds the same rows again, and nothing in the table stops it. Compare the expected count with the commit's count. [Production failures](#first-table-production-failures)
- The folder can hold files that the table does not read. Count files from the snapshot, not the folder. [Production failures](#first-table-production-failures)

## Read a table as it was at an earlier commit

**When:** A total changed and you must find out whether the table or the query changed, or a report must reproduce an earlier state.

1. List the table's history, and use that list as the only source of commit numbers. [Every commit is a snapshot](#every-commit-is-a-snapshot)
2. Read each commit's operation. An append only adds rows. An overwrite, a delete or a replace can change rows already in the table. [Every commit is a snapshot](#every-commit-is-a-snapshot)
3. In Iceberg, pass a snapshot ID from the snapshot list to the scan. Never pass a position in the list. [Snapshots in pyiceberg](#snapshots-in-pyiceberg)
4. In Delta Lake, read a version by its number in the history, through deltalake's query builder. [Read by number](#versions-in-ducklake-and-delta), [Decisions](#snapshot-decisions)
5. In DuckLake, read a snapshot number taken from the catalog's snapshot table. Do not count inserts yourself. [Read by number](#versions-in-ducklake-and-delta)
6. Compare formats by the hour each commit loaded, and compare row counts for that hour before you trust either. [Production failures](#snapshot-production-failures)

**Check:** Run [the example](examples/read-as-of-snapshot.py). The second snapshot should hold {{out:read-as-of-snapshot:snapshot 2 rows}} rows.

**Pitfalls:**
- A label passed where an ID belongs fails with an error that names the value. Look the ID up instead. [Snapshots in pyiceberg](#snapshots-in-pyiceberg)
- A timestamp picks a state by the clock. Use IDs or numbers when the answer must not change between runs. [Production failures](#snapshot-production-failures)

## Add a column to a table that already holds data

**When:** A new column is added, and the table already holds rows written before it existed.

1. Add the column with the format's schema call: `update_schema()` with `add_column` in Iceberg, or `alter.add_columns` in Delta Lake. Do not rewrite the files. [Add a column](#add-response-bytes)
2. Expect old rows to read as null. A field with no initial default reads null where the file has no value. [Columns tracked by ID](#columns-tracked-by-id)
3. Count nulls by hour, where each hour is one write. Rows written before the add should be null, and rows written after it should not be. [Old files read as null](#old-files-read-as-null)
4. If the pattern breaks, find the writer that sent different columns or nulls before you trust the column. [Old files read as null](#old-files-read-as-null)

**Check:** Run [the example](examples/add-column-to-existing-files.py). The rows written before the add with a null value, {{out:add-column-to-existing-files:iceberg rows written before the add with load_batch null}}, should equal the rows written before the add, {{out:add-column-to-existing-files:iceberg rows written before the add}}.

**Pitfalls:**
- A null in an added column looks like missing data. Check the pattern by hour, not the null count alone. [Old files read as null](#old-files-read-as-null)
- A schema change can leave the snapshot count and the file count where they were. [Production failures](#schema-production-failures)

## Rename a column without rewriting its files

**When:** A column's name is wrong, and the table already holds data under the old name.

1. In Iceberg, call `update_schema()` and `rename_column`. The field ID stays the same. [Rename by ID](#rename-column-without-rewrite)
2. Check that the field ID and the data file count are the same before and after the call. [Rename by ID](#rename-column-without-rewrite)
3. Tell readers that older snapshots still show the old name. [Rename by ID](#rename-column-without-rewrite)
4. In Delta Lake at the pinned release, do not use `set_column_metadata` to rename. It commits a version and changes no name. [No rename in Delta Lake](#rename-in-delta-lake)
5. To rename a Delta column, rewrite the table: read it, rename the column in memory, and write it back with mode and schema mode both set to overwrite. Plan it as a job. [No rename in Delta Lake](#rename-in-delta-lake)

**Check:** Run [the example](examples/rename-by-id.py). The field ID before the rename, {{out:rename-by-id:page_title field ID before the rename}}, should equal the field ID after it, {{out:rename-by-id:article_title field ID after the rename}}.

**Pitfalls:**
- A Delta rename that does nothing returns without an error, and the first query on the new name then fails. Read the column names back after every change. [Production failures](#schema-production-failures)
- Older versions keep their old names. A job pinned to an old version must use the names that were current then. [Production failures](#schema-production-failures)

## Change partitioning without rewriting old files

**When:** Readers' filters change, and the current partitioning plans too many files.

1. Before the change, count the files that your readers' filters plan, and the files a full scan plans. [Check what a query scans](#check-what-a-query-scans)
2. In Iceberg, change the spec in one `update_spec()` block: remove the old field and add the new one. Old files keep the spec they were written under. [Move to hourly partitions](#daily-to-hourly-partitions), [Partition specs](#partition-specs-are-versioned)
3. Check the spec list, then count data files under each spec. A count that moves without a planned rewrite means the change did more than you meant. [Files under each spec](#files-written-under-each-spec)
4. In Delta Lake, a partition change is a full overwrite with schema mode set to overwrite. Partition columns must be plain columns. [Other formats](#partitioning-in-other-formats)
5. In DuckLake, `ALTER TABLE ... SET PARTITIONED BY` applies to later loads only. Check the file count after the first load. [Other formats](#partitioning-in-other-formats)
6. After the first load under the new partitioning, rerun the counts from step 1, and keep them with the change record. [Check what a query scans](#check-what-a-query-scans)

**Check:** Run [the example](examples/plan-files-by-partition.py). The full scan should plan {{out:plan-files-by-partition:after the change, no filter plans}} files after the change, up from {{out:plan-files-by-partition:before the change, no filter plans}} before it.

**Pitfalls:**
- The day and hour transforms need the pyiceberg-core package. Without it, the first append under them fails, possibly at the first scheduled load. [Production failures](#partition-production-failures)
- A grain finer than the filters need gives many small files. Judge a change by plan counts, because the rows are the same before and after. [Production failures](#partition-production-failures)

## Delete rows

**When:** Rows that match a condition must be removed, and you need to know what each format writes to disk.

1. Count the rows the condition matches before the delete. A condition that matches no row makes pyiceberg warn and write no snapshot. [Delete rows](#delete-rows-iceberg-and-delta)
2. Expect Iceberg and Delta Lake deletes to rewrite each data file that holds a match. Check how many files that is. [Copy-on-write and merge-on-read](#copy-on-write-and-merge-on-read)
3. In DuckLake, `DELETE` writes a delete file and leaves the data files in place. Count the delete files too, since a small delete can be inlined and write none. [Copy-on-write and merge-on-read](#copy-on-write-and-merge-on-read), [Choose a format](#choice-production-failures)
4. Do not turn on Delta deletion vectors unless every reader in your set implements the feature. [Deletion vectors](#delta-deletion-vectors)

**Check:** Run [the example](examples/delete-low-views.py). The deleted rows should match across formats: {{out:delete-low-views:Iceberg deleted rows}} in Iceberg, {{out:delete-low-views:Delta Lake deleted rows}} in Delta Lake and {{out:delete-low-views:DuckLake deleted rows}} in DuckLake.

**Pitfalls:**
- A loop of single-row deletes costs one rewrite per delete. Gather the deletes into one write. [Production failures](#corrections-production-failures)
- The book does not measure the cost of reading through delete files. Measure it on your own table before you rely on merge-on-read. [Copy-on-write and merge-on-read](#copy-on-write-and-merge-on-read)

## Upsert or merge a corrected batch

**When:** A correction re-sends rows, some of which the table already holds and some of which are new.

1. Check that the batch holds each key once. The key is the set of columns that identify one row. [Late data as a correction](#late-data-as-correction)
2. Do not append a correction, because an append stores every row a second time. [Late data as a correction](#late-data-as-correction)
3. In pyiceberg, call `upsert` with the key columns as `join_cols`, and read the updated and inserted counts from its result. [Upserts in pyiceberg](#upserts-in-pyiceberg)
4. In DuckLake, write a `MERGE INTO` with a `WHEN MATCHED AND` condition that compares the columns you update. [MERGE in DuckLake](#merge-in-ducklake)
5. Run the same batch a second time, and check that it changes nothing. [Upserts in pyiceberg](#upserts-in-pyiceberg)
6. The book has no tested Delta Lake upsert or merge. Test one on your own table before you rely on it. [Decisions](#corrections-decisions)

**Check:** Run [the example](examples/redeliver-hour-upsert.py). The second upsert should report {{out:redeliver-hour-upsert:rows updated on second upsert}} updated and {{out:redeliver-hour-upsert:rows inserted on second upsert}} inserted rows.

**Pitfalls:**
- A repeated key fails the write, and a retry of the same batch fails again. Deduplicate the batch first. [Production failures](#corrections-production-failures)
- One upsert can add up to three snapshots, so count snapshots from the snapshot list, not from the calls. [Upserts in pyiceberg](#upserts-in-pyiceberg)

## Compact small files

**When:** Frequent loads have left many small data files, and reads or planning have slowed.

1. Count the data files from the table's own listing after each load. [Small files](#hourly-appends-leave-small-files)
2. Choose the schedule from how fast files arrive and the file size your readers need. A daily run is a starting point, not a measured best. [Small files](#hourly-appends-leave-small-files)
3. In Delta Lake, call `optimize.compact()`. [Delta Lake](#compact-in-delta-lake)
4. In DuckLake, call `ducklake_merge_adjacent_files`. It skips files that have delete files, which need `ducklake_rewrite_data_files`. [Merge adjacent files](#merge-adjacent-files)
5. In Iceberg, pyiceberg has no compaction call. Overwrite an unpartitioned table with its own scan, read as a stream, and run it while no other writer commits. [Rewrite to compact](#compact-iceberg-by-rewriting)
6. After each run, check the file count, the current row count against the count before, and that an older version still reads. If a check fails, stop. [Check each compaction](#check-each-compaction)

**Check:** Run [the example](examples/file-counts-before-after.py). The file count should fall from {{out:file-counts-before-after:files before compaction}} to {{out:file-counts-before-after:files after compaction}}.

**Pitfalls:**
- Storage grows before it shrinks, because the replaced files stay on disk until cleanup. [Production failures](#compaction-production-failures)
- The book does not test a compaction that overlaps a commit. Schedule it where no writer commits. [Production failures](#compaction-production-failures)

## Expire snapshots and remove unreferenced files

**When:** Old snapshots or versions keep storage in use, or a cleanup is planned.

1. Decide which snapshots or versions any reader, report or audit may still need. Expire the rest before you remove any file. [Snapshots keep files alive](#snapshots-keep-files-alive)
2. In Iceberg, expire by snapshot ID. The call deletes no data files. Find the files that no snapshot lists, and remove them only after no writer can commit them. [Expire in pyiceberg](#expire-snapshots-in-iceberg), [Orphan files](#find-orphan-files)
3. In Delta Lake, run `vacuum` as a dry run, compare its count with the table's history, then run it for real. Keep the default retention unless you have checked the longest read and write. [Dry run first](#vacuum-dry-run-first)
4. In DuckLake, expire the snapshots, clean up the scheduled files, then clean up the orphans. Run each one as a dry run before the real call. [Expire and clean up in DuckLake](#expire-and-clean-up-in-ducklake)
5. Use `cleanup_all` only when one catalog owns the data folder and no reader uses those files. [Expire and clean up in DuckLake](#expire-and-clean-up-in-ducklake)

**Check:** Run [the example](examples/expire-iceberg-snapshots.py). The table should still return {{out:expire-iceberg-snapshots:rows in the current snapshot}} rows, and {{out:expire-iceberg-snapshots:unreferenced data files left on disk}} unreferenced file should remain on disk.

**Pitfalls:**
- A query that started before a cleanup can fail partway, when a file it reads is gone. Schedule cleanup outside long reads. [Production failures](#maintenance-production-failures)
- A dry run can differ from the real run if a writer commits between them. Stop if the counts differ by more than you can explain. [Production failures](#maintenance-production-failures)

## Register tables in a catalog and open them from DuckDB

**When:** Several tools must find a table by name, a Delta table must be opened by path, or DuckDB must join tables from two catalogs.

1. Record which catalog or path names each table, and who owns it. [What a catalog holds](#what-a-catalog-holds)
2. Open a pyiceberg SQL catalog under one name everywhere in your code. [Register in a SQL catalog](#register-iceberg-in-sql-catalog)
3. Reload an Iceberg table before you read it when another writer may have committed since it was loaded. [Register in a SQL catalog](#register-iceberg-in-sql-catalog)
4. Attach a DuckLake catalog by its file path. Copy the catalog file with its data folder, while no session has it attached. [Attach a DuckLake catalog](#attach-ducklake-catalog)
5. Open a Delta table by its folder path. A missing path is an error, not an empty table. [Find by path](#find-delta-by-path)
6. To join across catalogs, read the Iceberg table as Arrow, register it in the DuckDB session, and count the rows that find no match. [Join across catalogs](#join-tables-from-two-catalogs)

**Check:** Run [the example](examples/join-pageviews-and-domains.py). The rows with no language should be {{out:join-pageviews-and-domains:rows with no language}}.

**Pitfalls:**
- A loaded table keeps the state it had at load time. Nothing fails, and the job reports fewer rows than the table holds. [Production failures](#catalog-production-failures)
- A fresh, empty catalog file pointed at a live data folder makes every data file an orphan. A cleanup with `cleanup_all` would then delete live files. [Production failures](#catalog-production-failures)

## Copy a table into another format and check the copy

**When:** A table must move to another format, and the new table must own its files and its history.

1. List what the copy will not keep: the commit history, the partition spec, and any value finer than the target stores. Decide for each one. [What a copy keeps and loses](#what-a-copy-keeps-and-loses)
2. Before the first copy, test timestamps with a value below a microsecond, and choose the rule in advance. [Timestamp precision](#timestamp-precision)
3. Read the source through its own library into Arrow, and write it with the target's own writer. [Copy into DuckLake](#iceberg-to-ducklake)
4. For DuckLake, create the empty table, set its partition spec with `ALTER`, and only then insert the rows. Print the partition list. [Copy into DuckLake](#iceberg-to-ducklake)
5. For an Iceberg target, write the target schema yourself, and print both schemas after the copy. [Copy into pyiceberg](#delta-to-iceberg)
6. After the last write to the source, compare the row counts and a digest of typed values over the same columns, on both sides. Keep the source until both match. [Sorted digest](#check-copy-with-digest)

**Check:** Run [the example](examples/verify-copies-by-digest.py). The Iceberg-to-DuckLake digest should match: {{out:verify-copies-by-digest:iceberg to ducklake match}}.

**Pitfalls:**
- A digest is true only for the rows that existed when it ran. A writer that commits later makes the copy stale. [Production failures](#migration-production-failures)
- Delta Lake cut nanosecond timestamps to microseconds with no error, and the row count did not change. Check values, not only counts. [Production failures](#migration-production-failures)

## Choose a format from the readers that will open it

**When:** Several tools will write and read a table, and you must choose a format, a format version or a feature.

1. List every reader and writer with its pinned version. A tool left off the list is a reader you have not tested. [Checklist](#checklist-for-new-table)
2. Measure each feature on each reader, not only on the writer. A successful write shows only that the writer can produce the files. [Measure support](#measure-support-before-choosing)
3. Pyiceberg at its pin writes format version 2 and refuses to create format version 3. Measure what your writer creates. [No version 3 table](#iceberg-v3-and-deletion-vectors)
4. Turn on Delta deletion vectors only when every reader implements the feature, and read the protocol back after the change. [Deletion vectors](#delta-deletion-vectors)
5. Promise a rename only where support is measured. Iceberg renames by field ID, and deltalake has no rename at these pins. [Checklist](#checklist-for-new-table)
6. Upgrade the readers first, then the writers, then the protocol. Read the table after each step. [Checklist](#checklist-for-new-table)

**Check:** Run [the example](examples/iceberg-format-versions.py). Creating format version 3 should raise {{out:iceberg-format-versions:format version 3 create}}.

**Pitfalls:**
- A vendor's support page describes one engine at one version. Use it to find what to test, and check the claim against your own pins. [Production failures](#choice-production-failures)
- The DuckLake catalog version is tied to the DuckDB release. An older DuckDB may not open the catalog after an upgrade. [Production failures](#choice-production-failures)
