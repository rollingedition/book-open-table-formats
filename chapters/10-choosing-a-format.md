---
chapter_id: choosing-a-format
---

# Choose a format from what your tools support

By the end of this chapter, you can list every tool that reads or writes your tables, measure what each one supports at its pinned version, and choose a format and its features from those measurements.

Across chapters 1 to 9, one table, `wiki.pageviews_hourly`, went through its whole life in three formats: Apache Iceberg, Delta Lake and DuckLake. Each format did the same jobs in a different way, and each library supported a different subset of them. The choice you face is not which format is best in general. It is which format every tool in your team can open, change and clean up, at the versions you run. This chapter sets out how to measure that, shows the measurements for this book's pins, and gives a short guide for the choice.

In the running case, the readers of `wiki.pageviews_hourly` are a dashboard that queries through DuckDB, a pipeline that appends through pyiceberg, and an archive job that reads a Delta Lake copy through deltalake. Each reader has its own pin, and each one must open the data the others write. That is the situation the rest of this chapter prepares you for.

Three things make the choice harder than it looks. The features that differ most between formats, such as deletes, renames and partition changes, are the ones a team needs first. Each library changes on its own schedule, so a table one tool reads at one version may not open at another. And a check that passes on one machine says nothing about the versions your other tools run. The sections in this chapter deal with each of these in turn.

## Measure support before you choose {#measure-support-before-choosing}

A format name does not tell you what a tool supports. Support belongs to a tool at one version: a writer can create a feature that a reader cannot open, and a vendor page describes the vendor's engine, not the library on your laptop. So start from the readers, not the format. Write down every tool that will read the table, with its version, and then test each feature you plan to use on each of those versions.

The examples measure in that order: the reader first, then the feature. The `iceberg-format-versions` example creates an Iceberg table in format version 2, deletes rows from it, and asks pyiceberg for format version 3. The `feature-check` example turns on deletion vectors in a Delta Lake table and reads it back. The `ducklake-catalog-version` example prints a DuckLake catalog's version, and the `ducklake-delete-files` and `ducklake-inlined-delete` examples count delete files after a small and a large delete. Each prints one labelled line per result, so you can compare the output with the tables in [the support matrix](#support-matrix-as-of-pins) and with your own readers.

The person who owns the table should run the measurements on the machines that run the readers, not on a developer's laptop with different versions. Keep the record in a small table with one row per reader. The columns are the reader, its pinned version, the features your tables need and the date you measured them. A feature with no measurement counts as unsupported, and that rule is the one that prevents most of the failures later in this chapter.

| Reader | Pinned version | Features you need | Measured on |
| --- | --- | --- | --- |
| pyiceberg | {{version:pyiceberg}} | Format version 2 writes, deletes | Date of your run |

Read each printed line as a claim that holds at one version of one tool. A line that says a write was refused is as useful as one that says it worked, because the refusal tells you which format version to drop. The examples print class names, not messages, so the output stays the same when a library rewords an error.

**Working rule:** Measure each reader on its own pinned version before you choose a format version or a feature. Write each result down with the version it was measured at. Run the measurement again whenever any reader's version changes, and do not reuse a support table from a vendor page.

## Read what the pinned tools did {#support-matrix-as-of-pins}

These tables state what the pinned tools did in this book's runs. Each column is one tool: pyiceberg for Iceberg, deltalake for Delta Lake, and the DuckLake extension in DuckDB for DuckLake. The pins are pyiceberg {{version:pyiceberg}}, deltalake {{version:deltalake}} and DuckDB {{version:duckdb}}. DuckDB {{version:duckdb}} is a long-term support release, dated Oct 21, 2026 ([DuckDB release calendar](https://duckdb.org/release_calendar)). DuckLake 1.0 was released on Apr 13, 2026 ([DuckLake release calendar](https://ducklake.select/release_calendar.html)). The catalog at these pins reports version {{out:ducklake-catalog-version:ducklake catalog version}}, and that is the version readers must open. The extension is a separate version: DuckDB reports its build as {{out:ducklake-catalog-version:ducklake extension version}}, and the extension ships with DuckDB {{version:duckdb}}, so its version follows DuckDB's. The last column names the example or source behind each cell. Not shown here means a gap in this book, not a missing feature.

[Example: ducklake-catalog-version](../examples/ducklake-catalog-version.py)

### Writing and reading versions {#matrix-writing-and-reading}

| Versions and reads | pyiceberg | deltalake | DuckLake | Shown in |
| --- | --- | --- | --- | --- |
| Write rows as commits | Yes | Yes | Yes | [Land three hours](#land-three-hours) |
| Read an earlier commit by number | Yes, by snapshot | Yes, by version | Yes, by snapshot | [Iceberg](#snapshots-in-pyiceberg), [Delta and DuckLake](#versions-in-ducklake-and-delta) |
| Create format version 3 | No, raises an error | — | — | [Example](../examples/iceberg-format-versions.py) |
| Rename a column | Yes, by field ID | No, a silent no-op | Not shown here | [Iceberg](#rename-column-without-rewrite), [deltalake](#rename-in-delta-lake) |
| Column mapping at create | — | Yes | — | [deltalake](#rename-in-delta-lake) |

### Changing and cleaning up {#matrix-changing-and-cleaning-up}

| Changes and cleanup | pyiceberg | deltalake | DuckLake | Shown in |
| --- | --- | --- | --- | --- |
| Delete rows | Rewrites files | Rewrites files | Writes delete files | [Chapter 5](#delete-rows-iceberg-and-delta) |
| Delete files written | None | None | Yes, unless inlined | [Format versions](../examples/iceberg-format-versions.py), [feature check](../examples/feature-check.py), [DuckLake](../examples/ducklake-delete-files.py), [inlined](../examples/ducklake-inlined-delete.py) |
| Upsert or merge | Upsert, several snapshots | Not shown here | `MERGE INTO` | [Upserts](#upserts-in-pyiceberg), [MERGE](#merge-in-ducklake) |
| Change partitioning | Old files keep the old spec | Overwrite with schema mode | Change with `ALTER TABLE` | [Chapter 4](#daily-to-hourly-partitions), [other formats](#partitioning-in-other-formats) |
| Compact small files | No method; overwrite the table | `optimize.compact()` | Merge adjacent files | [Iceberg](#compact-iceberg-by-rewriting), [deltalake](#compact-in-delta-lake), [DuckLake](#compact-in-ducklake) |
| Expire snapshots | By ID or `older_than`; deletes no files | Vacuum by retention | Expire, then clean up | [Iceberg](#expire-snapshots-in-iceberg), [deltalake](#vacuum-dry-run-first), [DuckLake](#expire-and-clean-up-in-ducklake) |

[Example: ducklake-delete-files](../examples/ducklake-delete-files.py)

[Example: ducklake-inlined-delete](../examples/ducklake-inlined-delete.py)

### Catalogs and DuckDB {#matrix-catalogs-and-duckdb}

| Catalogs and DuckDB | pyiceberg | deltalake | DuckLake | Shown in |
| --- | --- | --- | --- | --- |
| Catalog needed | Yes, a SQL catalog | No, found by path | Yes, the catalog file | [Chapter 8](#what-a-catalog-holds), [by path](#find-delta-by-path), [catalog file](#attach-ducklake-catalog) |
| DuckDB reads it | Through Arrow | Through Arrow, from `QueryBuilder` | Natively | [Chapter 1](#query-with-duckdb), [Chapter 9](#check-copy-with-digest), [Chapter 8](#attach-ducklake-catalog) |
| DuckDB writes it | — | — | Natively, through SQL | [Chapter 8](#attach-ducklake-catalog) |

DuckDB's own `delta` extension is a separate path from deltalake. It can write Delta Lake tables, which is covered in the decision guide below.

**Working rule:** Read each cell against the example or source it names. A cell with no example behind it is a claim to test on your own readers, not one to rely on.

## pyiceberg writes no version 3 table {#iceberg-v3-and-deletion-vectors}

The version 3 section of the Iceberg specification makes binary deletion vectors the row-level delete format. It says that "writers are not allowed to add new position delete files to v3 tables" ([Iceberg spec](https://iceberg.apache.org/spec/)). The same section adds row lineage for new rows, default column values, the variant type and nanosecond timestamps.

[Example: iceberg-format-versions](../examples/iceberg-format-versions.py)

pyiceberg at this pin writes format version 2. The example prints the format version of its table, which is {{out:iceberg-format-versions:format version}}. The delete removed {{out:iceberg-format-versions:rows deleted}} rows and left {{out:iceberg-format-versions:delete files after the delete}} delete files. The table has two snapshots: the first is an `{{out:iceberg-format-versions:snapshot 1 operation}}`, and the second, the delete, is an `{{out:iceberg-format-versions:snapshot 2 operation}}`. The same example asks for format version 3, and the create raises `{{out:iceberg-format-versions:format version 3 create}}`, so none of the version 3 features is available from this writer.

pyiceberg deletes by rewriting the data files it touches at these pins, although format version 2 also allows delete files ([chapter 5](#copy-on-write-and-merge-on-read)). Measure that rewrite on a copy of your own table before you choose.

| Feature | Iceberg v3 specification | pyiceberg at these pins |
| --- | --- | --- |
| Binary deletion vectors | Row-level delete format | Not written |
| Position delete files | Legacy, in format version 2 tables only | Not written |
| Row lineage, defaults, variant, nanosecond timestamps | Part of version 3 | Not available: no version 3 table |

**Working rule:** Write format version 2 with pyiceberg at these pins, and do not promise a version 3 feature to any reader. Choose version 3 only when the writer you use creates it and every reader in your set opens it, and check both on their own pins.

## Deletion vectors need a protocol upgrade {#delta-deletion-vectors}

The Delta Lake protocol, at the release tag its link names, says a table with deletion vectors needs reader version 3 and writer version 7, with the `deletionVectors` feature in both feature lists. It also says a reader must implement every feature in the reader list before it reads the table ([Delta protocol](https://raw.githubusercontent.com/delta-io/delta/v4.4.1/PROTOCOL.md)). A feature is a promise that the reader handles it, so a reader without it cannot read the table correctly.

[Example: feature-check](../examples/feature-check.py)

The example enables the deletion vector setting on a table of pageviews, deletes the rows below 180 views and prints what the table declares. The minimum reader version is {{out:feature-check:minimum reader version}}, and the minimum writer version is {{out:feature-check:minimum writer version}}. The reader features are `{{out:feature-check:reader features}}`. The example set only the deletion vector setting, so the library added the other two features on its own. The delete removed {{out:feature-check:rows deleted}} rows and rewrote {{out:feature-check:data files rewritten}} data file. The table holds {{out:feature-check:deletion vector files}} deletion vector files, and `QueryBuilder` reads {{out:feature-check:rows through QueryBuilder}} rows. So at this pin deltalake declares the feature but deletes by rewriting the file. The setting changes the protocol that readers must support, not the way deletes write. The book reads Delta tables only through `QueryBuilder`, as [chapter 8](#find-delta-by-path) shows.

Every feature in the list matters, not only the deletion vector. A reader that lacks `timestampNtz` or `variantType` has the same problem as one that lacks `deletionVectors`. Compare the printed feature lists with each reader's own list after every change.

So a deletion vector setting is a protocol change for every reader of the table, and the write succeeds before any reader is tested. Turn it on only when every reader in your set implements the `deletionVectors` feature. Otherwise keep copy-on-write.

A reader check, in the sense used here, opens the table through the reader's own interface, before the first production write. For the Delta Lake copy in the case, that check is a read through `QueryBuilder` and a read of the protocol, both in the feature-check example.

**Working rule:** Enable deletion vectors only after you have checked each reader's feature support on its own version. Read the table's protocol back after each change, and keep copy-on-write for any table that a reader outside your tested set will open.

## A checklist for a new table {#checklist-for-new-table}

Use this list before you create a table that more than one tool will open. Each item closes a risk that [the support matrix](#support-matrix-as-of-pins) and the [deletion vector section](#delta-deletion-vectors) showed.

1. List every reader and writer, with its version. A tool you leave off the list is a reader you have not tested, and it will be the one that fails.
2. For each feature the table needs, measure its support on each tool, as the examples in this chapter do. Measure the reader, not only the writer, because a successful write proves only that the writer can produce the files.
3. Set the format version and the protocol on purpose. Print them back, and read the table from each reader. Do not accept a default you did not choose.
4. Refuse any format version or feature that one reader in the set does not support. A table that one reader cannot open is an outage, however good the features are for the other readers.
5. Promise a rename only where the measured support allows it. Iceberg renames by field ID. deltalake has no rename at these pins, so plan a rewrite instead, and check the column names afterwards.
6. Keep a plain copy-on-write path for deletes unless every reader has been tested with deletion vectors. Copy-on-write costs rewritten files, and it opens everywhere.
7. Plan any upgrade in this order: readers first, then writers, then the protocol. Read the table after each step, and stop if a reader fails.
8. Run the measurements again on every pin change, and record the new versions in the table of readers.

**Working rule:** Keep the list with the table's design note, so the next person who adds a reader starts from the same record. A list kept up to date costs less than one rebuilt after a failed read.

## Pick a format from your readers {#choice-decision-guide}

Use this guide after you have measured your readers. The first column is the situation, not the format. It maps a measured reader set to a format, and it does not replace the measurements.

| If your situation is | Choose | Why |
| --- | --- | --- |
| pyiceberg and DuckDB read the table | Iceberg, format version 2 | pyiceberg reads it, and DuckDB queries its output |
| Several readers, and you need in-place renames | Iceberg | Renames keep the field ID, as [chapter 3](#rename-column-without-rewrite) shows |
| DuckDB is the only engine, and you want SQL changes | DuckLake | Deletes, merges and partition changes run as SQL |
| Delta tables already exist and deltalake reads them | Keep Delta Lake, with no rename promise | Change partitions by overwrite, and read back after each change |
| You want DuckDB to change a Delta table | Not at these pins | DuckDB can only append; use deltalake for updates and deletes |

DuckDB's `delta` extension can write Delta Lake tables. The DuckDB documentation for the pinned release, {{version:duckdb}}, says it can create tables and append to them, and it lists `UPDATE`, `DELETE`, `MERGE INTO`, `ALTER TABLE` and `DROP TABLE` as not supported ([DuckDB delta docs, Limitations](https://duckdb.org/docs/2.0/core_extensions/delta.html)). No example in this book runs the extension. The examples install nothing at run time and use each format's own library, so the reader sees the same library that writes the table, and nothing downloads while an example runs.

Read the table row by row. When two rows point in different directions, the readers decide. First choose the format that every reader in your measured set opens, then choose features that every reader supports.

Apply the guide to the running case. The dashboard and the pipeline both open the pageviews table with pyiceberg or DuckDB, which points to Iceberg format version 2. The archive job reads a Delta Lake copy, so the Delta row applies to that copy only, and the copy keeps its deletes as copy-on-write.

We recommend Iceberg format version 2 as the default for a table that several tools read, because pyiceberg writes it, DuckDB reads its output, and the examples measure both. Its costs at these pins: every delete rewrites the data files it touches, the table needs a catalog, and pyiceberg has no compaction or orphan-file cleanup call. We recommend DuckLake when DuckDB is the only engine, because the reader and the writer share one SQL surface. Its costs: readers are limited to DuckDB, the catalog version is tied to the DuckDB release, and small deletes are inlined unless you turn inlining off. Choose differently when the checks in this chapter show that a reader cannot do what you need.

**Working rule:** Pick the format that every reader in your measured set opens, then choose only the features that set supports. If no format meets every need, split the table or change the readers. Do not promise a feature to a reader that the measurements do not cover.

## What goes wrong in production {#choice-production-failures}

Most format failures in a shared lakehouse come from a tool the writer did not test, not from a bug in the format. Each failure in this section is one that an earlier section measured or cited, so you can check for it in your own tables.

**Choosing by vendor claims.** A support page describes one engine, often at a different version from yours. Use it to find what to test, not as the answer. Check the claim against your own pins.

**Testing one engine only.** A table can be written by one library and read by another that does not support the same feature. The write succeeds in one tool, and the failure appears when a second tool opens the table. Run the reader check for each tool in the set before the first write.

**A feature the reader does not implement.** The deletion vector setting adds a reader feature to the table, and the protocol requires readers to implement it. A reader outside your set may open the table and fail on it. Find out from the protocol record, which the feature-check example prints, and not from a later query error.

**A rename that does nothing.** At these pins, deltalake's `set_column_metadata` call returns without an error and bumps the version, but the column names stay the same. [Chapter 3](#rename-in-delta-lake) shows the trap. The check is to read the column names after the change.

**Upgrading in the wrong order.** A writer that moves ahead of its readers can produce a table that the readers cannot open, and the first sign is a failed read in another team's job. The order in the checklist keeps every reader able to open the table at each step.

**Judging a table by its write.** A write that succeeds shows that the writer can produce the files. It does not show that another tool can read them. The deletion vector check in this chapter writes, deletes and then counts the reader's features for exactly this reason.

**Counting rows for a DuckLake delete.** Under DuckLake's default inlining, a small delete of a few rows writes no delete file, and the same delete with inlining off writes one. A check that counts rows says the delete worked either way. Count delete files too, as the [DuckLake delete example](../examples/ducklake-delete-files.py) does.

**A catalog version that moves with DuckDB.** The DuckLake catalog at these pins reports version {{out:ducklake-catalog-version:ducklake catalog version}}, and the extension that opens it reports the build {{out:ducklake-catalog-version:ducklake extension version}}. The DuckLake documentation names the DuckDB release that version 1.1 requires ([DuckLake docs](https://ducklake.select/docs/stable/)). An older DuckDB may not open that catalog, so an upgrade can break a reader that did not change.

**Working rule:** Measure every reader on its own pinned version before you write, upgrade or rely on a vendor claim. When you upgrade a writer or a catalog, keep the old release in place until every reader has opened the result.

## Decisions this chapter made {#choice-decisions}

- We recommend Iceberg format version 2 with pyiceberg at these pins. We do not promise format version 3 to any reader, and we set no date for when that changes. The pin decides, and the measurement shows it.
- We state the matrix as measured at these pins, and each cell points to an example or a cited source.
- We keep deletion vectors out of tables that a reader outside the tested set will open.
- We do not promise a rename on a deltalake table at these pins.
- We count DuckLake delete files as well as rows.
- We treat an unmeasured feature as unsupported, because the failures in this chapter all came from a feature someone assumed.
- We state the DuckLake catalog version apart from the extension version. The catalog version decides which readers can open the catalog, and the extension version follows DuckDB.
- We chose a measured matrix over a vendor's support page, because the pinned versions decide what opens.
- The default of Iceberg format version 2 is a judgement, labelled as one. The measurements behind it are the examples, and they are the part to re-run.
- The book runs no DuckDB `delta` or `iceberg` extension. Its examples install nothing at run time and use each format's own library. The DuckDB `delta` extension can write Delta Lake tables, as its documentation describes.
