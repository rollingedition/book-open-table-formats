---
chapter_id: evolving-schema
---

# Add and rename columns without rewriting files

By the end of this chapter you can add a column to a table that already holds data, rename a column in Iceberg without rewriting its data files, and say why a rename in Delta Lake is a rewrite of the whole table. The versions this book pins are pyiceberg {{version:pyiceberg}} and deltalake {{version:deltalake}}.

The running case is `wiki.pageviews_hourly`, the table of hourly view counts for six hours of Wikimedia pageviews that [the first chapter](#first-table) loads. Two requests reach it after the first three hours have been written. The first asks for a lineage column: each row should record which hourly load wrote it, so that a bad load can be traced back to its rows. The table needs a column for that, and the rows it already holds were written before the column existed. The second is a naming fix: the dashboards call the title column `article_title`, while the table calls it `page_title`, the name the dump uses. Both are schema changes on a table that already has data. For each one, the question is what happens to the files that were written before the change.

A schema change is a metadata change in principle. The data files stay where they are, and the format records the added or renamed columns in its metadata. Whether the old files still read correctly depends on how the format finds a column inside a file. A format that finds columns by name has trouble when a name changes. A format that finds them by something that does not change does not. This chapter shows which of the two each format is, what each change costs, and which change the pinned Delta Lake library cannot make.

## Columns are tracked by ID {#columns-tracked-by-id}

A data file holds values for some columns, and the table's schema says which column each value belongs to. The simple way to match them is by name. That works until a name changes. The schema then names a column the old file does not have under that name, and the reader has to guess what the old values belong to.

A reader that matches by name and finds no column under the old name has two bad options: fail the query, or fill the column with null without saying so. The second is the quieter failure, and a dashboard can show it for weeks before anyone looks. We count the quiet null as the worse failure, which is a judgement, not a measured result. Matching by identity avoids both, because the column keeps its identity whatever its name is.

Iceberg gives every column an integer field ID when the column is created, and it matches columns by that ID. The name is a label that can change. The Iceberg specification says so directly: renaming a field "must change the name, but not the field ID" [1]. When a reader opens an old file, it matches each column by field ID. If the file holds no value for a field, the spec's projection rule first checks two cases that do not apply here, an identity-partition value and a name mapping. It then returns the field's `initial-default` when one is set, and otherwise returns null: "Return `null` in all other cases" [1]. The spec defines `initial-default` for a column added with a default. This chapter does not set one, so an added column reads as null in every old row.

You can see the IDs yourself. The schema of an Iceberg table lists each field with its ID, and [the example in the rename section](#rename-column-without-rewrite) prints the ID of one column before and after a rename. The ID is the value to check when you want to know whether a rename kept the column's identity.

Delta Lake starts from a different place. Its table schema names each column, and a reader matches by name. The Delta Lake protocol defines an optional column mapping feature that separates the two: every column gets a physical name stored in the files, and the name readers use is a display name that can change without touching the files. The protocol says column mapping supports "the renaming and dropping of columns without having to rewrite all the data" [2]. Whether a given library release does that is a separate question. [The section on Delta Lake](#rename-in-delta-lake) answers it for the pinned release.

**Working rule:** Expect an Iceberg column to keep its data through a rename, because the reader matches by field ID. Expect a Delta Lake column to match by name unless column mapping is on. Test a rename on a copy of the table before you rely on it, and read the column names back afterwards.

## Add load_batch after three hours {#add-response-bytes}

The first change is the one the case needs first: a column that does not exist in the files already written. In a pipeline, that happens on an ordinary day. The pipeline starts stamping each load with its number, and the table already holds rows that predate the stamp. The example starts from the first three hours, each written as its own commit, so the table holds three snapshots and three data files. An Iceberg snapshot is the table's state after a commit that changes data. Each append in this chapter adds one, and a schema change adds none.

In Iceberg, you add a column inside a schema update. The call takes the name and the type, and the table assigns the field ID:

```python
with table.update_schema() as update:
    update.add_column("load_batch", IntegerType())
```

The example runs the three early commits, adds the column, then appends hours 03 to 05. Each of those loads sets `load_batch` to its hour number, so its rows carry the value. It then counts rows by hour of the day: hours 00 to 02 were written before the add, and hours 03 to 05 after it. Each hour is one write, so the split is also the split by load. Each count is printed with a label, so the text can quote it.

[Example: add-column-to-existing-files](../examples/add-column-to-existing-files.py)

Before the add, the table has {{out:add-column-to-existing-files:iceberg rows written before the add}} rows, and {{out:add-column-to-existing-files:iceberg rows written before the add with load_batch null}} of them read null for `load_batch`. After the add and the three later appends, the table has {{out:add-column-to-existing-files:iceberg rows in the table}} rows. The {{out:add-column-to-existing-files:iceberg rows written after the add}} rows written after the add have {{out:add-column-to-existing-files:iceberg rows written after the add with load_batch null}} nulls in `load_batch`. The snapshot count stays at {{out:add-column-to-existing-files:iceberg snapshots before the add}} across the schema change, and the table keeps {{out:add-column-to-existing-files:iceberg data files before the add}} data files. The change touched the metadata and no data file. The two halves of the example hold the same rows and the same nulls, so a disagreement between them would point at a reader, not at the data.

Delta Lake adds a column through its `alter` object. The call takes a field, and the table's version goes up by one:

```python
DeltaTable(path).alter.add_columns([Field("load_batch", PrimitiveType("integer"))])
```

The same example does the Delta half. It writes the three early hours as three appends, adds the column, and appends hours 03 to 05. Delta counts versions from 0, so the first write is version 0. Every version number this chapter prints is Delta's real one. The version moves from {{out:add-column-to-existing-files:delta version before the add}} to {{out:add-column-to-existing-files:delta version after the add}}, and the table keeps {{out:add-column-to-existing-files:delta data files before the add}} data files. Before the add there are {{out:add-column-to-existing-files:delta rows written before the add}} rows, all null for `load_batch`. After the add and the three later appends the table has {{out:add-column-to-existing-files:delta rows in the table}} rows, and the {{out:add-column-to-existing-files:delta rows written after the add}} rows written after the add have {{out:add-column-to-existing-files:delta rows written after the add with load_batch null}} nulls. The example's table has no column mapping.

Both formats give the same result for this case. The old rows stay as they were written, and the column reads as null for them. The writer did not have to touch a single old file.

**Working rule:** Add a column with the format's own call, and never by rewriting the files. Expect the rows written before the add to read null for the column, and check that they do.

### Old files read as null {#old-files-read-as-null}

A null in an added column can look like missing data, and a reader who sees it may assume the old files are wrong. They are not. The old files hold the values the pipeline wrote, and the column they lack is filled on read. The rule the reader follows is the one [the columns section](#columns-tracked-by-id) quotes from the spec: with no `initial-default`, no value for the field means null.

That makes the null count a useful check. The example counts nulls by hour of the day, and each hour is one write, so the counts show the pattern to expect: every row written before the add is null, and no row written after it is. In Iceberg the output reads {{out:add-column-to-existing-files:iceberg rows written before the add with load_batch null}} nulls among {{out:add-column-to-existing-files:iceberg rows written before the add}} rows. In Delta Lake it reads {{out:add-column-to-existing-files:delta rows written before the add with load_batch null}} among {{out:add-column-to-existing-files:delta rows written before the add}}. Both match the rule.

If a count falls outside that pattern, a writer sent a different set of columns, or sent the column with nulls, and the cause is in the pipeline, not in the format. Look at the writer for the rows that break the pattern before you query the column, because a query over a column that is partly null will return an answer that looks reasonable.

**Working rule:** After an add, count nulls in the added column by hour of the day, where each hour is one write. Rows written before the add should be null, and rows written after it should not be. Any other pattern means a writer sent a different set of columns, or sent nulls in the column, so find that writer before you trust the column.

## Rename a column by its ID without rewriting files {#rename-column-without-rewrite}

With the field ID in place, a rename is one call in Iceberg. The call changes the name the schema reports, and keeps the ID:

```python
with table.update_schema() as update:
    update.rename_column("page_title", "article_title")
```

The example writes three hours, reads the field ID of `page_title`, renames the column, and reads the ID again. It counts the data files before and after the call, and it reads the table through DuckDB with the new name in the query.

[Example: rename-by-id](../examples/rename-by-id.py)

The field ID of `page_title` is {{out:rename-by-id:page_title field ID before the rename}} before the rename, and the field ID of `article_title` is {{out:rename-by-id:article_title field ID after the rename}} after it. The table keeps {{out:rename-by-id:data files before the rename}} data files on both sides of the call, so no file was rewritten. The DuckDB query counts {{out:rename-by-id:article_title not null}} non-null `article_title` values, out of {{out:rename-by-id:rows}} rows. Every row counts, including the rows that were written under `page_title`, because the reader matches the files by ID and reads them under `article_title`.

The dashboards need the name, and the table does not have to be rewritten to give it to them. The data stays as it was written, and the next read sees the renamed column. The example also reads the first snapshot's columns, and they are `hour_utc`, `domain_code`, `page_title`, `views` and `response_bytes`. A snapshot reads with the schema it was written with, so an older snapshot still reports the old name. [Chapter 2](#snapshots-and-time-travel) covers reading older snapshots in general, and the point here is narrower: a rename does not reach back into the history.

The cost comparison is what makes this the right call for Iceberg. A rename that keeps the ID touches the metadata and nothing else, so its cost does not grow with the size of the table. The same rename done by copying the table costs one read and one write of every row. [The section on Delta Lake](#rename-in-delta-lake) shows what that copy costs in that format.

**Working rule:** Rename an Iceberg column when its name is wrong, and check the field ID before and after the call. The field ID must be the same, and the data file count must not change. Tell every reader of the table about the rename, because the current schema has only `article_title`, and an older snapshot still has `page_title`.

### Delta Lake has no rename at this pin {#rename-in-delta-lake}

The same rename in Delta Lake has no call at this pin. The Delta Lake table alteration object has no rename method at deltalake {{version:deltalake}}, and its methods are listed in the package [3]. The one that looks like a rename is `set_column_metadata`, because it takes a dictionary of field metadata, and nothing stops you from passing a `name` key. The call returns without error when you pass a `name`. It renames nothing on any table, with or without column mapping, and that is the trap this section shows.

The trap example creates a table with column mapping on, which is the protocol's route to a rename, and then sends the call:

```python
DeltaTable(path).alter.set_column_metadata("page_title", {"name": "article_title"})
```

[Example: delta-rename-trap](../examples/delta-rename-trap.py)

The call returns, and the table's version moves from {{out:delta-rename-trap:version before the rename}} to {{out:delta-rename-trap:version after the rename}}, so the library wrote a commit. The `name` key is in the field's metadata. The column is still `page_title`: the names read back through the library are `hour_utc`, `domain_code`, `page_title`, `views` and `response_bytes`. The DuckDB query that asks for `article_title` fails with a `BinderException`, and the error lists `page_title` among the columns it could bind.

The trap is that the call looks like the rename and succeeds at the call. Nothing in the return value says the name is unchanged. Only a read of the names shows the truth.

Column mapping gives no rename at this pin. The protocol's column mapping section describes a rename [2], but deltalake {{version:deltalake}} has no call that performs one. The example creates the table with `delta.columnMapping.mode` set to `name`, and the call still changes no name. That shows what `set_column_metadata` does on a mapped table, and nothing about a rename with mapping, because the library has no call that makes one.

[Example: delta-rename-by-rewrite](../examples/delta-rename-by-rewrite.py)

A rewrite is the way to get the name at this pin. The second example reads the whole table through the library, renames the column in memory, and writes the result back with `mode="overwrite"` and `schema_mode="overwrite"`, which replace the data and the schema together. The table had {{out:delta-rename-by-rewrite:data files before the rewrite}} data files before the rewrite and has {{out:delta-rename-by-rewrite:data files after the rewrite}} after it. The rewrite makes version {{out:delta-rename-by-rewrite:version after the rewrite}}, which reads `article_title`, and {{out:delta-rename-by-rewrite:rows after the rewrite}} rows are in it.

The rewrite costs the size of the table, because every row is read and written again. The example's table is small, and the file count falls from {{out:delta-rename-by-rewrite:data files before the rewrite}} to {{out:delta-rename-by-rewrite:data files after the rewrite}}. A production table that holds millions of rows pays the same per-row cost, and a team should plan the rewrite as a job of its own. The version before the rewrite, {{out:delta-rename-by-rewrite:version before the rewrite}}, still reads `page_title`, which the example also prints. The rewrite therefore changes the current version only. Older versions keep the names they were written with. Before you run a rewrite, list the readers that ask for the column by name. The switch happens in one commit, so every reader of the current version moves at the same moment, while a job that pins an older version keeps the old name. Move those jobs with the readers, and record the version number the rewrite replaced until they have moved.

**Working rule:** Choose Delta Lake column names when the table is created, because at this pin a later rename is a rewrite of the whole table. Do not use `set_column_metadata` to rename a column: it changes the version and the metadata, and the column keeps its name. After any schema change, read the column names back through the library, as the trap example does, and check them before a query depends on them.

## What goes wrong in production {#schema-production-failures}

Schema changes fail quietly more often than they fail loudly. Four failures in this chapter are worth knowing, and each one has a way to notice it.

The first is the no-op rename. A `set_column_metadata` call succeeds, a commit appears in the history, and the column keeps its old name. The failure shows on the first query that uses `article_title`: the DuckDB query fails with a `BinderException`. A scheduled job that runs such a query fails on its first run after the change, so the error arrives close to its cause. Read the names back after any change, as the trap example does, before any query runs.

The second is the stale name in history. A rewrite gives the current version the name `article_title`, but the earlier versions keep the names they were written with, and the rewrite example shows it: version {{out:delta-rename-by-rewrite:version before the rewrite}} still reads `page_title`. An Iceberg snapshot does the same: the first snapshot still reports `page_title`. A query that reads an earlier version or snapshot by `article_title` finds no such column. Reads by version or snapshot number need the names that were current at that point, so a job that pins a version number also pins its schema.

The third is the null that looks like missing data. An added column reads as null in every row written before the add. A null count on that column is correct, and it is a fault only when it falls outside the before-and-after pattern the add example shows. Count nulls by hour, as the add example does, where each hour is one write, and investigate any row that breaks the pattern.

The fourth is the schema change that does not show in the snapshot list. The Iceberg add and rename both left the snapshot count where it was, and the file count too. A tool that looks for schema changes by counting snapshots will miss them. Compare the schema itself, by field ID and name, when you need to know what changed.

**Working rule:** After any schema change, do three things before a downstream query runs: read the column names back through the library, count nulls in each added column by hour, where each hour is one write, and compare the schema, not the snapshot count, with what the change was meant to do.

## Decisions this chapter made {#schema-decisions}

The table in this section sets the two formats side by side for the changes in this chapter. Each cell comes from an example in this chapter.

| Change | Iceberg | Delta Lake |
| --- | --- | --- |
| Add a column | Works. Old rows read null, and the snapshot count stays at {{out:add-column-to-existing-files:iceberg snapshots before the add}} | Works. Old rows read null, and the version goes up by one |
| Rename a column | Works. The field ID stays the same, and no data file is rewritten | No rename call. `set_column_metadata` commits and changes no name; a rewrite gives the name |
| Read an older snapshot or version | Shows the older name | Shows the older name |

- Add a column to a table that already holds data, in both formats. Old rows read it as null, and the add example shows the counts by hour that confirm the pattern. We recommend this over any copy of the table, because it writes no old file.
- Rename an Iceberg column with `rename_column`, and check that its field ID is the same before and after. We recommend it for any Iceberg column whose name is wrong, because its cost does not depend on the table's size.
- Choose Delta Lake column names when the table is created. At the pinned release, a rename there is a rewrite of the whole table, and that is the cost to plan for when a name is wrong.
- Do not rename a Delta Lake column with `set_column_metadata`. The call succeeds and changes nothing about the name.
- Do not count on column mapping for a rename at this pin. The library has no call that performs one, so turning mapping on gains no rename.
- Read the column names back after every schema change, in both formats, before a query depends on them.

### Sources {#schema-sources}

1. Apache Iceberg Table Spec, Schema Evolution (Column Projection), https://iceberg.apache.org/spec/, read Nov 6, 2026. Quoted: "Renaming an existing field must change the name, but not the field ID." and "Return `null` in all other cases."
2. Delta Transaction Log Protocol, Column Mapping section, tag v4.4.1, https://raw.githubusercontent.com/delta-io/delta/v4.4.1/PROTOCOL.md, read Nov 6, 2026. Quoted: "the renaming and dropping of columns without having to rewrite all the data."
3. deltalake {{version:deltalake}}, the methods of `DeltaTable.alter` in the installed package: `add_columns`, `add_constraint`, `add_feature`, `drop_column_not_null`, `drop_constraint`, `set_column_metadata`, `set_table_description`, `set_table_name` and `set_table_properties`. None renames a column. To list them, run `dir(DeltaTable(path).alter)`.
