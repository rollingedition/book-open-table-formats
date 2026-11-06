---
chapter_id: catalogs
---

# Register tables in a catalog and query them from DuckDB

By the end of this chapter you can register an Iceberg table in a SQL catalog, attach a DuckLake catalog, open a Delta Lake table by its path, and run one DuckDB statement that joins a table from Iceberg with a table from DuckLake. You can also say what each catalog holds, and what happens to a table when its catalog and its data files disagree.

The earlier chapters opened each table by its own route: a catalog object for Iceberg, a file for DuckLake, a folder for Delta Lake. Every chapter uses the same table, `wiki.pageviews_hourly`, the hourly view counts of Wikimedia pages. [The first table](#create-catalog-and-table) registered it in an Iceberg SQL catalog under the name `local`. This chapter explains what that name does, and what each format uses in its place. It also adds a second table, `wiki.domains`: a four-row list of domain codes and the language each one is in. It lives in the DuckLake catalog, and [the join section](#join-tables-from-two-catalogs) joins it to the Iceberg pageviews.

## What a catalog holds {#what-a-catalog-holds}

A table format stores a table as data files plus metadata files. The metadata names the table's current state: its schema, its partition spec and, directly or through manifest files, the data files of its current snapshot or version. Each commit writes new metadata and moves the table to it. The catalog is the registry that maps a table's name to the place where its current metadata is. A reader that asks the catalog for `wiki.pageviews_hourly` gets a pointer to the metadata, and then reads the data files that the metadata lists.

The catalog holds pointers, not the rows. The Parquet files stay in the warehouse folder, and the catalog says where to start reading. Names have a second level as well: `wiki` is a namespace, and every Iceberg table name in this book is a namespace and a table name joined by a dot.

Choosing a catalog comes down to two questions: who writes the table, and what reads it. A table that one job writes and one engine reads can live in a file-based catalog and be found by its path. A table that several engines read needs a name each of them can resolve, which is what a SQL catalog or a service provides. We recommend one catalog for each table, and a record next to the table that names its catalog and its owner. A join across catalogs, as [the join section](#join-tables-from-two-catalogs) shows, works in one session. It does not make the two catalogs one.

The three formats in this book use three different catalogs:

| Format | What names the table | Where the current metadata is |
| --- | --- | --- |
| Iceberg | A SQL catalog object, here named `local` | The catalog's database, which points at the metadata file |
| DuckLake | The catalog file, attached by path | Inside the catalog file |
| Delta Lake | Nothing; the table's folder path | In the table's folder |

The Iceberg REST catalog is the other kind of catalog, a service instead of a file. Its [REST catalog page](https://iceberg.apache.org/rest-catalog-spec/) describes it as a common API, specified with OpenAPI, for interacting with any Iceberg catalog, so that clients need one implementation for all of them. The same page says the protocol uses change-based commits, so the service can resolve conflicts and retry on its side, and that the catalog service writes the root metadata. This chapter runs no REST catalog. Each one needs a running service, and the examples here run offline with no service. The book does not compare the products that implement the protocol.

A catalog can hold more than names. Classifications and retention rules are table metadata too. [*Data Governance as Code*](https://rollingedition.com/data-governance-as-code) covers them, and this book does not teach them.

**Working rule:** Before you write to a table, find out which catalog or path names it, and back up that catalog along with the data folder. Parquet files do not name a table. The catalog does, as [the failure section](#catalog-production-failures) shows.

## Register a table in a SQL catalog {#register-iceberg-in-sql-catalog}

The pyiceberg `SqlCatalog` keeps its tables in a database. You give it a `uri` for that database and a `warehouse` for the folder where it writes data files and metadata. This book uses a SQLite file for the database and a local folder for the warehouse. The [pyiceberg configuration page](https://py.iceberg.apache.org/configuration/) says the SQL catalog "requires a database for its backend", and that "SQLite is not built for concurrency, you should use this catalog for exploratory or development purposes". The examples here are development examples, so that limit does not change anything in them. A production catalog needs a database that several writers can use.

The next example opens the same catalog file a second time, with the same name, and reads the table through that second object. It then registers a second name for the table's metadata, and shows what a catalog object with a different name can see.

[Example: register-iceberg-table](../examples/register-iceberg-table.py)

The second object reads {{out:register-iceberg-table:rows read through the second object}} rows of `wiki.pageviews_hourly`, the three hours that [chapter 1](#land-three-hours) landed. pyiceberg stores each table under the catalog name you pass to `SqlCatalog`. An object with the same name finds the same tables.

The catalog and the warehouse folder are a pair. The catalog holds the locations of the metadata files, and those locations sit under the folder that the `warehouse` property names. A copy of the catalog file without the warehouse folder, or the folder without the file, gives you pointers to files that are not there. Back up both, and move both together.

Each process opens its own catalog object, and none shares the state of another. The example uses two objects in one process to show the effect that a second process would have.

A loaded table is a snapshot of the catalog as it was when it was loaded. The example keeps a table object from before a fourth hour is appended through the first object. That object still reads {{out:register-iceberg-table:rows in the loaded object after another append}} rows. A reload reads {{out:register-iceberg-table:rows after a reload}}. A process that loads a table at its start and reads it at its end reads the state from the start. It has to reload to see the commits made in between.

**Working rule:** Open a SQL catalog file with one name everywhere in your code, and reload a table before you read it when another writer may have committed since you loaded it. Keep SQLite for one writer at a time, and use a database built for concurrent writers before you run several.

### Register a table from its metadata file {#register-from-metadata-file}

`register_table(identifier, metadata_location)` adds a name for an existing metadata file. The [pyiceberg API page](https://py.iceberg.apache.org/api/) gives it as the way to register a table from its metadata, and says that passing `overwrite=True` replaces an entry that already exists. The example registers the table's metadata under `archive.pageviews_copy`, in a namespace it creates first. The archive name reads the same rows as the original table: {{out:register-iceberg-table:rows in archive copy}}. Both names point at the same metadata, and the registration writes no data files.

A registered name is a second route to the same table, not a second table. Commits through one name are the commits of both, because both read the same metadata. Treat a second name as a read route, and not as a backup.

**Working rule:** Keep one writer name for each table. Register a metadata file under a second name only for a read you will remove afterwards, and pass `overwrite=True` only when you mean to replace that entry.

## Attach a DuckLake catalog {#attach-ducklake-catalog}

DuckLake's catalog is a database file, and DuckDB attaches it with an `ATTACH` statement that starts with `ducklake:` and gives the file's path. The data path, the folder where DuckLake writes Parquet files, is stored in the catalog. The [DuckLake connecting page](https://ducklake.select/docs/stable/duckdb/usage/connecting) says the data storage location only has to be given when a new DuckLake is created, and that the location is loaded from the catalog database after that. The helper `connect_ducklake` loads DuckLake into the DuckDB session with downloads off, and `attach_ducklake` runs the `ATTACH` for a catalog file under a folder.

The next example creates `wiki.domains` in a fresh DuckLake catalog, detaches it, and attaches the file again with no data path. It then lists the tables in the `wiki` schema of the file.

[Example: attach-ducklake](../examples/attach-ducklake.py)

The four rows of `wiki.domains` are written, and {{out:attach-ducklake:rows after re-attach}} rows read back after the re-attach. The table list shows one table in the `wiki` schema, `domains`. The second attach needed no data path, because the file already held it. The listing comes from the catalog, not from the data folder. The catalog file holds the list of tables, and the data folder holds the bytes. Losing the first loses the names even when the second is intact, as [the failure section](#catalog-production-failures) shows.

The [same connecting page](https://ducklake.select/docs/stable/duckdb/usage/connecting) says that DuckLake inlines small inserts into the catalog database, with a default limit of 10 rows. The helper this chapter uses sets the limit to 0, so every insert in these examples becomes a Parquet file in the data folder. Without that setting, a small table lives in the catalog file, and a copy of the data folder would not hold its rows. That is one reason the catalog file needs a backup of its own.

**Working rule:** Back up a DuckLake catalog file with the data folder, and copy the file while no session has it attached. Give the data path when you create a catalog, and attach later with the catalog file alone.

## Find a table by path, with no catalog {#find-delta-by-path}

A Delta Lake table in this book is a folder. It has no catalog, and nothing in this book's setup names it apart from its path. The `deltalake` package opens the folder with `DeltaTable(path)`, and every read goes through that path. Delta counts versions from 0, so the three appends in the next example are versions 0, 1 and 2.

[Example: open-delta-by-path](../examples/open-delta-by-path.py)

The table's current version is {{out:open-delta-by-path:delta version}}, and it reads {{out:open-delta-by-path:rows at that version}} rows. Version 0 holds {{out:open-delta-by-path:rows at version 0}} rows, the first hour alone. The rows here come through `read_delta`, which reads through deltalake's `QueryBuilder` and returns an Arrow table. The [decisions](#catalog-decisions) at the end of this chapter say why the book reads Delta tables that way.

The path is the whole name, so a typo in it is a missing table. The example opens a folder that does not exist, and `DeltaTable` raises `TableNotFoundError`. The error names the path it tried, but only once the wrong path has been used, so the check belongs in the code that builds the path.

A path is a good name for a table that one job writes and a few jobs read. It needs no service and no setup. It has costs, though. A path does not say which namespace a table is in, so two tables with the same folder name in different places look identical to a reader. An engine that wants to find the table by name has nothing to look up. The Delta tables in this book are small and local, and the path is enough for them. A shared lake read by several engines needs a catalog for its Delta Lake tables, and this book does not teach one.

**Working rule:** Keep each Delta table's path in one place your code reads, such as a configuration value, and open the table once at the start of a job to confirm the path. A path that does not exist is an error, not an empty table.

## Join tables from two catalogs {#join-tables-from-two-catalogs}

DuckDB can query an Arrow table that a library hands it, and a DuckLake table that is attached to the same session, in one statement. The Iceberg table leaves pyiceberg as an Arrow table through `scan_arrow`. DuckDB registers that table under a name, and the join reads `wiki.domains` through the attached catalog. The book does not load DuckDB's `iceberg` extension for this, so the Iceberg side is the Arrow table and nothing else.

[Example: join-pageviews-and-domains](../examples/join-pageviews-and-domains.py)

The join matches {{out:join-pageviews-and-domains:joined rows}} pageview rows to a language. The count of rows with no language is {{out:join-pageviews-and-domains:rows with no language}}, so every domain code in the pageviews is in the four-row dimension. The top three rows by views are the same page, `Main_Page` on the English domain.

The join key is `domain_code`, a string on both sides. A join on a key that differs in case or in trailing spaces matches nothing: the joined-row count would drop to 0, and the count of rows with no language would equal the table's row count. Check the key on both sides, in the same session, before you trust a join.

The query works because both sides meet in one DuckDB session. It does not make the two tables one. Each keeps its own commits, and the Arrow table is a snapshot of the Iceberg table at the moment `scan_arrow` ran. A join over a table that another writer is changing reads whatever state was current at that moment.

We recommend a cross-catalog join for lookups like this one: a small dimension table that stays in its own catalog, read by a query over a large table. When one table appears in almost every query that reads another, copy it into the catalog those queries use. Chapter 9 copies a table between formats and [checks each copy](#check-copy-with-digest), and that check is the step to take before you move a table anywhere.

**Working rule:** Join across catalogs in one session only when the side you read as Arrow fits in memory. Choose its state before you read it, and count the rows that find no match, which should be zero.

## What goes wrong in production {#catalog-production-failures}

The symptoms are usually small: a count that stops moving, a namespace that is missing, or a read error on a table that was read before. Each case below starts from one of them.

Three failures in this chapter are shown by an example, so you can recognize them when they happen. A fourth is named and not shown.

**The wrong catalog name.** The last line of [register-iceberg-table](#register-iceberg-in-sql-catalog) opens the same file under the name `other`. Listing the namespace then fails with `NoSuchNamespaceError: Namespace does not exist: wiki`. The table is still in the file. The SQL catalog scopes its tables by catalog name, so the second name sees an empty catalog. The message names the namespace, not the name, so the error looks like a missing schema. A job that builds its catalog name from configuration can fail this way after a small edit. You notice it at the first read, not at the write, so log the catalog name at the start of the job.

**The stale pointer.** A process that loads a table and reads it much later reads the state from when it loaded. [register-iceberg-table](#register-iceberg-in-sql-catalog) showed {{out:register-iceberg-table:rows in the loaded object after another append}} rows where a reload shows {{out:register-iceberg-table:rows after a reload}}. Nothing fails. The job runs and reports fewer rows than the table holds. You notice it when two jobs disagree about a count, so keep the load next to the read.

**The catalog file replaced by an empty one.** The next example attaches a fresh, empty catalog file to the data folder of a live catalog, then runs `ducklake_delete_orphaned_files` with `cleanup_all => true`. The [DuckLake cleanup page](https://ducklake.select/docs/stable/duckdb/maintenance/cleanup_of_files) shows `cleanup_all` deleting all orphaned files, and `dry_run` listing them instead. It defines an orphan as a file that the catalog you run it against does not track. A fresh catalog tracks nothing, so every Parquet file in the folder is an orphan to it.

[Example: ducklake-orphan-cleanup-hazard](../examples/ducklake-orphan-cleanup-hazard.py)

The example runs in a temporary folder, and prints counts only. The live catalog's data is in {{out:ducklake-orphan-cleanup-hazard:parquet files, live catalog}} Parquet file. The dry run with `cleanup_all` lists {{out:ducklake-orphan-cleanup-hazard:files the dry run lists}} file, which is the file the real run deletes. After the real run, the folder holds {{out:ducklake-orphan-cleanup-hazard:parquet files after cleanup_all}} Parquet files. The live catalog still names the table. It answers `count(*)` with {{out:ducklake-orphan-cleanup-hazard:count through the live catalog}}, from the catalog's statistics, and reading its columns fails with `{{out:ducklake-orphan-cleanup-hazard:reading the live catalog}}`. A count check would pass here, so it does not show missing files.

The safe order starts where the example did not. Back up the catalog file first. Run the cleanup with `dry_run => true` against the catalog that owns the data folder, and read the list. Run it for real only when every listed file is one you know is orphaned. Never run `cleanup_all => true` against a catalog you have not confirmed.

**Two writers on one SQLite catalog.** The pyiceberg documentation says SQLite is not built for concurrency. This chapter does not show what two writers do to the SQL catalog, so the book does not claim that they are safe. The stale-pointer example is the nearest case it does show: an object loaded before a commit reads the older state, and no error is raised. A writer that holds an old object does not write against that old state. At the pinned version of pyiceberg, an append through such an object is retried against the new metadata and commits on top of the newer state. The book makes no claim for other operations, or for two processes on one SQLite catalog. The reload rule in [the SQL catalog section](#register-iceberg-in-sql-catalog) applies to reads.

**Working rule:** Run an orphan cleanup with `dry_run => true` first, against the catalog that owns the data folder, and read the list before any real run. If the catalog file is missing or empty, restore it before you run anything that deletes files.

## Decisions this chapter made {#catalog-decisions}

- **Iceberg is registered in a pyiceberg SQL catalog on SQLite, with a local warehouse.** The book has one catalog name, `local`, and every example uses it. The pyiceberg documentation describes SQLite for development, and the examples are development examples.
- **Iceberg tables reach DuckDB as Arrow tables.** The book does not use DuckDB's `iceberg` extension, so the examples read Iceberg data through `scan_arrow`.
- **Delta Lake tables are found by path, and the book teaches no Delta catalog.** The examples open a folder, and a missing folder is an error.
- **Delta tables are read through `QueryBuilder` only.** This is a measurement from the book's build, not a primary source. At the pinned versions, the other Arrow conversion methods aborted the process at exit in about 17 of 20 runs. The `QueryBuilder` path aborted in none of 24 runs, including reads of earlier versions.
- **DuckLake is loaded by `connect_ducklake`, with downloads off.** Every example runs offline on the pinned DuckDB.
- **The cleanup hazard runs in a temporary folder and prints counts only.** The book shows the failure so the order of the safe steps is clear. It does not print file names, which change on every run.
- **Each example prints counts and labelled values only.** The examples name no file, path or ID in their output, so the output reads the same on every run and on every machine.
- **REST catalogs have no runnable example.** They need a network, so the chapter describes the protocol from its page and makes no claim about any product.
