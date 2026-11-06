"""Shared helpers for the examples: temporary warehouses, DuckLake loading,
the pageviews data, stable labels and sorted printing.

Nothing here prints a path, an ID, a timestamp or a file name. Labels replace
IDs, and errors go through safe_error.
"""

import contextlib
import datetime
import gzip
import hashlib
import os
import re
import shutil
import tempfile
import time
import tomllib
import urllib.parse
import warnings
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
from deltalake import DeltaTable, write_deltalake
from deltalake.query import QueryBuilder
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

os.environ["TZ"] = "UTC"
time.tzset()
warnings.simplefilter("error", DeprecationWarning)

NAMESPACE = "wiki"
TABLE = "pageviews_hourly"
DOMAINS = "domains"
ICEBERG_ID = f"{NAMESPACE}.{TABLE}"
DUCKLAKE_ALIAS = "lake"
KEY = ("hour_utc", "domain_code", "page_title")
CORRECTED_TITLES = 20

PAGEVIEWS_CSV = "pageviews-2024-06-03.csv"
DOMAINS_CSV = "domains.csv"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
VENDOR_DIR = Path(__file__).resolve().parent.parent / "vendor" / "ducklake"
VENDORED_GZ = VENDOR_DIR / "ducklake.duckdb_extension.gz"
SOURCE_TOML = VENDOR_DIR / "SOURCE.toml"

PAGEVIEWS_SCHEMA = pa.schema([
    ("hour_utc", pa.timestamp("us")),
    ("domain_code", pa.string()),
    ("page_title", pa.string()),
    ("views", pa.int64()),
    ("response_bytes", pa.int64()),
])
DOMAINS_SCHEMA = pa.schema([
    ("domain_code", pa.string()),
    ("language", pa.string()),
])


@dataclass
class Warehouse:
    """A temporary warehouse: its root, an Iceberg SQL catalog and a DuckDB connection."""

    root: Path
    catalog: SqlCatalog
    con: duckdb.DuckDBPyConnection


@contextlib.contextmanager
def new_warehouse():
    """Yield a Warehouse in a fresh temporary directory, removed on exit.

    The DuckDB connection has extension downloads and autoloading off, one
    thread and the UTC time zone. Load DuckLake with connect_ducklake.
    """
    with tempfile.TemporaryDirectory(prefix="warehouse-") as tmp:
        root = Path(tmp)
        catalog = SqlCatalog(
            "local",
            uri=f"sqlite:///{root}/catalog.db",
            warehouse=f"file://{root}/warehouse",
        )
        catalog.create_namespace(NAMESPACE)
        con = duckdb.connect()
        try:
            con.execute("SET autoinstall_known_extensions = false")
            con.execute("SET autoload_known_extensions = false")
            con.execute("SET threads = 1")
            con.execute("SET TimeZone = 'UTC'")
            yield Warehouse(root=root, catalog=catalog, con=con)
        finally:
            con.close()


def open_catalog(wh, *, name="local"):
    """Return a SqlCatalog on the warehouse's catalog file and folder. Objects with the same name see the same tables."""
    return SqlCatalog(name, uri=f"sqlite:///{wh.root}/catalog.db",
                      warehouse=f"file://{wh.root}/warehouse")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def connect_ducklake(con=None):
    """Load DuckLake into a DuckDB connection and return the connection.

    With the vendored official file present, the .gz is decompressed into a
    temporary directory, both hashes are checked against SOURCE.toml, and the
    extension is LOADed by path with downloads off. Without it, the reader's
    path runs: INSTALL ducklake, then LOAD. allow_unsigned_extensions is never
    set; DuckDB checks the official signature when it loads.
    """
    if con is None:
        con = duckdb.connect()
    if not VENDORED_GZ.exists():
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")
        return con

    source = tomllib.loads(SOURCE_TOML.read_text(encoding="utf-8"))
    if _sha256_file(VENDORED_GZ) != source["sha256_gz"]:
        raise RuntimeError("vendored DuckLake archive does not match SOURCE.toml")
    with tempfile.TemporaryDirectory(prefix="ducklake-") as tmp:
        extension = Path(tmp) / "ducklake.duckdb_extension"
        with gzip.open(VENDORED_GZ, "rb") as source_file, extension.open("wb") as target:
            shutil.copyfileobj(source_file, target)
        if _sha256_file(extension) != source["sha256"]:
            raise RuntimeError("decompressed DuckLake extension does not match SOURCE.toml")
        con.execute("SET autoinstall_known_extensions = false")
        con.execute(f"LOAD '{extension}'")
    return con


def create_iceberg_table(wh, partition_spec=UNPARTITIONED_PARTITION_SPEC, identifier=ICEBERG_ID, properties=None):
    """Create the table (wiki.pageviews_hourly unless identifier is given) and return it. Unpartitioned unless partition_spec is given; properties go to the table, such as format-version."""
    schema = Schema(
        NestedField(1, "hour_utc", TimestampType(), required=False),
        NestedField(2, "domain_code", StringType(), required=False),
        NestedField(3, "page_title", StringType(), required=False),
        NestedField(4, "views", LongType(), required=False),
        NestedField(5, "response_bytes", LongType(), required=False),
    )
    return wh.catalog.create_table(identifier, schema=schema, partition_spec=partition_spec, properties=properties or {})


def attach_ducklake(con, alias, root, inlining_row_limit=None):
    """Attach a DuckLake catalog file and its data folder under root, named alias.

    inlining_row_limit sets DATA_INLINING_ROW_LIMIT; None keeps DuckLake's default.
    """
    root = Path(root)
    options = f"DATA_PATH '{root}/{alias}_data/'"
    if inlining_row_limit is not None:
        options += f", DATA_INLINING_ROW_LIMIT {int(inlining_row_limit)}"
    con.execute(f"ATTACH 'ducklake:{root}/{alias}.ducklake' AS {alias} ({options})")


def create_ducklake_table(con, root, inlining_row_limit=0):
    """Attach the DuckLake catalog under root, then create wiki.pageviews_hourly.

    Inlining is off unless inlining_row_limit is given; None keeps DuckLake's default.
    """
    attach_ducklake(con, DUCKLAKE_ALIAS, root, inlining_row_limit=inlining_row_limit)
    con.execute(f"CREATE SCHEMA {DUCKLAKE_ALIAS}.{NAMESPACE}")
    con.execute(
        f"CREATE TABLE {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE} ("
        "hour_utc TIMESTAMP, domain_code VARCHAR, page_title VARCHAR, "
        "views BIGINT, response_bytes BIGINT)"
    )


def create_ducklake_domains(con, root):
    """Attach the DuckLake catalog under root with inlining off, then create and fill wiki.domains."""
    attach_ducklake(con, DUCKLAKE_ALIAS, root, inlining_row_limit=0)
    con.execute(f"CREATE SCHEMA {DUCKLAKE_ALIAS}.{NAMESPACE}")
    con.execute(f"CREATE TABLE {DUCKLAKE_ALIAS}.{NAMESPACE}.{DOMAINS} (domain_code VARCHAR, language VARCHAR)")
    con.register("domains_arrow", read_domains())
    con.execute(f"INSERT INTO {DUCKLAKE_ALIAS}.{NAMESPACE}.{DOMAINS} SELECT * FROM domains_arrow")
    con.unregister("domains_arrow")


def load_ducklake_with_correction(con):
    """Insert hours 0 to 2 into the DuckLake table, delete hour 02, then insert the corrected hour 02."""
    lake = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
    for hour in (0, 1, 2):
        con.register("hour", read_cut_hour(hour))
        con.execute(f"INSERT INTO {lake} SELECT * FROM hour")
        con.unregister("hour")
    con.execute(f"DELETE FROM {lake} WHERE hour_utc = TIMESTAMP '2024-06-03 02:00:00'")
    con.register("fixed", hour_corrections(read_cut_hour(2)))
    con.execute(f"INSERT INTO {lake} SELECT * FROM fixed")


def load_three_formats(wh, hours):
    """Load each hour into the Iceberg, Delta Lake and DuckLake tables, one commit per hour in each format.

    Returns the Iceberg table, the Delta table's path, and the DuckDB connection with DuckLake attached.
    """
    iceberg = create_iceberg_table(wh)
    delta_path = str(wh.root / "delta" / TABLE)
    lake = connect_ducklake(wh.con)
    create_ducklake_table(lake, wh.root)
    for hour in hours:
        rows = read_cut_hour(hour)
        iceberg.append(rows)
        write_deltalake(delta_path, rows, mode="append")
        lake.register("hour", rows)
        lake.execute(f"INSERT INTO {DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE} SELECT * FROM hour")
        lake.unregister("hour")
    return iceberg, delta_path, lake


def format_report(wh, iceberg, delta_path, lake):
    """Read the three tables back. Return {format: (rows, views, data files)}, sums and counts from each format."""
    summary = "SELECT count(*), sum(views) FROM pageviews"
    scans = {"iceberg": scan_arrow(iceberg), "delta": read_delta(delta_path)}
    report = {}
    for name, arrow in scans.items():
        rows, views = sql_rows(wh.con, summary, pageviews=arrow)[0]
        files = iceberg_file_count(iceberg) if name == "iceberg" else delta_file_count(DeltaTable(delta_path))
        report[name] = (rows, views, files)
    lake_table = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
    rows, views = lake.execute(f"SELECT count(*), sum(views) FROM {lake_table}").fetchall()[0]
    report["ducklake"] = (rows, views, ducklake_file_count(lake, DUCKLAKE_ALIAS, TABLE))
    return report


def _timestamp(hour: int) -> pa.Scalar:
    return pa.scalar(datetime.datetime(2024, 6, 3, hour), type=pa.timestamp("us"))


def hour_filter(hour):
    """Return an Iceberg row filter for one hour of the case's day (2024-06-03, UTC)."""
    start = datetime.datetime(2024, 6, 3, hour)
    return And(GreaterThanOrEqual("hour_utc", start), LessThan("hour_utc", start + datetime.timedelta(hours=1)))


def read_cut_hour(hour):
    """Return the pageviews rows for one hour of the cut (0 to 5), typed.

    hour_utc is a timestamp without a zone, in UTC. Counts are int64.
    """
    if not 0 <= hour <= 5:
        raise ValueError("hour must be 0 to 5")
    table = pcsv.read_csv(
        DATA_DIR / PAGEVIEWS_CSV,
        convert_options=pcsv.ConvertOptions(
            column_types={name: kind for name, kind in zip(PAGEVIEWS_SCHEMA.names, PAGEVIEWS_SCHEMA.types)},
        ),
    )
    mask = pc.equal(table["hour_utc"], _timestamp(hour))
    return table.filter(mask).select(PAGEVIEWS_SCHEMA.names)


def read_domains():
    """Return the domains table (domain_code, language), typed as strings."""
    return pcsv.read_csv(
        DATA_DIR / DOMAINS_CSV,
        convert_options=pcsv.ConvertOptions(column_types={
            "domain_code": pa.string(),
            "language": pa.string(),
        }),
    ).select(DOMAINS_SCHEMA.names)


def hour_corrections(table):
    """Return the hour-02 table with the late correction applied (same schema).

    The first CORRECTED_TITLES distinct page titles in byte order (across all
    four domains) have views halved, rounded down.
    """
    titles = table["page_title"].to_pylist()
    first = sorted(set(titles), key=lambda title: title.encode("utf-8"))[:CORRECTED_TITLES]
    corrected = set(first)
    views = [
        count // 2 if title in corrected else count
        for title, count in zip(titles, table["views"].to_pylist())
    ]
    index = table.schema.get_field_index("views")
    return table.set_column(index, "views", pa.array(views, type=pa.int64()))


def _labels(ids, word):
    return {value: f"{word} {number}" for number, value in enumerate(ids, start=1)}


def snapshot_labels(ids):
    """Map snapshot IDs, given in commit order, to "snapshot 1", "snapshot 2", and so on."""
    return _labels(ids, "snapshot")


def version_labels(versions, word="version"):
    """Map Delta versions or DuckLake snapshot numbers, in commit order, to "version 1", and so on.

    Pass word="commit" to get "commit 1", which does not read as Delta's own version numbers.
    """
    return _labels(versions, word)


def _sort_key(row):
    # None sorts first in each column and never meets a non-None value in a comparison.
    return tuple((value is not None, value) for value in row)


def sql_rows(con, sql, *, keep_order=False, **tables):
    """Register each Arrow table under its keyword name, run sql, return the rows.

    The rows are sorted, so printed output is stable, unless keep_order is True,
    which returns them in the order the SQL gives."""
    for name, table in tables.items():
        con.register(name, table)
    try:
        rows = con.execute(sql).fetchall()
    finally:
        for name in tables:
            con.unregister(name)
    return rows if keep_order else sorted(rows, key=_sort_key)


def scan_arrow(table, snapshot_id=None):
    """Return the Iceberg table as of snapshot_id (current if None), sorted by all columns."""
    arrow = table.scan(snapshot_id=snapshot_id).to_arrow()
    return arrow.sort_by([(name, "ascending") for name in arrow.column_names])


def parquet_files(table):
    """Return the sorted data-file paths a scan of the current snapshot plans. Count them; do not print them."""
    return sorted(task.file.file_path for task in table.scan().plan_files())


def iceberg_file_count(table):
    """Return the number of data files in the current snapshot."""
    return len(table.inspect.files())


def read_delta(path, version=None):
    """Return the Delta table at path (as of version; current if None) as a sorted Arrow table.

    Reads go only through deltalake's QueryBuilder. DeltaTable.to_pyarrow_table()
    and to_pyarrow_dataset() abort at exit with these pins (plan section 16).
    """
    dt = DeltaTable(path) if version is None else DeltaTable(path, version=version)
    arrow = pa.table(QueryBuilder().register("t", dt).execute("SELECT * FROM t"))
    # DataFusion returns string_view and binary_view, which Arrow cannot sort; cast them back.
    plain = {pa.string_view(): pa.string(), pa.binary_view(): pa.binary()}
    arrow = arrow.cast(pa.schema([pa.field(f.name, plain.get(f.type, f.type)) for f in arrow.schema]))
    return arrow.sort_by([(name, "ascending") for name in arrow.column_names])


def delta_file_count(dt):
    """Return the number of data files in the current Delta version."""
    return len(dt.file_uris())


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ducklake_file_count(con, alias, table):
    """Return the number of data files DuckLake lists for a table in the wiki schema."""
    if not (_IDENTIFIER.match(alias) and _IDENTIFIER.match(table)):
        raise ValueError("alias and table must be plain identifiers")
    rows = con.execute(
        f"SELECT count(*) FROM ducklake_list_files('{alias}', '{table}', schema => '{NAMESPACE}')"
    ).fetchall()
    return rows[0][0]


def ducklake_file_paths(con, alias, table):
    """Return the set of data-file paths DuckLake lists for a table in the wiki schema. Compare them; do not print them."""
    if not (_IDENTIFIER.match(alias) and _IDENTIFIER.match(table)):
        raise ValueError("alias and table must be plain identifiers")
    rows = con.execute(
        f"SELECT data_file FROM ducklake_list_files('{alias}', '{table}', schema => '{NAMESPACE}')"
    ).fetchall()
    return {row[0] for row in rows}


def _local_path(uri):
    return urllib.parse.urlparse(uri).path


def orphan_data_files(table):
    """Return the sorted local paths of Parquet files under the table that no snapshot references.

    Count them; do not print them.
    """
    on_disk = {str(path) for path in Path(_local_path(table.location())).rglob("*.parquet")}
    referenced = set()
    for snapshot in table.metadata.snapshots:
        files = table.inspect.files(snapshot_id=snapshot.snapshot_id)
        referenced.update(_local_path(path) for path in files["file_path"].to_pylist())
    return sorted(on_disk - referenced)


def digest_rows(rows):
    """Return the SHA-256 hex digest of the sorted rows, one canonical line per row."""
    text = "\n".join(repr(row) for row in sorted(rows, key=_sort_key))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _cell(value):
    return "NULL" if value is None else str(value)


def print_rows(rows, *, keep_order=False):
    """Print the rows, one per line, tab-separated. None prints as NULL.

    The rows are sorted unless keep_order is True, which prints them in the order given."""
    for row in (rows if keep_order else sorted(rows, key=_sort_key)):
        print("\t".join(_cell(value) for value in row))


def print_schema(schema):
    """Print an Arrow schema: one line per column, sorted by name, with its type and nullability."""
    for field in sorted(schema, key=lambda field: field.name):
        nullable = "nullable" if field.nullable else "required"
        print(f"{field.name}\t{field.type}\t{nullable}")


_HOME = os.path.expanduser("~")
_ABSOLUTE = re.compile(r"(?<![\w./:])/[^\s'\"),]+")


def safe_error(exc, wh=None):
    """Return "ClassName: message" with the warehouse, home folder, temp folder, file URIs and other absolute paths replaced."""
    message = str(exc)
    if wh is not None:
        message = message.replace(str(wh.root), "<warehouse>")
    if _HOME not in ("", "/", "~"):
        message = message.replace(_HOME, "<home>")
    message = message.replace(tempfile.gettempdir(), "<tmp>")
    message = re.sub(r"file://\S+", "<uri>", message)
    message = _ABSOLUTE.sub("<path>", message)
    return f"{type(exc).__name__}: {message}"


def split_nulls(con, table, label, column, before_hour, *, sides=("before the add", "after the add")):
    """Print the rows of table split at before_hour, with the nulls in column for each side.

    Rows whose hour of day is below before_hour count as before. Each label line
    names the side, so the prose can bind the counts. Used by chapter 3.
    sides names the two halves in the printed labels.
    """
    sql = f"""SELECT EXTRACT(hour FROM hour_utc) < {int(before_hour)} AS early, count(*) AS n,
              count(*) - count({column}) AS missing FROM t GROUP BY 1"""
    for early, n, missing in reversed(sql_rows(con, sql, t=table)):
        when = sides[0] if early else sides[1]
        print(f"{label} rows {when}: {n}")
        print(f"{label} rows {when} with {column} null: {missing}")


def copy_to_ducklake(wh, table, *, partition_by=None):
    """Copy an Arrow table into a new DuckLake table, lake.wiki.pageviews_hourly, and return its name.

    The copy goes through Arrow. The DuckLake catalog is attached with inlining off, so the rows are written as Parquet files.
    With partition_by, such as "hour(hour_utc)", the target is created empty, given that partition spec, and only then filled,
    so the copied rows are written as partitioned files. Without it, the table is created with the rows in one statement.
    """
    connect_ducklake(wh.con)
    attach_ducklake(wh.con, DUCKLAKE_ALIAS, wh.root, inlining_row_limit=0)
    wh.con.execute(f"CREATE SCHEMA {DUCKLAKE_ALIAS}.{NAMESPACE}")
    wh.con.register("source", table)
    target = f"{DUCKLAKE_ALIAS}.{NAMESPACE}.{TABLE}"
    if partition_by is None:
        wh.con.execute(f"CREATE TABLE {target} AS SELECT * FROM source")
        return target
    wh.con.execute(f"CREATE TABLE {target} AS SELECT * FROM source LIMIT 0")
    wh.con.execute(f"ALTER TABLE {target} SET PARTITIONED BY ({partition_by})")
    wh.con.execute(f"INSERT INTO {target} SELECT * FROM source")
    return target


def _ducklake_table_id(alias, table):
    return (f"(SELECT table_id FROM __ducklake_metadata_{alias}.ducklake_table "
            f"WHERE table_name = '{table}' AND end_snapshot IS NULL)")


def ducklake_partition_transforms(con, alias=DUCKLAKE_ALIAS, *, table=None, current=False):
    """Return the transforms of the partition columns DuckLake lists for the attached catalog. Empty when no table is partitioned.

    With table, only that table's columns are listed. With current=True, only the spec still in force (end_snapshot is NULL)
    is listed, in key order. Both options read the catalog tables through the internal __ducklake_metadata_<alias> database.
    """
    if table is not None and not _IDENTIFIER.match(table):
        raise ValueError("table must be a plain identifier")
    if table is None and not current:
        rows = con.execute(f"SELECT transform FROM __ducklake_metadata_{alias}.ducklake_partition_column").fetchall()
        return [row[0] for row in rows]
    meta = f"__ducklake_metadata_{alias}"
    clauses = []
    if table is not None:
        clauses.append(f"c.table_id = {_ducklake_table_id(alias, table)}")
    if current:
        clauses.append("i.end_snapshot IS NULL")
    rows = con.execute(
        f"SELECT c.transform FROM {meta}.ducklake_partition_column c "
        f"JOIN {meta}.ducklake_partition_info i USING (partition_id, table_id) "
        f"WHERE {' AND '.join(clauses)} ORDER BY c.partition_key_index"
    ).fetchall()
    return [row[0] for row in rows]


def ducklake_files_by_partition(con, alias, table):
    """Return (partition value, data files, rows) for each partition value of a table's current data files, in value order.

    Reads the catalog tables through the internal __ducklake_metadata_<alias> database. Files with no partition value show as None.
    """
    if not (_IDENTIFIER.match(alias) and _IDENTIFIER.match(table)):
        raise ValueError("alias and table must be plain identifiers")
    meta = f"__ducklake_metadata_{alias}"
    return con.execute(
        f"SELECT v.partition_value, count(*), sum(f.record_count) "
        f"FROM {meta}.ducklake_data_file f "
        f"LEFT JOIN {meta}.ducklake_file_partition_value v USING (data_file_id, table_id) "
        f"WHERE f.table_id = {_ducklake_table_id(alias, table)} AND f.end_snapshot IS NULL "
        f"GROUP BY v.partition_value ORDER BY v.partition_value"
    ).fetchall()


def write_delta_hours(root, hours):
    """Append each hour of the cut to a Delta Lake table under root, one commit per hour, and return its path."""
    path = str(Path(root) / "pageviews_hourly")
    for hour in hours:
        write_deltalake(path, read_cut_hour(hour), mode="append")
    return path
