#!/usr/bin/env python3
"""Refresh the vendored DuckLake extension for the book's offline examples.

The examples run with the network blocked, and DuckDB's INSTALL would fetch
the extension from extensions.duckdb.org, so DuckDB's official linux_amd64
artifact for the duckdb version pinned in examples/requirements.txt is vendored
under examples/vendor/ducklake/, compressed, exactly as served. Readers on their
own machines should use `INSTALL ducklake` instead; the vendored copy is only
for offline runs and the Lab.

Usage:
    python tools/refresh-ducklake.py          download, store the .gz, rewrite SOURCE.toml
    python tools/refresh-ducklake.py --check  re-download and compare with SOURCE.toml
"""

import argparse
import datetime
import gzip
import hashlib
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REQUIREMENTS = ROOT / "examples" / "requirements.txt"
VENDOR = ROOT / "examples" / "vendor" / "ducklake"
VENDORED = VENDOR / "ducklake.duckdb_extension.gz"
UNCOMPRESSED = VENDOR / "ducklake.duckdb_extension"
SOURCE = VENDOR / "SOURCE.toml"

PLATFORM = "linux_amd64"
UPSTREAM = "https://github.com/duckdb/ducklake"


def duckdb_pin() -> str:
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^duckdb==(\S+)\s*$", line)
        if match:
            return match.group(1)
    sys.exit(f"no 'duckdb==' pin found in {REQUIREMENTS}")


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_source() -> dict:
    """Parse the flat key = value lines that SOURCE.toml uses."""
    values = {}
    for line in SOURCE.read_text(encoding="utf-8").splitlines():
        match = re.match(r'^(\w+)\s*=\s*"?([^"]*)"?\s*$', line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


def write_source(version: str, url: str, gz: bytes, raw: bytes) -> None:
    text = "\n".join(
        [
            "# Provenance for the vendored DuckLake extension. Written by tools/refresh-ducklake.py.",
            "#",
            "# The vendored file is DuckDB's official artifact exactly as served: the gzip",
            "# compressed extension, unchanged. DuckDB cannot LOAD a .gz, so",
            "# examples/lib/lakehouse.py decompresses it into a temporary directory, checks",
            "# the SHA-256 of the decompressed file against `sha256` below, and then LOADs it",
            "# by path. The decompressed file is never written into the repository.",
            f'url = "{url}"',
            f'duckdb_version = "{version}"',
            f'platform = "{PLATFORM}"',
            f'vendored_file = "{VENDORED.name}"',
            f'sha256_gz = "{sha256(gz)}"',
            f"size_bytes_gz = {len(gz)}",
            f'sha256 = "{sha256(raw)}"',
            f"size_bytes = {len(raw)}",
            'licence = "MIT"',
            'licence_file = "LICENSE"',
            f'upstream = "{UPSTREAM}"',
            f'fetched = "{datetime.date.today().isoformat()}"',
            "",
        ]
    )
    SOURCE.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--check", action="store_true",
                        help="re-download and compare with SOURCE.toml; exit 1 on mismatch")
    args = parser.parse_args()

    version = duckdb_pin()
    url = f"https://extensions.duckdb.org/v{version}/{PLATFORM}/ducklake.duckdb_extension.gz"
    gz = fetch(url)
    # Decompressed only in memory, to record the hash DuckDB's loader needs.
    raw = gzip.decompress(gz)

    if args.check:
        recorded = read_source()
        actual = {
            "url": url,
            "duckdb_version": version,
            "sha256_gz": sha256(gz),
            "size_bytes_gz": str(len(gz)),
            "sha256": sha256(raw),
            "size_bytes": str(len(raw)),
        }
        problems = [
            f"{key}: recorded {recorded.get(key)!r}, upstream {value!r}"
            for key, value in actual.items()
            if recorded.get(key) != value
        ]
        if problems:
            print("MISMATCH:", *problems, sep="\n  ")
            return 1
        if not VENDORED.exists():
            print("MISMATCH: vendored file is missing; run without --check")
            return 1
        if sha256(VENDORED.read_bytes()) != actual["sha256_gz"]:
            print("MISMATCH: vendored file differs from upstream; run without --check")
            return 1
        if UNCOMPRESSED.exists():
            print("MISMATCH: uncompressed extension found in the repository; delete it")
            return 1
        print(f"ok: vendored DuckLake matches upstream v{version} "
              f"(gz {actual['sha256_gz'][:8]}, extension {actual['sha256'][:8]})")
        return 0

    VENDOR.mkdir(parents=True, exist_ok=True)
    VENDORED.write_bytes(gz)
    if UNCOMPRESSED.exists():
        UNCOMPRESSED.unlink()
    write_source(version, url, gz, raw)
    print(f"wrote {VENDORED.name} ({len(gz)} bytes) for duckdb {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
