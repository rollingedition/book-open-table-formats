# Data for Open Table Formats

This folder holds the data the book's examples read. Every file here is vendored: the repository holds the exact bytes, and the examples never download anything.

| File | What it is | Licence |
|---|---|---|
| `pageviews-2024-06-03.csv` | 2,999 rows of hourly view counts for public Wikipedia article titles, 00:00 to 05:59 UTC on 3 June 2024 | CC0 1.0 (Wikimedia's pageviews data) |
| `domains.csv` | Four rows mapping a domain code to a language. Written by hand for this book. | CC BY 4.0 (the book's own data, not Wikimedia's) |
| `LICENSE` | The CC0 1.0 Universal legal code, for the pageviews file | CC0 1.0 |

## Source and licence

Source files (one per hour, all for 3 June 2024):

- https://dumps.wikimedia.org/other/pageviews/2024/2024-06/pageviews-20240603-000000.gz
- https://dumps.wikimedia.org/other/pageviews/2024/2024-06/pageviews-20240603-010000.gz
- https://dumps.wikimedia.org/other/pageviews/2024/2024-06/pageviews-20240603-020000.gz
- https://dumps.wikimedia.org/other/pageviews/2024/2024-06/pageviews-20240603-030000.gz
- https://dumps.wikimedia.org/other/pageviews/2024/2024-06/pageviews-20240603-040000.gz
- https://dumps.wikimedia.org/other/pageviews/2024/2024-06/pageviews-20240603-050000.gz

Licence statement, from the pageviews readme at https://dumps.wikimedia.org/other/pageviews/readme.html:

> All Analytics datasets are available under the Creative Commons CC0 dedication.

The statement links to https://creativecommons.org/publicdomain/zero/1.0/. The legal code is in `LICENSE`.

The pageviews file is a derived extract (a cut of those six files), so it stays CC0. `domains.csv` is the book's own data and falls under the book's CC BY 4.0 licence.

## Personal data

None. Each row is an aggregate count of views for a public article title, taken from dumps that hold no user, IP or account data. The cut was checked for titles that look like a private person's details. One row was dropped: a user-talk page named for a contributor's username (in hour 00). No other row matched the checks: e-mail-shaped strings, phone-number-shaped strings, or user and user-talk pages in English, German, French or Japanese.

## The cut

Run `tools/cut-pageviews.sh examples/data` from the repository root. It streams the six dumps through `gunzip` and `mawk` and never stores a full dump. It needs `curl`, `gunzip`, `mawk` or `gawk`, and GNU `sort` and `head`. The steps:

1. Keep rows whose `domain_code` is exactly `en`, `de`, `fr` or `ja`. Drop rows whose `page_title` starts with `Special:` or is `-`.
2. Within each hour, sort by `count_views` descending, then `page_title` in byte order (`LC_ALL=C`), then `domain_code`. Keep the first 500 rows.
3. Drop any kept row whose title matches the privacy checks above. The dropped rows are not replaced.
4. Write the five columns `hour_utc, domain_code, page_title, views, response_bytes`, sorted by `hour_utc`, then `views` descending, then `page_title`. A title is quoted only if it contains a comma or a double quote.

Row counts per hour (from the script's own counter):

| hour_utc | candidates after step 1 | kept in step 2 | dropped in step 3 | rows in file |
|---|---:|---:|---:|---:|
| 2024-06-03T00:00:00 | 1,232,465 | 500 | 1 | 499 |
| 2024-06-03T01:00:00 | 1,235,024 | 500 | 0 | 500 |
| 2024-06-03T02:00:00 | 1,252,548 | 500 | 0 | 500 |
| 2024-06-03T03:00:00 | 1,249,390 | 500 | 0 | 500 |
| 2024-06-03T04:00:00 | 1,196,887 | 500 | 0 | 500 |
| 2024-06-03T05:00:00 | 1,219,003 | 500 | 0 | 500 |
| **total** | | **3,000** | **1** | **2,999** |

The data has messy values on purpose: underscores in titles, titles in Japanese, a few titles quoted for their commas, and every `response_bytes` value at 0. Counts are skewed: the top title has far more views than the 500th.

## Checksums

SHA-256 of each vendored file:

| File | SHA-256 |
|---|---|
| `pageviews-2024-06-03.csv` | `2567626f8c329774765c89e1492fbdcee07af65edfd3de42790c41a47df50f58` |
| `domains.csv` | `a34c26c3cc323ca3b5a2315553d0f5aae56d2316426155d541d9fbf16ca7f1b3` |
| `LICENSE` | `a2010f343487d3f7618affe54f789f5487602331c0a8d03f49e9a7c547cf0499` |
