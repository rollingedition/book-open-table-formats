#!/usr/bin/env bash
# Cut the vendored pageviews subset for Open Table Formats (plan section 3.2).
#
# Streams six hourly Wikimedia pageviews dumps for 2024-06-03 (00:00 to 05:00 UTC)
# through a filter, so no full dump is stored. Writes:
#   OUT_DIR/pageviews-2024-06-03.csv   (2,999 data rows: six hours by 500, less one private row)
#   and prints per-hour candidate, dropped and kept counts to stdout
# The dimension table examples/data/domains.csv is hand-written and not generated here.
#
# Usage: tools/cut-pageviews.sh [OUT_DIR] [WORK_DIR]
#   OUT_DIR   default: <repo>/examples/data
#   WORK_DIR  default: a temporary directory, removed on exit
#
# Needs: bash, curl, gunzip, mawk or gawk, GNU coreutils (sort, head, wc).
# Every sort runs under LC_ALL=C, so "byte order" means byte order.

set -euo pipefail
export LC_ALL=C

BASE="https://dumps.wikimedia.org/other/pageviews/2024/2024-06"
DAY="20240603"
HOURS="000000 010000 020000 030000 040000 050000"
TOP_N=500
DOMAINS_RE='^(en|de|fr|ja)$'

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/examples/data}"
WORK_DIR="${2:-}"
if [ -z "$WORK_DIR" ]; then
  WORK_DIR="$(mktemp -d)"
  trap 'rm -rf "$WORK_DIR"' EXIT
fi
mkdir -p "$OUT_DIR" "$WORK_DIR"

OUT_CSV="$OUT_DIR/pageviews-2024-06-03.csv"
COUNTS_CSV="$WORK_DIR/cut-counts.csv"
ALL_ROWS="$WORK_DIR/all-rows.txt"
REVIEW_LOG="$WORK_DIR/privacy-review.txt"
: > "$REVIEW_LOG"
: > "$ALL_ROWS"
echo "hour_utc,candidates,top_n,private_dropped,kept" > "$COUNTS_CSV"

for HHMMSS in $HOURS; do
  HH="${HHMMSS:0:2}"
  HOUR_UTC="2024-06-03T${HH}:00:00"
  URL="$BASE/pageviews-${DAY}-${HHMMSS}.gz"
  FILTERED="$WORK_DIR/filtered-$HH.txt"
  TOP="$WORK_DIR/top-$HH.txt"
  TALLY="$WORK_DIR/tally-$HH.txt"

  # 1. Stream the dump. Keep the four domains and drop Special: pages and "-" titles.
  #    Rows are "domain title views bytes", space-separated (titles hold no spaces).
  curl -sSf --max-time 900 "$URL" \
    | gunzip \
    | mawk -v dre="$DOMAINS_RE" -v tally="$TALLY" '
        NF != 4 { next }
        $1 !~ dre { next }
        $2 ~ /^Special:/ || $2 == "-" { next }
        { cand++; print $1, $2, $3, $4 }
        END { print cand + 0 > tally }
      ' > "$FILTERED"
  CANDIDATES="$(cat "$TALLY")"

  # 2. Sort by views descending, then title byte order ascending, then domain
  #    (only breaks ties between the same title in two domains). Keep the first 500.
  #    (sorted to a file first, so head closing early cannot make sort fail under pipefail)
  sort -k3,3nr -k2,2 -k1,1 "$FILTERED" > "$FILTERED.sorted"
  head -n "$TOP_N" "$FILTERED.sorted" > "$TOP"
  KEPT="$(wc -l < "$TOP")"

  # 3. Privacy check on the 500 rows. Drop a row whose title looks like a private
  #    person's details: an e-mail address, a phone-number-shaped string, or a user or
  #    user-talk page (a contributor username; the same prefixes in de, fr and ja).
  #    Each drop is logged to REVIEW_LOG (a work file, not vendored) for a human check.
  #    Kept rows are appended to the shared file as "hour domain title views bytes".
  mawk -v h="$HOUR_UTC" -v review="$REVIEW_LOG" -v dropped_out="$TALLY" '
        $2 ~ /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z][A-Za-z]+/ ||
        $2 ~ /\+[0-9][0-9 ._()-]{7,}[0-9]/ ||
        $2 ~ /\(?[0-9]{3}\)?[-. _][0-9]{3}[-. _][0-9]{4}/ ||
        $2 ~ /^(User|User_talk|Benutzer|Benutzer_Diskussion|Utilisateur|Discussion_utilisateur|利用者|利用者‐会話):/ {
          drop++; print h, $1, $2 >> review; next
        }
        { print h, $1, $2, $3, $4 >> rows_out }
        END { print drop + 0 > dropped_out }
      ' rows_out="$ALL_ROWS" "$TOP"
  PRIV_DROPPED="$(cat "$TALLY")"
  echo "$HOUR_UTC,$CANDIDATES,$KEPT,$PRIV_DROPPED,$((KEPT - PRIV_DROPPED))" >> "$COUNTS_CSV"
  rm -f "$FILTERED" "$FILTERED.sorted" "$TOP" "$TALLY"
done

# 4. Final order: hour_utc, views descending, page_title byte order, domain.
#    Map to the five CSV columns. A page_title is quoted only if it holds a comma or
#    a double quote (none of the 2024-06-03 cut's titles do, if the counts file shows 0).
{
  echo "hour_utc,domain_code,page_title,views,response_bytes"
  sort -k1,1 -k4,4nr -k3,3 -k2,2 "$ALL_ROWS" \
    | mawk '{
        t = $3
        if (index(t, ",") || index(t, "\"")) { gsub(/"/, "\"\"", t); t = "\"" t "\"" }
        print $1 "," $2 "," t "," $4 "," $5
      }'
} > "$OUT_CSV"

cat "$COUNTS_CSV"
echo "wrote $OUT_CSV ($(($(wc -l < "$OUT_CSV") - 1)) data rows)"
