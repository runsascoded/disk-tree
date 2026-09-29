#!/usr/bin/env bash
# One laptop capture → path index + snapshot, back to R2 (specs/m3-site.md).
#
#   ingest.sh r2://disk-tree/captures/<host>/<root slug>/<stamp>
#
# Mirrors .github/workflows/daily-ingest.yml's union → path-index → upload →
# footers-to-D1 steps, with the capture's shards as the listing. R2 keys arrive
# as AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and the D1 token as
# CLOUDFLARE_API_TOKEN (Batch injects them from Secrets Manager); the endpoint
# is R2_ENDPOINT_URL, the database D1_DB_ID / D1_DB_NAME. Logs are private (CloudWatch), but print
# totals only anyway: no laptop paths.
set -euo pipefail

capture=${1:?usage: ingest.sh r2://<bucket>/<capture dir>}
: "${R2_ENDPOINT_URL:?}"
export AWS_DEFAULT_REGION=auto
bucket=${capture#r2://}; bucket=${bucket%%/*}
key=${capture#r2://$bucket/}
date=${DATE:-$(date -u +%F)}
gen=${GEN:-$(date -u +%Y%m%d%H%M)}
store=${STORE:-laptop}
work=${WORK:-/work}
s3() { aws s3 "$@" --endpoint-url "$R2_ENDPOINT_URL" --only-show-errors; }

echo "[ingest] capture=$key date=$date gen=$gen store=$store vcpu=$(nproc) mem=$(awk '/MemTotal/{printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
mkdir -p "$work/listing" "$work/index" "$work/snap"

t0=$(date +%s)
s3 cp "s3://$bucket/$key/" "$work/listing/" --recursive --exclude '*' --include '*.parquet'
echo "[ingest] fetched $(find "$work/listing" -name '*.parquet' | wc -l) shards, $(du -sh "$work/listing" | cut -f1) in $(( $(date +%s) - t0 ))s"

t0=$(date +%s)
( cd /app/cloud && /usr/bin/time -v -o "$work/time.txt" .venv/bin/dt-cloud path-index -d "$date" \
    -l "$work/listing/*.parquet" -P "$work/index/path-index.parquet" -o "$work/snap" )
echo "[ingest] path-index: $(( $(date +%s) - t0 ))s, peak RSS $(awk -F': ' '/Maximum resident/{printf "%.2f GiB", $2/1048576}' "$work/time.txt")"
echo "[ingest] outputs: index $(du -sh "$work/index" | cut -f1), snap $(du -sh "$work/snap" | cut -f1)"

t0=$(date +%s)
s3 cp "$work/index/" "s3://$bucket/listing/$store/$date/index/$gen/" --recursive
s3 cp "$work/snap/"  "s3://$bucket/snapshots/$store/$date/" --recursive
echo "[ingest] uploaded to s3://$bucket/{listing/$store/$date/index/$gen,snapshots/$store/$date}/ in $(( $(date +%s) - t0 ))s"

t0=$(date +%s)
: "${D1_DB_ID:?}" "${CLOUDFLARE_API_TOKEN:?}" "${CLOUDFLARE_ACCOUNT_ID:?}"
( cd /app/cloud && .venv/bin/dt-cloud index-sync "$date" -g "$gen" -b "$bucket" -k "listing/$store/$date/index/$gen" \
    -d "$work/index" -v path -v coarse16 -v coarse20 -v coarse24 )
echo "[ingest] footers → D1 $D1_DB_NAME in $(( $(date +%s) - t0 ))s"
