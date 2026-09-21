#!/usr/bin/env bash
# Daily r2.rbw.sh ingestion: scan the public R2 buckets (ctbk, crashes,
# jc-taxes) → union path-index + coarse tiers + snapshot → upload to the
# disk-tree-demo bucket → publish footers to the disk-tree-demo-db D1. The Map
# (site/) reads that index. Invoked by com.runsascoded.disk-tree.r2-ingest;
# idempotent per date (a re-run lands a fresh generation and flips the pointer).
#
# Creds come from .envrc via direnv (R2_HCCS_RO_* for HCCS ctbk/crashes,
# R2_RO_* for RAC jc-taxes, R2_RW_* to write disk-tree-demo, CF_TOKEN +
# CLOUDFLARE_ACCOUNT_ID for D1). The HCCS account endpoint stays out of this
# public repo — read from the untracked ~/.config/disk-tree/buckets.yml.
set -euo pipefail

REPO=/Users/ryan/c/disk-tree
cd "$REPO"
eval "$(direnv export bash)"

DATE=$(date -u +%Y-%m-%d)
GEN=$(date -u +%Y%m%d%H%M)
BUCKET=disk-tree-demo
D1_ID=261d0c80-8f73-442e-979a-4e25725b2271
HCCS_EP=$(awk '/r2:\/\/ctbk/{f=1} f&&/endpoint_url/{print $2; exit}' "$HOME/.config/disk-tree/buckets.yml")
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "[r2-ingest] $DATE gen=$GEN"

# 1. List each bucket with its account's read creds → canonical listing shards.
list() { AWS_ACCESS_KEY_ID="$1" AWS_SECRET_ACCESS_KEY="$2" AWS_DEFAULT_REGION=auto \
  disk-tree bulk-list "r2://$3" -E "$4" -o "$WORK/listing/$3" -P 4; }
list "$R2_HCCS_RO_ACCESS_KEY_ID" "$R2_HCCS_RO_SECRET_ACCESS_KEY" ctbk     "$HCCS_EP"
list "$R2_HCCS_RO_ACCESS_KEY_ID" "$R2_HCCS_RO_SECRET_ACCESS_KEY" crashes  "$HCCS_EP"
list "$R2_RO_ACCESS_KEY_ID"      "$R2_RO_SECRET_ACCESS_KEY"      jc-taxes "$R2_ENDPOINT_URL"

# 2. Union all three (grouped by bucket → the Map's top cells) → path-index +
#    coarse tiers (beside -P) + snapshot JSONs.
( cd cloud && uv run dt-cloud webdata -d "$DATE" -l "$WORK/listing/*/*.parquet" \
    -P "$WORK/index/path-index.parquet" -o "$WORK/snap" )

# 3. Upload tiers + snapshot to the index bucket (RW), then footers → D1.
put() { AWS_ACCESS_KEY_ID="$R2_RW_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$R2_RW_SECRET_ACCESS_KEY" AWS_DEFAULT_REGION=auto \
  aws s3 cp "$1" "s3://$BUCKET/$2" --recursive --endpoint-url "$R2_ENDPOINT_URL" --only-show-errors; }
put "$WORK/index/" "listing/$DATE/index/$GEN/"
put "$WORK/snap/"  "snapshots/r2/$DATE/"

( cd cloud && CLOUDFLARE_API_TOKEN="$CF_TOKEN" D1_DB_ID="$D1_ID" D1_DB_NAME="$BUCKET-db" INDEX_VARIANTS=path \
    uv run dt-cloud index-sync "$DATE" -g "$GEN" -b "$BUCKET" -k "listing/$DATE/index/$GEN" \
      -d "$WORK/index" -v path -v coarse16 -v coarse20 -v coarse24 )

echo "[r2-ingest] $DATE gen=$GEN done"
