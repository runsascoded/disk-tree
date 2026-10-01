# disk-tree

Cloud storage usage analyzer: a scanning/indexing CLI (`disk-tree`), a cloud overlay (`dt-cloud`), and a Cloudflare-hosted site (`site/`) over the indexes.

## Project Vision

Track storage usage across object stores: S3, R2 and GCS buckets.

Key goals:
- **Always-ready index**: Scheduled scans (the r2 demo's daily ingest) so the index is there when you need it
- **Fast indexing**: Shard a bucket's listing across workers (`bulk-list`), aggregate out of core (`import`)
- **Site**: Treemap visualizations, diffs and an age lens over each scan's path index

## Architecture

### Python Backend (`src/disk_tree/`)

**Listing** (`find/bulk*.py`): `bulk-list` shards a bucket's object listing (`gcs://`, `s3://`,
`r2://` via its S3 endpoint) across worker processes into layer-1 listing parquet shards.

**Aggregation** (`find/import_listing.py`, `find/index.py`, `find/aggregate_{duckdb,stream}.py`):
`import` aggregates a listing bottom-up into the canonical layer-2 frame — columns `path`, `size`,
`mtime`, `kind`, `parent`, `uri`, `n_desc`, `n_children`, `n_files`, `depth` (+ opt-in pivots,
`mtime_mean`, labels), sorted `(depth, path)` so parquet pushdown prunes by depth and path prefix.
Three engines, byte-identical output: `pandas` (in memory), `duckdb` (out of core), `stream`
(O(depth) over sorted listings).

**Output** (`storage/`, `sqla/model.py`): each imported bucket's layer-2 is adopted into the write
dir (`<DISK_TREE_ROOT>/scans/<uuid>.parquet`, or `--to <dir|url>`) and recorded as a `Scan` row
(`path`, `time`, `blob`, root `size`/`n_children`/`n_desc`/`mtime`) in `<DISK_TREE_ROOT>/disk-tree.db`.
cw-s3's jobs read that blob back (`ls $DISK_TREE_ROOT/scans/*.parquet`) as `dt-cloud index-write`'s input.

**CLI** (`cli/`):
```bash
disk-tree bulk-list URI    # Shard URI's object listing (gcs:// | s3:// | r2:// with `-E <endpoint>`) into
                          # layer-1 parquet shards at `-o` (`-a` adaptive range-splitting, `-P` procs)
disk-tree import -l GLOB…  # Aggregate listing(s) into a canonical layer-2 per bucket (`-b`; default every
                          # bucket in the listings): `-e pandas|duckdb|stream`, `-p` pivot sums, `-m`
                          # mtime_mean, `-i` also cut the path-store tiers, `-w/--to` write dir or URL

disk-tree recompress PATH…  # Rewrite v1 layer-2 listings as v2 in place, lossless (spec `listing-slim.md`
                          # phase 2): files, dirs (recursive `*.parquet`, sidecars skipped) or fsspec URLs
                          # (`gs://`, `r2://`, `s3://`). Streams by row group (never a whole file in memory):
                          # drops `uri` (scan root → metadata; refused unless `uri == <root>/<path>` on every
                          # row) and the `sum_*` pivots equal to `size`, re-encodes under
                          # `$DISK_TREE_PARQUET_CODEC` in ≤64K-row groups to a `.v2.tmp` sibling, verifies
                          # (row count + order-insensitive digest of `(path,size,mtime,kind)`), then swaps
                          # (atomic rename; copy + delete on a URL). v2 input is skipped; a failure leaves
                          # the original untouched, exit 1. `-n` dry-run, `-k` keeps `<stem>.v1.parquet`,
                          # `-j` JSON. Per-file old/new size + ratio, and totals
disk-tree listing-format PATH…  # The audit: each parquet's listing format (v1 | v2 | not-a-listing), codec,
                          # row groups, rows, size (+ root/implied for v2) from the footer only; `-j` JSON

disk-tree tiers L2        # Cut the path store's sorts (spec `path-store.md` §1.2/§4.1) from a layer-2 — a
                          # local path or an fsspec URL (copied once through `blobfs.open_read`): `path` =
                          # every row (objects + dirs) sorted `(depth, path, …labels)`; `bysize` = the same
                          # rows sorted `(⌊log2 size⌋ desc, path, …labels)`, size 0 last, the bucket computed
                          # in SQL (never stored). 8K-row groups (`-r`), `tier`/`sort` (+ `bucket: log2`) in
                          # the parquet metadata, the source's listing format inherited. `-t path,bysize`
                          # (default both), `-s STEM` (may be a URL: cut locally, uploaded), `-g` writes the
                          # `.groups.json` footer sidecar beside each (`find/groups.py`; carries `b_min` now),
                          # `-v usr` extra sorted copies led by those columns, `-j` JSON; `-m` DuckDB memory limit (default 8GB — an
                          # external sort, spills to `-T`, default `.duckdb-tmp` beside the stem; unbounded it took
                          # 28 GB for 57M rows), `-p` threads. Prints rows, groups,
                          # bytes, KV per tier. `import -i` does the same at import time (bare `-i` = both;
                          # the `dirs`/`objects`/`coarse` tiers are retired). The cloud overlay's
                          # `dt-cloud index-write` (cw) and `dt-cloud path-index -P` (gcs, the r2 demo)
                          # cut the same two sorts from their bucket unions — objects as rows, L2 column
                          # names — under `path-index[-bysize][-by-user].parquet` (spec §4.2–4.4)
disk-tree tiers plan SIDECAR P THR  # The reader's span selection run offline over a tier's `.groups.json`
                          # (phase 0's instrument): for `path` it mirrors `readRects` exactly (depth rect
                          # `dP+1..`, path range `[P/, P0)`, `b_max ≥ thr·atten^(d−dP−1)` per group); for
                          # `bysize` it is `b_max ≥ thr_min ∧ p_max ≥ P/ ∧ p_min < P0`. Reports groups
                          # selected, rows they hold, bytes (compressed chunks), and — from the parquet —
                          # rows that actually pass, i.e. the decode waste. `P` = `.` for the root; `-a`
                          # attenuation, `-d` max depth, `-t` tier (default: from the name), `-C` skips the
                          # count, `-j` JSON. Measured on a 20K-child flat dir at 2048-row groups: `path`
                          # decodes 10 groups / 20,015 rows for 1,250 answers, `bysize` 1 group / 2,048
```

### Site (`site/`) and widget packages

`site/` is the hosted/serverless app: Vite + React + TypeScript, with Pages Functions over R2 / GCS +
D1 (r2.rbw.sh, the gcs/cw deployments). Its chart-lib-free DIY-SVG/canvas widgets come
from two workspace packages:
- **`@rdub/treemap`** (`packages/treemap/`) — the SOTA treemap core + layout/color primitives:
  `<Treemap>` (SVG + canvas renderers, shared-edge tiling, dust texturing, per-cell `ring`
  emphasis, hover/pin events), `<VoronoiTreemap>` (`/voronoi` subpath), `useHoverPin`, `squarify`,
  `DEFAULT_PALETTE`/`ageFade`/`parseQuery`, `styles.css`. Disk-agnostic; the intended external
  consumer (e.g. file-tree) pins this.
- **`@disk-tree/react`** (`packages/react/`) — `<TimeSeries>`/`<BytesOverTime>` built on the core
  (which it re-exports for back-compat).

### Cloud site auth routes (`site/functions/`)

One gate (`_lib/auth.ts`, `@open-athena/auth` over D1): `identify` → `Identity` (`via: session | grant | public`), `requireViewer` / `requireStager` / `requireAdmin`.
- `/auth/google` (+ `/callback`, `/onetap`) — Google OIDC → email session; `/auth/email/*` — emailed code / magic link → email session
- `/api/auth/*` — the package's routes: `whoami`, `exchange` (`?key=` share link → grant session), `logout`, request-access, admin grant/request/log console
- `/api/token` — a session's personal agent token (a non-expiring Bearer grant, the base scope only; grants can't mint one)

## Development

```bash
# Python setup — one uv workspace: the engine (root) + the cloud overlay
# (`cloud/`, package `dt-cloud`) share ONE `uv.lock` and one `.venv`.
uv sync                                                 # engine only
uv sync --all-packages --all-extras --all-groups        # engine + dt-cloud, every extra, test groups
disk-tree bulk-list -a s3://<bucket> -o work/listing/<bucket>
disk-tree import -e stream -s s3 -b <bucket> -l 'work/listing/<bucket>/shard-*.parquet'

# Site
pnpm install
cd site && pnpm dev
```

## Packaging / Distribution

`uv build` builds the engine's wheel (`src/disk_tree`; CLI only); there is no release workflow.
`dt-cloud` (`cloud/`) is a workspace member installed from the lock, not published.

## Data Flow

1. `disk-tree bulk-list` writes a bucket's sharded object listing
2. `disk-tree import` aggregates it by directory into a layer-2 parquet (+ a `Scan` row), or
   `dt-cloud path-index` aggregates the listings directly (gcs, the r2 demo's daily ingest)
3. `dt-cloud path-index` / `index-write` / `disk-tree tiers` cut the path store; `dt-cloud index-sync`
   publishes its footers to D1
4. `site/`'s Pages Functions read the path store (row-group range reads) and render treemaps / diffs

## Config

Default paths (override with `DISK_TREE_ROOT`):
- `~/.config/disk-tree/disk-tree.db` — SQLite `Scan` rows (`import`)
- `~/.config/disk-tree/scans/` — `import`'s layer-2 blobs
- `~/.config/disk-tree/buckets.yml` — per-bucket `endpoint_url` / `profile`

`import --to <dir|url>` (`config.set_write_target`) writes the blobs elsewhere — a local dir or an **fsspec URL** (`r2://bucket/prefix`, `s3://…`, `gs://…`); `DISK_TREE_SCAN_DIRS` (colon-separated, the first is the write target) sets it from the env. `r2://` rides s3fs with the bucket's endpoint from `DISK_TREE_R2_ENDPOINT_URL` or its `buckets.yml` entry. Every parquet read/write goes through `blobfs.py` (the local-vs-URL seam), and blobs are written in 64K-row groups so a `depth`/`path` pushdown over R2 fetches kilobytes.

**Cross-account credentials** — a `buckets.yml` entry (or `defaults`) may carry a `profile:` naming an AWS credential profile (`blobfs.bucket_profile`), so a source and a target in *different* accounts each authenticate with their own key. It threads to every S3/R2 seam: the `s3fs` blob IO (`_s3fs(endpoint, profile)`) and the `boto3` bulk lister (`S3BulkLister(profile=…)`, `bulk-list -f`). No profile → ambient credentials (env / default profile), the single-account default. Cross-account needs per-bucket endpoints too, so leave `DISK_TREE_R2_ENDPOINT_URL` unset (it globally overrides all per-bucket endpoints).

Stream-engine tuning knobs (env, all with measured defaults — see the constants block in `find/aggregate_stream.py`):
- `DISK_TREE_FLUSH_ROWS` — output row-group size (read-side: smaller = less fetched per directory browse, bigger footer)
- `DISK_TREE_PARALLEL_FINALIZE_MIN_ROWS` — below this the finalize stays serial even at `-j N`
- `DISK_TREE_SCAN_BATCH_ROWS` — pass-1 read-batch size (partition balance / seek granularity)

## Tests

```bash
pytest tests/                    # engine
cd cloud && pytest               # dt-cloud (same venv; sync with --all-packages first)
```

Tests build their listing fixtures inline (`tmp_path` parquets). CI and the job images install `--frozen` from the workspace lock (`deploy/sheet-mirror/Dockerfile` is the reference recipe: `uv sync --frozen --no-dev --no-editable --package dt-cloud --extra …` into `UV_PROJECT_ENVIRONMENT=/usr/local`); a plain `pip install .` resolves fresh and ships pins the tests never ran.

## Performance

- Depth column enables parquet predicate pushdown (only load needed rows)
- The site reads the path store by row-group range reads, planned from footers in D1 (`index-sync`)
