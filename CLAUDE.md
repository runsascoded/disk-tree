# disk-tree

Disk/cloud space usage analyzer with caching, CLI, and web UI.

## Project Vision

Track disk space usage across:
- Local filesystems (laptop, external SSDs)
- S3 buckets

Key goals:
- **Always-ready index**: Run overnight scans so you don't wait when running out of space
- **External media snapshots**: Keep cached views of SSDs even when unplugged
- **Fast indexing**: Shell out to `gfind`/`aws s3 ls` instead of slow Python stat calls
- **Fresher child patching**: When viewing a parent, newer child scans automatically patch in updated stats
- **Web UI**: Treemap visualizations and directory browsing

## Architecture

### Python Backend (`src/disk_tree/`)

**Indexing** (`find/index.py`):
- Local: `gfind -printf '%y %b %T@ %p\0'` → null-terminated, 512-byte block sizes (handles sparse files)
- S3: `aws s3 ls --recursive` → parses listing format
- Excludes CloudStorage paths (`~/Library/CloudStorage`) to avoid blocking on cloud I/O
- Builds DataFrame with columns: `path`, `size`, `mtime`, `kind`, `parent`, `uri`, `n_desc`, `n_children`, `depth`
- `depth` column enables predicate pushdown when loading parquet (major performance win)
- Aggregates sizes upward through directory tree
- Returns `IndexResult(df, error_count, error_paths)`

**Data Model** (`sqla/model.py`):
- `Scan` table: `id`, `path`, `time`, `blob`, `error_count`, `error_paths`, `size`, `n_children`, `n_desc`
  - Root stats (`size`, `n_children`, `n_desc`) denormalized to avoid parquet reads on scan list
- `ScanProgress` table: real-time tracking of active scans
- Results stored as Parquet in `~/.config/disk-tree/scans/<uuid>.parquet`
- SQLite metadata DB at `~/.config/disk-tree/disk-tree.db`
- Index on `(path, time)` for efficient fresher child queries

**Server API** (`server.py`):
- Flask server on port 5001
- `GET /api/scans` — List all scans (most recent per path, with denormalized stats)
- `GET /api/scan?uri=<path>&depth=N` — Get scan details for a path
  - Uses depth filtering for parquet predicate pushdown
  - Patches in fresher child scans automatically (uses SQLite stats, avoids parquet reads)
  - Falls back to filesystem listing if no scan exists
- `GET /api/s3/buckets` — List S3 buckets with scan stats
- `POST /api/scan/start` — Start a new scan (background thread)
- `GET /api/scans/progress` — Current progress of active scans
- `GET /api/scans/progress/stream` — SSE stream for real-time progress
- `GET /api/compare?uri=<path>&scan1=&scan2=[&recursive=1&budget=N&max_depth=N]` — per-child Δ table; `recursive=1` returns the delta frontier across depths (added/removed dirs not descended). Served as a slice of the pair's persisted **diff index** when one exists (`index.status == 'done'`, complete, ~1 s at the root); otherwise a best-first walk (|Δsize| priority, `budget` expansions) answers and a background build starts — the UI polls and refetches when it lands. Statuses: `added | removed | changed | touched` (same size & count, mtime moved) `| unchanged`; `unchanged: {top, rest}` carries each expanded dir's biggest unchanged children + an aggregate of the rest
- `GET /api/diff/status?scan1=&scan2=` — diff-index build state for a pair (`none | building | done | failed`, counts, seconds)
  - index-served responses accept `min_frac` (default 2e-5): drop rows whose bytes and |Δ| are both under that fraction of the compared subtree (undrawable cells), their parents marked `pruned`; `min_frac=0` serves everything the row budget allows
- `GET /api/filter?uri=<path>&q=<query>&depth=N` — recursive filter with true re-aggregation (matched bytes only, outermost matches, rolled up to a depth-N slice). Slash-free queries match path segments; with a fresh vocab sidecar the query is answered from the index (`indexed: true` in the response)
- `GET /api/filter/stream` — SSE variant: one cumulative snapshot per depth (iterative deepening), final event `done: true`
- `GET /api/histogram?uri=<path>&bins=N&limit=N` — Byte-weighted mtime histogram per child
  - Loads every descendant file row (no depth pushdown possible; path-prefix pushdown prunes sibling subtrees); response cached, UI fetches lazily
- `POST /api/delete` — Delete a file/directory and update scan parquets
- Static file serving for bundled UI (SPA with catch-all routing)

**CLI** (`cli/`):
```bash
disk-tree index [URL]     # Scan directory or s3:// bucket
  -C, --no-cache-read     # Force fresh scan (`index` otherwise returns any cached scan unconditionally)
  -e, --require-external  # Skip (exit 0) if the write target is the boot disk (no opted-in external
                          # volume mounted). For scheduled scans that must land on external media
  -g, --gc                # Garbage collect old scans
  -m, --mean-mtime        # Emit `mtime_mean` (size-weighted mean mtime; feeds the UI age lens)
  -M, --measure-memory    # Track peak memory
  -q, --no-progress       # Suppress the tqdm progress bar (scheduled/redirected runs — keeps logs small)
  -s, --sudo              # Run gfind with sudo (implies `-C`: a cached scan can't be known to be sudo)
  -x, --extents           # Map physical extents → per-dir reclaimable bytes (APFS clones/hardlinks),
                          # written as a `<blob>.reclaim.parquet` sidecar. macOS + local scans only;
                          # exact when the scan root contains the sharing sources (home/full scan),
                          # else an upper bound. `du` shows a `frees` column when the sidecar exists

disk-tree scans           # List cached scans (JSON)

disk-tree diff ARGS       # Per-path Δ table between two scans (URI → two most recent, or two scan ids;
                          # a URI without its own scans falls back to the nearest ancestor's)
  -r, --recursive         # Best-first walk down changed spines → delta frontier across depths
  -b, --budget N          # Recursive mode: max directory expansions (default 100)

disk-tree diff-index A B | PATH… | -a   # Persisted full diff of a scan pair (spec `done/diff-index.md`):
                          # vectorized per-depth outer join, streamed to
                          # `~/.config/disk-tree/diffs/<a>-<b>.parquet` (two 7M-row home scans:
                          # ~16 s, 3.8 GB peak). `disk-tree index` / `sync` build it against the
                          # path's previous scan automatically (`-D` to skip); `-f` rebuilds,
                          # `-g` GCs indexes whose scans are gone (`-n` previews)

disk-tree migrate-row-groups  # Rewrite scan blobs to ≤64K-row parquet row groups (a directory listing
                          # decodes every overlapping row group: ~4 ms vs ~40 ms at 1M rows)

disk-tree filter URI QUERY  # Recursive filter, true re-aggregation: sizes of everything matching QUERY
                            # (`/…/` regex or substring); outermost matches only — never double-counts
                            # Slash-free queries match path segments (basenames); queries with `/` match
                            # full paths. Uses the vocab sidecar automatically when fresh (-B forces brute)

disk-tree vocab URI       # Build the vocab sidecar (`<blob>.vocab.parquet`) for the scan covering URI:
                          # sorted segment names + name→row-group block index. Accelerates segment-local
                          # filter queries; refuses chunked (hybrid `child_scan_id`) blobs

disk-tree histogram URI   # Byte-weighted mtime distribution per child (sparklines; -j for JSON)

disk-tree du URI          # Top-N heaviest children per level, from the freshest covering scan —
                          # `du -d1 | sort -rh` without a filesystem walk (-d depth, -n top-N,
                          # -a to include files, -j for JSON). Sizes are per-path block counts,
                          # so extents shared via APFS clones/hardlinks are charged to every
                          # linking path — see the caveat under Performance.
                          # Shows a `frees` column (true reclaim) when a `-x` reclaim sidecar exists

disk-tree reclaim PATH…   # What deleting PATHs would *actually* free: maps each file's physical
                          # extents (`fcntl(F_LOG2PHYS_EXT)`) and subtracts blocks the surviving
                          # partner roots still reference (`-p` adds one, `-P` drops the
                          # auto-detected uv/pnpm caches). macOS-only. Measured 2026-08-29:
                          # `oa/marin/.venv` reports 2.84 GiB, frees 249 MiB (91% cloned from
                          # `~/.cache/uv`); ~43 s, dominated by walking 756K partner files

disk-tree repos ROOT      # Delete-safety audit of git repos under ROOT (cleanup companion to `du`):
                          # size + dirty/untracked counts + whether every local branch is on a
                          # github/gitlab remote. `recoverable` = clean tree + all branches hosted;
                          # DELETABLE also requires zero untracked. `-r` filters to recoverable,
                          # `-m` sets a size floor (default 200M), `-j` for JSON

disk-tree overcount URI   # How much URI's apparent size overstates physical bytes (APFS clones +
                          # hardlinks). Per top-level child: apparent vs `exclusive`
                          # (ATTR_CMNEXT_PRIVATESIZE — bytes only this subtree holds, i.e. what a
                          # delete frees) vs `shared`. No open() per file; ~32K files/s. macOS-only.
                          # Measured 2026-08-29: `oa/marin` 18.7 GiB apparent → 3.92 GiB exclusive

disk-tree fetch [BUCKET…] # Bulk-list configured buckets → dated raw-listing shards
disk-tree pull [BUCKET…]  # fetch + import as dated scans
disk-tree sync            # pull all configured buckets (cron entrypoint); builds each bucket's
                          # diff index vs its previous scan (`-D` skips)
                          # Config: ~/.config/disk-tree/buckets.yml (see specs/personal-sync.md)

disk-tree migrate         # Backfill SQLite stats from parquet files
disk-tree migrate-depth   # Add depth column to existing parquets

disk-tree-server          # Start Flask API server
```

### Web UI (`ui/`)

Vite + React + TypeScript with Material-UI, TanStack Query, and `@disk-tree/react` widgets
(chart-lib-free DIY SVG: `<Treemap>`, `<TimeSeries>`, `<StalenessScatter>`, `<AgeHistograms>`).

**Key features**:
- Directory listing with size, mtime, n_children, n_desc columns
- Breadcrumb navigation
- Rescan button with real-time progress (SSE)
- Multi-select with keyboard navigation (Shift+arrows)
- Bulk delete for selected items
- Viz panel with a `View:` toggle — Treemap (+ age lens), Staleness scatter, Age histograms
- Treemap drills past the response's depth: unloaded dirs fetch their subtree
  (`<Treemap hasChildren/loadChildren>`), one request per drill, cached per node
- Filter box: display-only dimming by default; the footer label toggles **re-aggregate**
  mode (`/api/filter`) — treemap shows matched bytes only, matched dirs stay drillable
- Pagination and search/filter
- S3 bucket list with treemap visualization

**Key files**:
- `src/App.tsx` — Main layout with routing
- `src/components/ScanList.tsx` — Scans list with pagination
- `src/components/ScanDetails.tsx` — Directory listing component
- `src/components/S3BucketList.tsx` — S3 bucket browser with treemap
- `src/hooks/useScanProgress.ts` — SSE-based progress tracking

## Development

```bash
# Python setup
uv sync
disk-tree index .

# Start API server
disk-tree-server  # http://localhost:5001

# Web UI
cd ui
pnpm install
pnpm dev        # http://localhost:7788
```

## Packaging / Distribution

The package is published to PyPI as `disk-tree` and can include the built web UI:

```bash
# Build with UI included
cd ui && pnpm build   # Creates ui/dist/
uv build              # Wheel includes disk_tree/static/ from ui/dist/

# Install from PyPI
pip install disk-tree
disk-tree-server      # Serves both API and UI on :5001
```

The server auto-detects static assets:
1. Packaged: `disk_tree/static/` (included in wheel via hatch `force-include`)
2. Development: `ui/dist/` (relative to source)

If no UI is found, server prints a message and only serves the API.

## Data Flow

1. `disk-tree index /path` runs `gfind` or `aws s3 ls`
2. Output parsed into DataFrame, aggregated by directory
3. Saved as Parquet, metadata recorded in SQLite
4. API server queries SQLite for scan list
5. `/api/scan?uri=...` loads Parquet, patches fresher child stats
6. UI renders directory listing with real-time updates

## Config

Default paths (override with `DISK_TREE_ROOT`):
- `~/.config/disk-tree/disk-tree.db` — SQLite metadata
- `~/.config/disk-tree/scans/` — Parquet blob storage

**Blob storage is a search path, not a single directory.** The DB stays on the boot disk (small, always mounted); blobs may live anywhere on `config.scan_read_dirs()`, since `Scan.blob` holds a basename. Creating `<volume>/disk-tree/scans` on an external volume opts it in — no config needed — and it becomes the *write* target while mounted; unplugging simply drops it out of the search path. `DISK_TREE_SCAN_DIRS` (colon-separated, priority order) overrides discovery, and an explicit `DISK_TREE_ROOT` disables it entirely so tests and alternate profiles stay self-contained. A candidate under an unmounted `/Volumes/<name>` is never written to — that would silently create the directory on the boot disk.

- `disk-tree scans dirs` — show the write target and every read dir, with blob counts
- `disk-tree scans move [DEST]` — relocate blobs (no DB rewrite). Keeps each path's newest scan **and its chunk closure** on the boot disk by default (`-L` to move those too), so browsing the latest scan doesn't depend on the volume being plugged in

Stream-engine tuning knobs (env, all with measured defaults — see the constants block in `find/aggregate_stream.py`):
- `DISK_TREE_FLUSH_ROWS` — output row-group size (read-side: smaller = less fetched per directory browse, bigger footer)
- `DISK_TREE_PARALLEL_FINALIZE_MIN_ROWS` — below this the finalize stays serial even at `-j N`
- `DISK_TREE_SCAN_BATCH_ROWS` — pass-1 read-batch size (partition balance / seek granularity)

## Tests

```bash
pytest tests/
```

Test fixtures in `tests/data/` (mock gfind/s3 output → expected parquet).

## Current State (www branch)

- CLI indexing works for local + S3
- Parquet caching with depth column for predicate pushdown
- SQLite stats denormalization for fast scan listing
- Flask API with real-time progress (SSE)
- Fresher child scan patching (non-transitive, one level)
- Web UI with directory listing, treemap, multi-select, bulk actions
- S3 bucket list with treemap visualization
- Delete functionality with scan parquet updates
- Migration commands for existing data (`migrate`, `migrate-depth`)
- Static file serving (bundled UI in PyPI wheel)

## Performance

- `/api/scan?uri=/` optimized from ~4s to ~26ms (154x speedup)
- Depth column enables parquet predicate pushdown (only load needed rows)
- `StorageBackend.load(path_prefix=)` pushes a subtree restriction down to parquet row-group pruning / SQL range predicates (rows sorted `(depth, path)`); wired into scan/compare/histogram/path-stats reads — see `specs/diff-and-search.md`
- Denormalized stats avoid parquet reads for scan list and fresher child patching

**Sizes are per-path, not per-extent.** `gfind -printf '%b'` reports blocks allocated to a *path*; APFS clones (reflinks) and hardlinks let several paths share one set of extents, and each linking path is charged the full amount. So a subtree's reported size is an upper bound on what deleting it frees. Measured 2026-08-29: deleting 35 dormant `.venv` dirs totalling 16.9 GiB freed 9 GiB — uv's default macOS link mode is `clone`, so the remainder stayed live in `~/.cache/uv`. Clones are invisible to `stat` (distinct inodes, `nlink == 1`), so inode/link-count bookkeeping catches hardlinks only — and hardlinks are nearly irrelevant here: a census of `$HOME` found `nlink > 1` over-counting just **4.3 GiB of 385.9 GiB (1.1%)**, which is why `%i`/`%n` are *not* indexed. `disk-tree reclaim` (extent intersection, for a custom keep-set) and `disk-tree overcount` (`ATTR_CMNEXT_PRIVATESIZE`, no open per file, for apparent-vs-exclusive) answer the question properly, on demand. The whole-*volume* overcount is free without either — `df` counts shared blocks once, so `apparent_total − df_used` is the number, but only for a scan that covers the entire volume (a subtree's apparent can't be compared to the volume's `df`); for a subtree, `overcount`'s `Σexclusive` is the physical footprint.

## TODOs / Known Issues

- Fresher child patching is not transitive (grandchild patches don't propagate)
- No scheduled/overnight indexing yet
- S3 pagination not explicitly handled (relies on aws cli)
