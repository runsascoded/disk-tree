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
- `POST /api/delete` — Delete a file/directory and update scan parquets

**CLI** (`cli/`):
```bash
disk-tree index [URL]     # Scan directory or s3:// bucket
  -C, --no-cache-read     # Force fresh scan
  -g, --gc                # Garbage collect old scans
  -s, --sudo              # Run gfind with sudo
  -m, --measure-memory    # Track peak memory

disk-tree scans           # List cached scans (JSON)

disk-tree migrate         # Backfill SQLite stats from parquet files
disk-tree migrate-depth   # Add depth column to existing parquets

disk-tree-server          # Start Flask API server
```

### Web UI (`ui/`)

Vite + React + TypeScript with Material-UI, Plotly treemaps, TanStack Query.

**Key features**:
- Directory listing with size, mtime, n_children, n_desc columns
- Breadcrumb navigation
- Rescan button with real-time progress (SSE)
- Multi-select with keyboard navigation (Shift+arrows)
- Bulk delete for selected items
- Treemap visualization (Plotly, lazy-loaded)
- Pagination and search/filter
- S3 bucket list with treemap visualization

**Key files**:
- `src/App.tsx` — Main layout with routing
- `src/components/ScanList.tsx` — Scans list with pagination
- `src/components/ScanDetails.tsx` — Directory listing component
- `src/components/S3BucketList.tsx` — S3 bucket browser with treemap
- `src/components/LazyPlot.tsx` — Code-split Plotly wrapper
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
pnpm dev        # http://localhost:5180
```

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

## Performance

- `/api/scan?uri=/` optimized from ~4s to ~26ms (154x speedup)
- Depth column enables parquet predicate pushdown (only load needed rows)
- Denormalized stats avoid parquet reads for scan list and fresher child patching

## TODOs / Known Issues

- Fresher child patching is not transitive (grandchild patches don't propagate)
- No scheduled/overnight indexing yet
- S3 pagination not explicitly handled (relies on aws cli)
