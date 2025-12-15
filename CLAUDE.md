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
- **TTL-aware caching**: Smart cache invalidation (child inherits parent freshness, etc.)
- **Web UI**: Treemap visualizations for browsing disk usage

## Architecture

### Python Backend (`src/disk_tree/`)

**Indexing** (`find/index.py`):
- Local: `gfind -printf '%y %s %T@ %p\n'` → streams file metadata
- S3: `aws s3 ls --recursive` → parses listing format
- Builds DataFrame with columns: `path`, `size`, `mtime`, `kind`, `parent`, `uri`, `n_desc`, `n_children`
- Aggregates sizes upward through directory tree

**Data Model** (`sqla/model.py`):
- `Scan` table: `id`, `path`, `time`, `blob` (path to parquet file)
- Results stored as Parquet in `~/.config/disk-tree/scans/<uuid>.parquet`
- SQLite metadata DB at `~/.config/disk-tree/disk-tree.db`

**CLI** (`cli/`):
```bash
disk-tree index [URL]     # Scan directory or s3:// bucket
  -C, --no-cache-read     # Force fresh scan
  -g, --gc                # Garbage collect old scans
  -s, --sudo              # Run gfind with sudo
  -m, --measure-memory    # Track peak memory

disk-tree scans           # List cached scans (JSON)
```

### Web UI (`www/`)

Next.js 15 with App Router, Material-UI, Plotly treemaps.

**Routes**:
- `/` — List all scans
- `/file/[[...segments]]` — Browse local filesystem paths
- `/s3/[[...segments]]` — Browse S3 buckets/prefixes

**Key files**:
- `app/db.ts` — SQLite connection (better-sqlite3)
- `src/scan-details-action.ts` — Load parquet data for a path (uses hyparquet)
- `components/scan-details.tsx` — Directory listing with breadcrumbs, size table
- `components/plot.tsx` — Plotly treemap wrapper

## Development

```bash
# Python setup
uv sync
disk-tree index .

# Web UI
cd www
pnpm install
pnpm dev        # http://localhost:3000
```

## Data Flow

1. `disk-tree index /path` runs `gfind` or `aws s3 ls`
2. Output parsed into DataFrame, aggregated by directory
3. Saved as Parquet, metadata recorded in SQLite
4. Web UI queries SQLite for scan list
5. Route loads Parquet via hyparquet, filters to requested path depth

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
- Parquet caching functional
- Web UI shows scan list and directory details
- Plotly treemap component scaffolded but not fully wired
- Refresh button placeholder (doesn't trigger re-scan yet)

## TODOs / Known Issues

- Web UI DB path hardcoded in `www/app/db.ts`
- TTL logic not implemented (always uses cache if exists)
- No scheduled/overnight indexing yet
- Treemap visualization incomplete
- S3 pagination not explicitly handled (relies on aws cli)
