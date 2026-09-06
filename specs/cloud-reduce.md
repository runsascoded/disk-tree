# Cloud reduce — scan a ~full laptop disk with zero local footprint

Phase 2 of `done/remote-scan-targets.md`. Phase 1 put the *persistent* blob
in the cloud and proved serve-from-cloud; the scan itself still ran on the
laptop, so the crisis case ("disk ~full, need to scan *it*") still needed local
scratch for the reduce and for the blob before upload. This phase removes
that: the pipeline splits so the only step that touches the full machine
barely writes to it, and everything downstream runs where there *is* disk.

    capture (laptop)         gfind → layer-1 listing shards streamed to R2
                             bounded memory, zero local disk
    reduce (anywhere)        shards → layer-2 scan blob (+ Scan row) → R2
                             a CI runner / VM / the laptop once its SSD is back
    serve (CFW / Flask)      range reads over the blob — Phase 1, unchanged

This is the same offline job marin's publish side runs and the one `apps/cfn/`
scaffolds; the only difference is the capture *source* (a laptop's listing vs.
bucket listings). So the reduce is built input-agnostic and the "local scan,
stored and served by the cloud" story becomes `apps/cfn/`'s headline demo,
with the R2 fleet union as a second input later — not a separate build.

## `disk-tree capture PATH --to URL`

Walks with the same `gfind` / excludes / sudo / error handling as `index`
(`backend.list`), but instead of building a frame emits **canonical layer-1
listing shards** — exactly what the bucket listers write:
`bucket, name, size_bytes, created, storage_class_id`, with `bucket` = the
scan root and `name` = the path relative to it. One batch (`-n`, default
200K rows) is buffered at a time, written through `blobfs` (so `r2://` works;
the listers' own `_write_shard` only knows fsspec's registered schemes), then
freed. Nothing is written locally.

Layout: `<to>/<host>/<root slug>/<stamp>/shard-NNNNN.parquet` +
`_SUCCESS.json` (`format, version, scheme='file', root, host, time, n_rows,
n_shards, error_count, error_paths`). Prints the capture dir.

**Files only.** Every engine treats a listing row as a file and derives
directories from name prefixes, so directory rows would read as phantom files.
Consequences, accepted: empty directories are not captured; directory
`mtime`s are derived, not recorded. No bytes are lost — APFS directories hold
0 blocks (measured: `%b` = 0 for every dir under `~/c`). Symlinks are captured
as files with their own block size, as `index` does.

**Unsorted.** Shards are in walk order; sorting 7M names is exactly the
scratch-hungry step the crisis can't afford. The DuckDB engine handles
unsorted input on the reduce side, where the disk is.

## `disk-tree reduce CAPTURE [--to URL]`

Reads the manifest, pulls the shards to a local work dir if the capture is
remote (the engines glob local paths, and this machine has disk — that's the
point), and runs `import_bucket` with the capture's root/time, `-e duckdb` by
default. `--to` uploads the blob (Phase 1's `set_write_target`); the Scan row
carries the capture's error count/paths; the diff index against the path's
previous scan is built like `index` does (`-D` skips).

`Scan.path` must be byte-identical to `index`'s: the engines' `scan_root =
f'{scheme}://{bucket}'` now goes through `backends.url.canonical`, which
collapses a `file` root to the bare path (`/Users/ryan`) and leaves every
cloud scheme unchanged. `import` also gained `-w/--to` (`-t` is `--time`
there).

Equivalence, tested: for the same tree, `capture → reduce` (pandas and duckdb)
reproduces `index`'s layer-2 rows (`path, kind, parent, uri, size,
n_children, depth`) exactly, minus the empty directory.

## Landed — capture + reduce (2026-09-06)

`cli/capture.py` (`capture`, `reduce`), `canonical()` scan roots in all three
engines + `import_bucket`, `import -w/--to`; 7 offline tests
(`tests/test_capture.py`) including the `index` equivalence for both engines;
full suite 499 green. Real chain against R2 (`file-tree-demo/disk-tree/`,
isolated root): `capture specs/` → shard + manifest in R2; `reduce` fetched the
shard *from* R2, aggregated (duckdb), pushed the blob *to* R2; the local root
held only the 28 K SQLite DB — **no `scans/` dir at all**; `du` read the scan
back from R2. Test objects deleted after.

What the run exposed for step 3: a runner's `reduce` records the Scan row in
*its* throwaway DB, so the laptop never learns about the blob. Hence the next
piece: `reduce` emits `<blob>.scan.json` beside a remote blob, and
`disk-tree scans register URL` imports it — the manifest route from
`done/remote-scan-targets.md`, made concrete.

## Reduce runner + the scan manifest (step 3 — built, awaiting secrets + push)

**Manifest.** `reduce --to <url>` and `index --to <url>` write
`<blob>.scan.json` beside a remote blob (`scan_manifest.py`: `format,
version, time, path, blob, size, n_children, n_desc, mtime, error_count,
error_paths`). `disk-tree scans register SRC` (one manifest, or a dir/URL of
them) inserts the Scan rows into the local DB, idempotently (same path+blob
→ skipped), and notes when the blobs' dir isn't on the search path. A local
blob gets no manifest — the local DB already has the row. Tested: a scan
reduced under one root, registered and `du`-read under a fresh one.

**Runner.** `.github/workflows/reduce.yml`, `workflow_dispatch` with inputs
`capture` (URL), `to` (default `r2://file-tree-demo/disk-tree/scans`),
`engine`, `memory_limit` (5 GB; the runner has ~7 GB RAM, 14 GB disk — a
7M-row reduce peaks ~3.8 GB). `uv sync --extra r2`, then the plain
`disk-tree reduce -D -e … -M … -t <to> <capture>` — the same invocation runs
on a VM / Batch later. Credentials from repo **secrets `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY`** and **variable `R2_ENDPOINT_URL`** (s3fs reads the
standard `AWS_*` env; `AWS_DEFAULT_REGION=auto` for R2).

The crisis loop, end to end:

    laptop   disk-tree capture ~ -t r2://bucket/disk-tree/captures     # prints the capture URL
    GitHub   Reduce workflow: capture=<that URL>, to=r2://bucket/disk-tree/scans
    laptop   disk-tree scans register r2://bucket/disk-tree/scans       # DISK_TREE_SCAN_DIRS includes it
    laptop   disk-tree du ~  /  disk-tree-server                        # served from the R2 blob

Verified against R2 with the laptop standing in for the runner (2026-09-06):
capture → R2; a "runner" root reduced *from* R2, leaving blob (3.8 KB) +
`.scan.json` (313 B) in R2; a *fresh* "laptop" root `scans register`ed it
from R2 and `du` read the tree back. Each root held only its 28 K SQLite DB.
Test objects deleted after.

Two things only Ryan can do before this runs for real: add the secrets /
variable, and lift the push hold (the workflow exists only once it's on
GitHub). Then: dispatch once against a real capture, CIC the served result.

## `apps/cfn/` (step 4 — open)

SPA + Pages Functions serving the reduced blob from R2 (Phase 1's read path;
marin's `data/[[path]].ts` shape), deployed to `*.pages.dev`. Confirm before
deploying. Then CIC the whole chain: laptop → R2 → cloud reduce → `pages.dev`.

## Not in scope

The metadata routes (`snapshots.json` manifest / D1) — still as scoped in
`done/remote-scan-targets.md`; the reduce records into the local SQLite DB
today. Root `/` captures: `uri` for a `/` root would be `//path` through the
engines' `f'{scan_root}/{path}'`; verify before relying on a full-disk capture.
