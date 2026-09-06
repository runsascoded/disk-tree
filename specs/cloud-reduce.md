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

## Reduce runner (step 3 — open)

Recommendation: a GitHub Actions `workflow_dispatch` job (7 GB RAM covers a
7M-row reduce's ~3.8 GB peak; zero infra; the same plain `disk-tree reduce`
invocation runs on a VM / Batch later). Inputs: capture URL, target URL.
Needs R2 credentials as repo secrets — Ryan's action. Alternatives if
preferred: GCP Batch (marin's choice), any VM.

## `apps/cfn/` (step 4 — open)

SPA + Pages Functions serving the reduced blob from R2 (Phase 1's read path;
marin's `data/[[path]].ts` shape), deployed to `*.pages.dev`. Confirm before
deploying. Then CIC the whole chain: laptop → R2 → cloud reduce → `pages.dev`.

## Not in scope

The metadata routes (`snapshots.json` manifest / D1) — still as scoped in
`done/remote-scan-targets.md`; the reduce records into the local SQLite DB
today. Root `/` captures: `uri` for a `/` root would be `//path` through the
engines' `f'{scan_root}/{path}'`; verify before relying on a full-disk capture.
