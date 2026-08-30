# Reflink-aware sizing: what deleting a directory *actually* frees

## Problem

disk-tree's sizes (from `gfind -printf '%b'`) sum per-path allocated blocks. APFS
**clones** (reflinks) and hardlinks let many paths share one set of physical
blocks, and each linking path is charged the full amount. So a subtree's reported
size is an *upper bound* on what deleting it frees — routinely off by a lot: the
uv/pnpm caches clone into project trees, so `oa/marin` reports 18.7 GiB apparent
but holds 3.92 GiB exclusively; six marin venvs report 14.3 GiB apparent, of
which deleting all six frees 278 MiB.

We already ship two on-demand answers (see `extents.py`):

- **`reclaim PATH…`** — maps physical extents (`fcntl(F_LOG2PHYS_EXT)`, needs an
  `open()` per file) and subtracts blocks a chosen *partner* set (the caches)
  still references. Exact for a fixed keep-set; can't roll up arbitrary subtrees.
- **`overcount URI`** — per-file *private* bytes via
  `getattrlist(ATTR_CMNEXT_PRIVATESIZE)` (0x8; no `open()`, ~32K files/s).
  `private` = bytes shared with **no other file on the volume**.

Neither answers the real question at arbitrary granularity: **"how much would
`rm -rf D` free, for any D?"** `private` is context-independent (vs the whole
volume) and therefore *undercounts* a subtree that shares blocks internally —
measured: `rm -rf wt/` frees 1.80 GiB, but `Σprivate` under wt/ is 1.08 GiB; the
0.72 GiB gap is blocks cloned *between* the 5 worktrees, freed only when the whole
subtree goes.

## Why there's no cheap shortcut

- `statfs()` (what `df` reads) is an O(1) container-wide free-block counter APFS
  maintains — accurate for the whole volume (`apparent_total − df_used` is the
  volume overcount, but only for a scan covering the entire volume), with **no
  per-subtree equivalent**.
- APFS's internal physical-extent **refcount tree** (what `fsck_apfs` walks) has
  no public userspace API. `ATTR_CMNEXT_PRIVATESIZE` is the kernel exposing one
  slice of it per file; physical extent offsets come only from an fd via
  `F_LOG2PHYS_EXT`. So exact per-subtree reclaim requires reading every file's
  extents — an `open()` per file.

## Model: attribute each extent to the LCA of its referencers

An extent is freed by `rm -rf D` **iff every file referencing it lives under D**
— equivalently, iff the lowest common ancestor (LCA) of all its referencing
files' directories is at or below D. So:

1. Walk the tree; for each file, read its physical extents.
2. Maintain `extent → (lca_dir, length)`, folding each new referencer's directory
   into the running LCA.
3. Attribute each extent's `length` to its `lca_dir`. Because the value lives at
   the LCA, it rolls up correctly: **reclaim(D) = Σ extents whose LCA ∈ subtree(D)**.

This is btrfs's *exclusive* accounting generalized to arbitrary subtree
granularity, computed in one pass. Validated on `oa/marin` (prototype):
`rm -rf wt/` → 1.80 GiB, `.git/` → 1.22, `discord/` → 1.17 — the numbers a user
actually wants before deleting.

### Correctness notes

- **Hardlinks**: dedupe by inode, but the LCA must fold in *all* the inode's
  paths (a hardlink under two dirs raises the LCA). The prototype recorded only
  the first path — the real impl needs inode → all-paths. Minor in practice
  (hardlink over-count is ~1.1% of `$HOME`), but required for correctness.
- **Compressed / zero-length files**: no extents (decmpfs stores data in an
  xattr); count their `st_blocks` as exclusive (they're never shared).

## Cost (measured, this APFS, 2026-08-29)

- Extent mapping: **~23K files/s** (open + `F_LOG2PHYS_EXT`); 430K files in 18.5s.
  Whole `$HOME` (7.3M files) ≈ **5 min**.
- Extent map: 177K distinct extents for 430K marin files; **~3 MiB packed**
  (16 B/extent). Whole home ≈ 5–8M extents, ~80–130 MiB transient; the stored
  artifact is just one rolled-up number per directory.
- Contrast: the normal scan is `gfind` (readdir+lstat, **no open**) at 30–48K/s.
  The open-per-file mapping is fundamentally heavier — hence **opt-in** (`-x`).

## Proposed shape

1. **`index -x/--extents`**: after the normal scan, run the mapping pass and store
   a per-directory `reclaimable` (LCA-attributed exclusive bytes) column /
   sidecar alongside `size`. Off by default.
2. **Server/UI**: when the column is present, show `reclaimable` next to apparent
   size (a second bar, or a "frees N" badge), so the treemap answers "what do I
   get back" directly.
3. **`overcount`/`reclaim`** stay as the no-index, on-demand answers.

## Enabling perf work: a `getattrlistbulk` walker

The scan ceiling is the APFS per-volume metadata lock: ~42K/s single-thread, and
**threads do not stack** (measured 4/8/16 ≈ 1×). The one lever above it is
`getattrlistbulk` — one syscall returns a whole directory's entries *with* their
attributes, amortizing the per-file path resolution that caps both `gfind` and
`getattrlist`. `gfind` does not use it (findutils walks with `fts`), so a
purpose-built walker (Rust/PyO3 or C extension — **no kext**, ordinary syscalls)
could:

1. batch directory reads (the speedup),
2. capture `st_blocks` **and** `ATTR_CMNEXT_PRIVATESIZE` in the same pass
   (dedup-aware sizes with no extra walk),
3. emit binary / aggregate in-process (removes the gfind→text→Python RT),

folding three current costs into one. Forking `gfind` is the wrong base (GPLv3;
its `fts` core is exactly what we'd replace). Open question before committing to
it: nail the `getattrlistbulk` buffer layout (variable, keyed by
`ATTR_CMN_RETURNED_ATTRS`) and measure whether it clears 42K/s by the hoped 50–
100%. Note `getattrlistbulk` gives `private` cheaply but **not** physical extents
— the exact per-subtree `reclaimable` still needs the `open()`+`F_LOG2PHYS_EXT`
pass; bulk accelerates the *sizing* scan, not the extent index.

## Status

- Shipped: `reclaim`, `overcount`, `extents.py` primitives, this measurement set.
- Shipped: **`index -x`** → `reclaimable_by_dir` (extent→LCA rollup) written as a
  `<blob>.reclaim.parquet` sidecar; `du` joins it as a `frees` column; `scans
  move` keeps sidecars beside their blobs. Coverage caveat is enforced in docs +
  tests: exact only when the scan root contains the sharing partners, else an
  upper bound (a narrow subtree can't see clones from `~/.cache/uv`).
- Next: surface `frees` in the server/UI treemap; the `getattrlistbulk` walker.
