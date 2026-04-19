# Fast File Tree Indexing

Why `gfind` is fast, why pure-Python walkers can't match it, and what a Rust-based indexer could look like.

## Context

Current approach: `gfind <root> -printf '%y %b %T@ %p\0'`, null-terminated records, parsed in Python with a 64K-buffer `read` loop. Parse → `pd.DataFrame` → aggregate → parquet.

Empirically this beats anything we built with `os.walk` / `os.scandir` + threads by ~OoM on large trees. This document explains why, and sketches a path to match or beat `gfind` directly.

## Why `find` is fast

1. **`getdents64(2)` batches directory entries** — one syscall returns many entries (typically hundreds to thousands, bounded by buffer size, often 32KB). Python's `os.scandir` uses it too, but wraps each entry in a `DirEntry` Python object.
2. **Per-entry stat is avoided when possible** — `gfind`'s `%y` (type) comes from `d_type` in the directory entry itself (no `stat` call), on filesystems that populate `d_type` (APFS, ext4, btrfs yes; some NFS and older FSes no). `%b` and `%T@` do need a `stat`, but `find` issues `fstatat` with the directory fd (no path re-resolution).
3. **Tight C loop, zero heap churn** — no Python object allocation per entry, no GIL, no refcount work. `find` is essentially `while (entry = getdents()) { stat(entry); printf(entry); }` with kernel and stdlib doing the heavy lifting.
4. **Streaming output** — `find` doesn't buffer the whole tree; it writes as it walks. Readers can begin parsing before the walk completes.
5. **Kernel page-cache warmup** — repeated scans are ≈free; the dentry and inode caches hold the metadata.

Benchmark target: `gfind ~ -printf '%y %b %T@ %p\0' | wc -c` on a warm cache ≈ ~15-25K entries/sec on an M-series Mac with APFS (varies with dir fanout).

## Why pure-Python walkers can't match it

`os.scandir`:

- Per-entry Python `DirEntry` object (allocation + refcount)
- `entry.stat()` re-enters the GIL and allocates an `os.stat_result`
- Function-call overhead per entry

`os.walk`:

- Builds lists of names per directory (allocation)
- Adds its own error-handling layer

Threading helps (I/O-bound, mostly), but Python threads still pay GIL cost on every `stat`, and the orchestration overhead (queues, locks) eats the parallelism gain for deep, fanned trees.

## Alternatives

### Keep shelling to `gfind` (status quo)

- **Pros**: zero new dependencies, known fast, works over SSH unchanged
- **Cons**: subprocess/pipe parse overhead; format is coupled to `gfind`'s `-printf` flags; can't easily embed into tests; portability (BSD `find` on bare macOS doesn't support `%b %T@`)

### Rust inner loop via PyO3

Libraries:

- **[`walkdir`]** — single-threaded, tight recursive walker
- **[`jwalk`]** — parallel walker built on rayon; on SSDs often beats GNU `find`
- **[`fd`]** (the binary) — uses `ignore` + `walkdir`; handy reference

Shape:

```rust
// pseudo-code of the PyO3 surface
#[pyfunction]
fn index_local(path: &str, threads: usize) -> PyResult<RecordBatch> {
    // jwalk::WalkDir::new(path).parallelism(...).into_iter()
    //   .filter_map(|e| Entry { path, size, mtime, kind })
    //   .collect_into_arrow_batch()
}
```

**Pros**:
- Likely ≥`gfind` speed on SSD (jwalk parallelizes across dirs)
- No subprocess / no text parsing — direct Arrow/Parquet writeback
- Testable / embeddable; no `gfind` install requirement on macOS
- Can emit records incrementally as an Arrow `RecordBatchReader` for backpressure

**Cons**:
- New build step (PyO3 + `maturin`), cross-platform wheel matrix
- Doesn't help SSH (still need remote `gfind` / `find`); would need a Rust binary shipped to the remote too if we wanted a unified fast path
- Maintenance burden: small, but real

### `fd` as drop-in

Call `fd` binary instead of `gfind`. Similar perf; friendlier flag surface. Same portability concern (user install).

## Where the bottleneck shifts

If walking is 10× faster, the next bottleneck becomes:

- DataFrame construction (Python allocation of column arrays)
- Aggregation loop (iterative groupby by depth — currently O(depth × rows) passes)
- Parquet write (I/O bound; compression time non-trivial)

A Rust indexer that writes **directly to Parquet** (or to an Arrow IPC stream) would skip DataFrame construction entirely. The aggregation can either happen in Rust (radix-sort by path depth, single pass) or stay in Python/DuckDB on the materialized Arrow batches.

## Streaming into storage

Today: read all entries → build DataFrame → aggregate → write once.

Proposal: emit fixed-size Arrow batches (e.g. 100K rows) as the walker runs. Two consumers:

1. **Writer** appends batches to the storage backend (append-friendly: `parquet` row groups; `duckdb` `COPY`; `sqlite` bulk insert)
2. **Aggregator** maintains running per-directory rollups in a hashmap (path → {size, n_desc, n_children, mtime_max}); finalized on stream close

Benefit: bounded memory for arbitrarily large scans; scans can be resumed / checkpointed.

## Progress & errors

Current: callback every N seconds with `(items_found, items_per_sec, error_count)`; `ErrorCollector` stores up to 100 permission-denied paths (parsed from `gfind` stderr regex).

Rust path: callback wired via `py.allow_threads` + periodic GIL re-entry. Errors flow through the same `ErrorCollector`.

## Benchmark harness

Propose `bench/walkers.py`:

- Inputs: `~`, `/usr`, a synthetic fixture (`mkfixture.py` generates a deterministic tree of N files across M dirs)
- Walkers: `gfind`, `fd`, `os.scandir`, `jwalk` (if built), `find` (BSD fallback on macOS)
- Metrics: wall time, peak RSS, items/sec, warm vs cold cache
- Output: table + flamegraph hooks

Makes "is a Rust impl worth it?" a measurable question on each user's machine.

## Decision

**Short term**: keep shelling to `gfind`; add the benchmark harness; measure on real user trees.

**If numbers justify**: build a `disk-tree-walker` Rust crate with `jwalk`, expose via PyO3. Keep shell-out as the SSH fallback indefinitely; Rust path only for local (and, later, a `disk-tree-walker` binary copied to the remote via `scp` for SSH).

## Open questions

- Are we bottlenecked on walk, parse, DataFrame, or parquet write today? (Answer requires the benchmark harness.)
- For SSH, is piping gfind output across the network the bottleneck, or is remote walk itself? Would `ssh host 'gfind ... | zstd'` help?
- Can we keep `%b` (512-byte blocks) semantics in a Rust port? `std::fs::Metadata` on Unix gives `st_blocks` via `MetadataExt::blocks()` — yes.
- Should n_desc / n_children be computed in the walker (requires tree traversal order) or post-hoc (current approach: DataFrame groupby)?

[`walkdir`]: https://docs.rs/walkdir
[`jwalk`]: https://docs.rs/jwalk
[`fd`]: https://github.com/sharkdp/fd
