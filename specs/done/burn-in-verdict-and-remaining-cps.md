# Fleet burn-in: 6/6 PASSED — and four more CPs waiting

Follow-up to `specs/done/parallel-finalize-memory.md` (thanks for `4bf2304` + `140ab91` — the size-gate, the spawn-death explanation and the reachable CLI resume all landed on exactly the gaps I flagged).

## Verdict

The 588M-object, six-bucket burn-in against production `tree.json` **passed 6/6: 1088 checks, 0 failed.**

```
marin-eu-west4:    root b=495,795,177,564,686  o=193,950,693  named_children=32 checked
marin-us-central1: root b=857,656,311,476,850  o= 57,360,487  named_children=68 checked
marin-us-central2: root b=1,020,456,922,537,527 o=280,540,423 named_children=55 checked
marin-us-east1:    root b= 80,444,485,357,009  o=  8,309,575  named_children=24 checked
marin-us-east5:    root b=1,101,848,444,622,261 o= 46,057,173 named_children=65 checked
marin-us-west4:    root b=142,038,031,285,776  o=  7,585,086  named_children=19 checked
```

Timings on a 16-core/61GB node at `-j 14`: central2 (298.5M rows) **stream 6:05 + finalize 9:08 ≈ 15 min**, against a 1:19:49 baseline. eu-west4 (369M rows) finalizes in **38:26**. Output verified content-identical to the prior fleet run (same row count, same content hash) — note the *bytes* differ because the new finalize emits exactly `ceil(rows/_FLUSH_ROWS)` row groups (1,139) where the old run had 2,430 irregular ones. **Byte-identity is a `-j 1` vs `-j N` property of one build, not a cross-version one**; the fork's identity test asserts the former only.

## Remaining CPs (all `[CP→disk-tree upstream]`, 252 tests green)

You have through `de1dda3`. Outstanding, in order:

1. **`8cac954` — `_merge_batches`: convert each batch's keys once, not once per split.**
   The merge re-derived `to_numpy(zero_copy_only=False)` on the *remaining tail* after every split, so a batch splitting N times paid N full conversions, each materializing a Python string per row. Splits are the common case: the dir↔file merge alternates once per directory. Held the finalize at ~37K rows/s vs the stream pass's ~830K. Now converted once and consumed through an integer cursor. `test_merge_batches_converts_keys_once_per_batch` locks it with a counting proxy (proxy `slice` too, or post-split conversions go uncounted): **2 conversions vs 198** for 2 batches across 49 splits.

2. **`6f50e5a` — `_shard_rows`: bound the per-source decode buffer.**
   `iter_batches` was called with no `batch_size`, so every open merge source buffered pyarrow's 65536-row default. The merge holds one live batch per *open* source and lazy-open still leaves ~1K open (central2 peaked at 969 of 5,105 run sources; eu-west4 at 987), so one worker hit **13.6GB anon RSS and was OOM-killed 40s into the stream pass** — `BrokenProcessPool`, whole job dead. Rows leave as Python tuples regardless, so `_SHARD_BATCH_ROWS = 1<<13` costs no throughput. Parametrized batch-size-independence test over `{1,2,3,8192}`, since sortedness checks, dirty-key filtering and `[lo,hi)` clipping all run per batch.

3. **`4525ddb` — decouple `_PART_FLUSH_ROWS` from `_FLUSH_ROWS`.**
   `_FLUSH_ROWS` was doing two unrelated jobs: the final file's row-group size (contractual) and the part writers' buffer threshold (pure memory knob). A worker keeps one part writer open per `(depth, kind)` — ~44 on a 22-deep tree — each buffering Python tuples (measured **484 B/row vs 292 B/row for equivalent Arrow**, so ~121 MiB per writer at `_FLUSH_ROWS`, ~5GB/worker worst case, plus ~100 MiB transient per flush). Part row-group size never reaches the published file, so this is free to tune. Test: part flush 7 vs 32768 → byte-identical output.

4. **`d5f2ec3` — `_depth_stream`: coalesce the merge's alternation slices.**
   `_merge_batches` emits one slice per dir↔file alternation — millions of few-row batches per depth. Each became a chunk downstream, so every flush built a table with ~10^5 chunks per column and walked them all. **py-spy: 10/10 samples in the flush block, 0 in the merge.** eu-west4 depth 9 (92M rows) took 46+ min where depth 11 (30M rows, fewer alternations) took 2. `_coalesce` concatenates to ~`batch_rows` batches at one copy per row; both the parallel workers and the serial `emit()` path benefit.

## Open lever: the finalize is Amdahl-bound on one depth

eu-west4's 38:26 is essentially its largest depth. Depth 9 alone holds 92.2M of 369M rows (29 parts); depth 10 holds 81.5M. Everything else finishes in ~2 min and then idles.

The parallelism is already available and unused: within a depth, the stream's `wNNN` parts are **disjoint contiguous keyspace ranges**, so `(depth, worker-range)` groups could each merge independently and be concatenated in order. The only wrinkle is the per-depth boundary part (`{depth:04d}-dir.b.parquet`), which isn't range-restricted — but it is *tiny* (5 rows at central2 depth 6, 3 at depth 7, 1 at depth 15), so splitting it across the range groups is cheap. That would cut the straggler by roughly the worker count and is, I think, the last order-of-magnitude lever in the finalize.

## Read-side finding you may care about (measured, real layer-2 data)

Sweeping row-group size on the 8.27GB central2 output, and timing 40 random directory browses through a byte-counting reader:

| RG rows | KiB fetched per browse | bytes/row | footer (full file) |
|---|---|---|---|
| 8,192 | 1,503 | 29.6 | ~74 MiB |
| 32,768 | 1,872 | 27.4 | ~19 MiB |
| 65,536 | 2,882 | 26.3 | ~9.5 MiB |
| **262,144 (current)** | **8,140** | 25.1 | 2.6 MiB |
| 1,048,576 | 21,070 | 24.9 | 0.7 MiB |

At the current `1<<18`, **one directory browse fetches ~8 MiB to return ~28 rows.** Layer-2 is sorted by `(depth, path)`, so a directory's children are contiguous — this is the scan-browser's whole access pattern. `1<<16` cuts fetched bytes 2.8×, is the *fastest* scan, and costs +4.8% file size.

Also worth knowing: **`write_page_index=True` changed nothing** (1,503 → 1,597 KiB, read counts unchanged) — pyarrow 22 writes a page index but exposes no API to read one and evidently doesn't exploit it on this path. A client-side reader doing its own range GETs might still benefit; untested.

---

## Implementation notes (disk-tree, 2026-08-16)

**All four CPs applied clean** (`f798d5d`, `3cafd55`, `848ead3`, `6088428`), suite green, and the 10M byte-identity smoke is unchanged at `-j 1/4/8` (md5 `9f664083…`, same value as before the CPs — the optimizations moved no bytes). Timings at that scale are flat, as expected: these win where the merge and flush dominate, not on a 10M shallow tree.

Nice side-effect: your `python -c` caveat proved itself immediately — my first probe script hit the new `RuntimeError` and told me exactly what to do.

### The open lever is implemented — and the "disjoint ranges" premise needed one correction

`(depth, worker-range)` groups now merge independently and concatenate in order (`_range_groups`), so a straggler depth splits across workers instead of running alone. On an 8M-row *depth-skewed* fixture (one dominant depth, 8 workers) the finalize went **4.0s → 3.0s**; your eu-west4 shape (one depth = 30% of rows, everything else idle after 2 min of 38) should gain far more.

Two things bit me, both now enforced rather than assumed:

1. **Disjoint keys do not imply disjoint rows.** A directory's path is a *prefix* of its keys, so a dir owned entirely by worker `i` can sort below worker `i`'s `lo` — the same prefix-sibling inversion that makes parts measure unsorted. So the split is *proved* per depth from the parts' own min/max statistics (strictly ascending group bounds or no split), not taken on faith. Cheap: footer stats, parent-side, once per depth.
2. **A worker with no rows at a depth still owns its keyspace slice.** My first version iterated only groups that had parts at that depth, so boundary rows inside an empty worker's range were emitted by nobody: the 10M fixture came out with **5 spanning dirs missing** — identical manifest row count, five fewer rows in the file. Caught by md5, not by any assertion; the manifest count can't see it. Both cases now have unit tests (`test_range_groups_refuses_when_group_paths_overlap`, `test_range_groups_cover_ranges_with_no_parts_at_this_depth`), plus an end-to-end byte-identity test with forced boundaries.

If the split can't be proved safe for a depth, that depth falls back to the whole-depth merge — correctness first, and the stage line reports how many groups were formed (`finalize: 7 depth(s) split into 14 range group(s)`).

### Read-side finding: made tunable, default unchanged

`_FLUSH_ROWS` is now `DISK_TREE_FLUSH_ROWS`, with your measurements in the constant's docstring. The default stays `1<<18`: DT's own server reads *local* files, where the footer is the cost that matters and the page cache absorbs the rest — your 2.8×-fewer-bytes win is a range-request-over-object-storage win, which is your deployment, not DT's default one. Flip it per-deployment rather than per-build.

One caveat on the +4.8%: on a synthetic fixture of near-identical paths the same change cost **+29.6%**, because per-group overhead dominates when paths compress well. Worth measuring per-bucket before standardizing on it.

**Not adopted:** nothing. Byte-identity is documented as a within-build `-j 1` vs `-j N` property, matching your framing — this change moves bytes across versions again (row-group *count* is unchanged; the range split only reorders work, not output).
