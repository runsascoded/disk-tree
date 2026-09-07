# A.3 gate, first round: `marin-us-west4` (2026-09-07 listing) on the mgu node

From the mgu session. `mgu-scale-unification.md` A–E as landed (`bcbe063..01b5e19`), vendored into mgu (`73d57ab`) and run against one real bucket on the 16-core / 61 GB EC2 node, compared row-for-row against mgu's production path index for the same listing (`gcs-usage cascade-a2a`, mgu `efcd9dc`). Bucket: 7.5M objects, 690k `(path, usr)` rows.

Invocation (`~/mgu-gate/run-west4.sh` on the node):

```
disk-tree import -e duckdb -l '<bucket shards>/*.parquet' -b marin-us-west4 -s gcs \
  -d out/db -k 1 -L labels-marin-us-west4.parquet -c usr -p storage_class_id -m -H \
  -i dirs,coarse -O out/tiers -E 24 -r 8192 -S usr -M 40GB -T out/tmp -x 60GiB -t <listing time> -o out/l2
```

Labels: mgu's prefix map exported per bucket by `gcs-usage labels` (`(prefix, usr)`, prefix relative to the bucket, 1318 rows here at depths 1–8, after mgu's depth-12 cap).

## Result: bytes exact; the deltas are placeholder semantics

| column | mismatches over 690,708 shared `(path, usr)` rows |
|---|---|
| `size` vs `b` | 0 |
| `sum_storage_class_id_*` vs `c2..c4` (NULL ≡ 0) | 0 |
| `mtime_mean` vs `wts / wb` | 0 within 2 s (mgu rounds `epoch(created)` per object; DT is exact — DT is the better number) |
| `n_files` vs `o` | 43 rows, Σ(mgu − DT) = 43 |
| rows only mgu has | 188 (186 zero-byte, 1-object dirs + the 2 `a//b` names) |
| rows only DT has | 9 (zero-byte, zero-object dirs) |

Every root slice (per-user and unclaimed) is byte- and count-identical. `-k 1`: 46 partitions (largest 4.8M files), **4:03 wall, 21.1 GB peak RSS** under `-M 40GB`, with labels + pivot + mean mtime + size histogram + two tiers × two sorts.

## Asks

1. **Partition keys must be directories, and small partitions batched.** `-k 2` on this bucket made 10,848 partitions because every *file* at depth 2 (`RealEstate10K/<id>.mp4`, 10k of them) is its own depth-2 prefix; at ~50 ms of SQL per cascade that is an hour of overhead for zero memory benefit (killed at 1,598/10,848 after 10 min). The spec's intent: partition by the depth-K prefixes that have descendants, fold rows at depth ≤ K into the top cascade (the log's "0 shallow files" counted only files shallower than K, not at K), and batch partitions greedily so each cascade holds up to ~N files (N ≈ the memory budget) — the fleet's `datakit/store*` needs K = 3 for size, and at K = 3 the rest of the fleet is thousands of tiny prefixes. Without this the fleet gate can't run: `-k 1` puts the 164M-dir `datakit` in one partition, `-k 3` spends hours on per-file cascades.
2. **Folder placeholders: define the policy, and I'd argue for mgu's.** 229 zero-byte objects named `…/` in this bucket (TensorBoard's `plugins/profile/<ts>/`). DT today: `profile/` becomes the dir row's own marker (not counted in `n_files`) while each child `…/<ts>/` is a *file* named with a trailing slash under `profile` (`n_files` 22, `n_children` 22, no dir row for `…/<ts>`). mgu: every one of them is an object at its dir path (`…/<ts>` is a dir row with `o` = 1; `profile` has `o` = 23). For an object store they are objects — listable, billable per op, and exactly what a sweep deletes — so `n_files` should count them, at the dir path they name (a dir row exists for `…/<ts>`, `n_files` = 1). Then this bucket's `o` matches to the object.
3. **`//` names** (the 2026-08-14 finding, still open): `tokenized/starcoderdata-12f018//.artifact.json`. mgu keeps a dir row `tokenized/starcoderdata-12f018/` (empty last component, 4 B, 1 object); DT puts the object somewhere else. Any deterministic subtree-preserving policy is fine; collapsing `//` → `/` matches user intent.
4. **`--coarse-floor BYTES`** (absolute) beside `-E`: the coarse tier's floor is `2^(round(log2 total_size) − E)` of the *import's* total — one bucket here, 8 MiB — while mgu's floors are fleet-wide (3 PiB → 256 MiB at E=24) and the site's tier planner assumes every bucket's coarse tier shares one floor. The fleet import runs per bucket (`-b`), so the floor has to be passed in. (`total_size` in the KV metadata should then be the fleet's, or a separate `floor_total`.)
5. **Memory at scale (for the A.3 fleet run, once 1 lands):** 21 GB peak for a 4.8M-file partition with all extensions on is ~4.4 KB/file. The fleet is 613M objects / 179M dirs; the size histogram is 82 extra BIGINT columns through the cascade. If peak stays ∝ the largest partition, ~5M-file partitions fit a 64 GB node; the batching in ask 1 is what makes that a knob.

## Landed (DT, 2026-09-07)

1. **Directory keys + batching** — `find/aggregate_duckdb.py`. A partition key is now the depth-K prefix of rows *deeper* than K (`_part_expr(…, min_nseg=k+1)`), so a file at depth K is a top-cascade row, never a one-file partition; keys are packed in sorted order into cascades of ≤ `partition_files` files (`_batch_partitions`; a bigger key stands alone; ≤ 0 = one cascade per key), each batch one contiguous `name` range `[first/, last0)` plus the exact `part IN (…)` predicate. Default `DEFAULT_PARTITION_FILES = 4,000,000` (from this round's 4.8M files ↔ 21 GB); `disk-tree import -k K -P N`. Stats: `partitions` = cascades, new `partition_keys`. Log line: `partition depth K: <keys> dir keys → <cascades> cascades (largest key …, batch ≤ N files), <n> files at depth ≤ K`. Output byte-identical for every `(K, N)` (tests at K = 1/2/3/6 one-key-per-cascade, plus K=1 N=5 → 6 cascades over 10 keys, plus the default budget → 1 cascade).
2. **Placeholders: mgu's policy adopted.** A listing name ending in `/` is an object at the directory it names: it becomes that dir's *own* row (`kind = 'dir'`, at the stripped path, carrying its size/mtime) and counts in `n_files` there (`profile/<ts>/` → dir row `profile/<ts>` with `n_files = 1`; `profile` sums to 23 for 22 + itself); it is never a file child, and a placeholder pre-empts the synthesized dir row (`_dir_rows_insert` skips paths already present as dirs — the old behavior emitted *both* a file row and a dir row at the same path, and counted the parent's `n_children` twice). Mechanism: an `obj` column on every input row (1 per listed object, 0 per synthesized dir) feeds `n_files` in the cascade seed and in `dirs0`. duckdb engine only — the pandas/stream engines still see `x/` as a file named `x` (not what mgu runs; flagged, not fixed).
3. **`//` names**: DT's policy stands — `a//b` collapses to `a/b` (the path the user meant, subtree-preserving, deterministic); mgu's empty-component dir row is the other consistent choice, and the a2a should map `tokenized/x/` ↔ `tokenized/x` and expect the object under it. Not changed.
4. **`--coarse-floor BYTES`** (`-F`): `write_tiers(coarse_floor_bytes=…)` pins the coarse floor; KV metadata gains `floor_source = explicit | derived`, `total_size` stays this import's, `coarse_exp` is recorded as given. Pass `coarse_floor(fleet_total)` (the same function, exported) per bucket.
5. **Memory**: with 1, peak ∝ the largest cascade ≈ `partition_files × 4.4 KB` at full extensions — `-P 4000000` targets ~18–21 GB; a 64 GB node can take `-P 10000000`. Untested at that scale here; the fleet run is the measurement.

## Next

mgu will run the six-bucket gate on GCP Batch (the daily job's image, 250 GiB, NVMe spill) as soon as ask 1 lands, with `--coarse-floor` from the fleet total if 4 does; the a2a then covers every bucket, and `--partition-depth` peak RSS vs. `webdata`'s (141.7 GB at 100 GB `memory_limit` on 2026-09-06) is the number that decides whether `viz.py` goes.
