# mgu → DT unification at fleet scale: layer-2 as the index tiers, sliced cascade, hour-grained access, as-of

Written 2026-09-06 from the mgu session (Marin GCS Usage, `~/c/oa/marin-gcs-usage`; spec workflow — this session asks, the DT session implements, ideally landing each item as its own commit so mgu can CP or adopt). Context and the mgu-side design: `marin-gcs-usage/specs/view-serving.md` (the "from scratch" serving design) and `specs/webdata-memory.md` (why the daily job OOMs at 124 GB).

## Why

mgu diverged from DT's aggregation in 2026-08: at the time layer-2 lacked class pivots and mean mtime, so mgu kept its own DuckDB SQL (`viz.py`: `dir_stats` → `dir_attr` → `ptu`, a per-`(path, team, usr)` descendant-inclusive rollup) and, to keep that rollup small enough, indexed **dirs only** — files exist in mgu's index as a count. Both of the original blockers have since shipped in DT (`--pivot-sum`, `--mean-mtime`), DT's cascade is level-wise (which is what mgu needs for memory), and DT's layer-2 has file rows. The divergence now costs: a 220M-row dirs-only index that can't address files, an aggregation that fills a 128 GB node with hash-agg state, and two implementations of the same rollup. Direction (Ryan, 2026-09-06): **adapt upstream to mgu's scale and adapt mgu to consume upstream — best of both, one implementation.** mgu keeps only its overlays (attribution identities, ledger/marks, storage-class pricing, the CF site).

Fleet numbers to design against (2026-09-05): 613M objects, 179M dirs (96 % of them single-object; `datakit/store*` alone is 164M dirs), 3 PiB, 6 buckets, listing ≈ 10.5 GB of parquet. Target: work at 10× that.

## Asks, in the order mgu needs them

### A. Fleet-scale cascade (needed first; gates everything below)

`find/aggregate_duckdb.py` builds the level tables in an **in-memory** DuckDB (`duckdb.connect()` + `memory_limit` + `temp_directory`). Intermediates spill, but base tables in an in-memory database don't page out, so at 613M input rows the level tables themselves must fit RAM.

1. **File-backed database option**: `connect(<path on the spill disk>)` so level tables live in the buffer pool and page to disk under `memory_limit`. Expose as `--db <path>` (default: temp file under `temp_dir`). Expected: peak RSS ≈ `memory_limit`, wall time up somewhat on the spill phases (local NVMe on the Batch node makes that cheap).
2. **Prefix-partitioned cascade** (`--partition-depth k`, optional): a dir and all its descendants share their depth-k prefix, so run the cascade per distinct depth-k prefix (memory ∝ that partition's rows), then cascade the k-level stubs to the root. No shuffle if the input listing shards are already prefix-contiguous (mgu's `bulk-list` streams are). Partitions can run sequentially on one node or as separate tasks — this is the knob that makes RAM / time / $ a user choice at 10× scale.
3. **Gate**: mgu's 2026-09-05 listing on the `mgu` EC2 node (61 GB RAM, 16 cores, needs disk) or a Batch node — report peak RSS, wall time, and row counts per level. The 9/4 mgu run for reference: object-level aggs 21 min, dir rollup 5 min on an n2-highmem-16 with DuckDB capped at 86 GB.

### B. Attribution slices as cascade group keys

mgu's rollup is per `(path, team, usr)`, not per path: every path row is split into attribution slices (team = employer/communal pool, usr = the person) so lenses and per-user totals are exact. The per-**dir** labels come from a deepest-prefix-wins join against an attribution table (`prefix → team, usr`, ~10⁴ prefixes) — mgu-side data, DT-side mechanism:

- `--label <parquet> --label-cols team,usr`: join labels onto leaf rows by deepest matching prefix (the join itself is generic: longest-prefix match of `path` against the label table's `prefix`), then carry the label columns as **additional group keys through the cascade**, so layer-2 rows are `(path, team, usr, size, n_files, pivots, mtime_wsum, …)` — a path appears once per distinct (team, usr) under it. Unlabeled leaves get NULL labels (mgu maps that to `unattributed`).
- Without `--label` nothing changes (one row per path, as today).

This is the whole of mgu's `dir_attr` + `ptu` — once DT does it, `viz.py`'s aggregation is a CLI invocation plus the JSON overlays.

### C. Layer-2 written as index tiers

Consumers read layer-2 over HTTP range requests (mgu's Pages Functions; DT's own server could too), so the output wants to be **sorted parquet with small row groups**, in tiers:

| tier | rows | sort | floor |
|---|---|---|---|
| `dirs` | every dir row (× label slices) | `(depth, path)` | none |
| `objects` | every file row | `(path)` (pre-order; one range per subtree) | none |
| `coarse` | dir rows whose subtree `size ≥ F` | `(depth, path)` | `F = 2^(round(log2 total_size) − E)`, `E` default 24 (256 MiB at 3 PiB; ~2M of 220M paths) |

Options: `--tiers dirs,objects,coarse`, `--coarse-exp E`, `--row-group-rows 8192`, and `--sort-variant <cols>` to emit extra sorted copies (mgu needs `(usr, depth, path)` and `(team, depth, path)` of the dirs and coarse tiers — parquet has no secondary index, a sorted copy is one). Every row keeps `kind` ('dir' | 'file'). The floor is a tier boundary, never a loss: every kept row's sums are exact; the planner picks the coarsest tier whose floor is under the query's pixel threshold.

Also: `bulk-list` should record `started` / `finished` timestamps in `_SUCCESS.json` (today: `bucket`, `prefix`, `objects` only) — the scan's as-of instant for D below.

### D. Access plane: hour grain, as-of, per-scan state

`access/aggregate.py` rolls layer-1a up per `(bucket, path, UTC day, op)`. mgu needs to cut reads at the listing's as-of instant, and a day-grained row can't be split, so today's "as-of" is coarse by up to a day.

1. **Hour grain**: `date_trunc('hour', ts)` (column `hour TIMESTAMP` replacing `day DATE`); `last_ts` stays exact. Row count grows only for dirs read across many hours.
2. **`--as-of <ts>`** on the aggregate/consumer path: rows with `hour < as_of`. mgu re-aggregates its retained raw shards (`access/raw/`, lossless per-request rows) once so history follows the same rule.
3. **Per-scan state** (`dt access state --as-of T --live <dirs tier> --prev <state>`): one row per `(bucket, dir)` that exists in the scan's listing — `last_ts, read_ops, read_bytes` (+ per-op sums) — built incrementally from the previous state (semi-joined to the live dirs) plus the shards in `[prev, T)`. Reads that belong to since-deleted dirs drop out; cost is O(live dirs) + O(new rows), not O(everything since logging began). Measured on mgu 2026-09-05: 64.3M dirs ever read, of which 52.0M (81 %) no longer exist.
4. The state joins into the cascade as a **max-aggregated column** (`--max-col last_ts` from a side table keyed by path): subtree-max last-read per row, which mgu emits as `a`.

### E. Size histogram column (cheap once A–C exist)

Per path, a log2 histogram of descendant files by size — counts and bytes per bin (≈40 bins). It is additive through the cascade (children are disjoint sets), so it costs one vector-sum per level. `histogram.py` does the same idea for byte-weighted mtime per child at query time; this is the size axis, materialized. mgu wants it for an on-page "what sizes are the files under this path" chart that follows drills and scopes.

## Non-goals

Attribution *content* (identities, rules, W&B mining), the marks ledger and its fold, storage-class pricing, the CF site — all stay mgu-side. Naming: DT columns stay long-form (`read_ops`, not `ro`); mgu's wire abbreviations are its serializer's business.

## Acceptance

- A: the 2026-09-05 mgu listing cascades to completion under a fixed `memory_limit` well below input size, peak RSS reported; with `--partition-depth`, peak RSS ∝ largest partition.
- B: for a labeled leaf set, Σ over slices at every path equals the unlabeled path row; deepest-prefix semantics match mgu's `dir_attr` (mgu will diff its 2026-09-04 `ptu` against DT's output on the same inputs).
- C: tiers are sorted as declared, row groups ≤ N rows, `coarse` ⊂ `dirs` with exact sums; `_SUCCESS.json` carries timestamps.
- D: hour-grained shards; `--as-of` excludes exactly the rows at/after the instant; state file row count = live read dirs.
- E: histogram sums per level equal a direct computation over the listing for a sample of paths.
