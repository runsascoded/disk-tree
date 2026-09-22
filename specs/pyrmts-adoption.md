# pyrmts adoption: one engine for age / over-time / diff, plus redundant-scan compression

Status: **spec, no code** (2026-09-22). Direction set by the user ("we should probably adopt here, especially for our public demo where scans will be largely redundant"). Design inputs from three pyrmts-session heads-ups (2026-09-22) are folded in; pyrmts has confirmed the plan shape and will review this spec — particularly §3 (the union-path-index → `Pyramid` mapping) and §3.2 (per-location dataset scoping).

Supersedes the deferred age-index item in [`union-of-roots.md`](./union-of-roots.md) (phase 7) and resolves it: cw's "age-index redesign" *became* pyrmts.

## 1. Why

disk-tree's `cloud` base carries **three ad-hoc structures** that pyrmts replaces with one cross-consumer engine:

| today (ad-hoc, base) | pyrmts equivalent |
|---|---|
| `write_age_pyramid` (age tiers, `age-pyramid-<bin>.parquet`) | the tier pyramid — age = created-date `binCol` |
| the over-time index (`_lib/overTime.ts`, `overtime.py`, `d249b55`, US'd from cw) | multi-scan store → `seriesFor` / `seriesAcrossGroups` |
| the persisted diff-index backend (`/api/compare`, `<a>-<b>.parquet`) | the always-on diff-index → `diffOverSpan` |

And it adds the thing we can't do today: **compress the kept-forever daily scans.** The r2 public demo stores one full index per daily scan, each ~a duplicate of the last — the exact O(N) blowup pyrmts's multi-scan consolidation targets. cw-s3 measured on real path-indices: **~0.020 % churn per 12 h scan → 5.9× @ N=6, ~78× @ N=81, ~335× @ daily·1 yr, ~626× @ N=730** — compression *grows* with scan count on near-static data.

Because the diff-treemap, over-time chart and age chart are widgets of the **shared base**, cw/gcs inherit the serve wiring by rebasing. The serve side is wired **once, here**.

## 2. What pyrmts is (the adoption surface)

`~/c/pyrmts` — multi-scale timeseries pyramids: pre-compute `(shard × bin)`-tier aggregates once; serve any range × bin-budget query in O(log) bins from the edge. Polyglot: Python build (`pyrmts`, `pyrmts-engine`, `pyrmts-ops`) + TS serve (`pyrmts`, `pyrmts-cfw`, `pyrmts-geo`, `pyrmts-react`). Shipping; used by ctbk, awair, crashes; co-developed with cw-s3. Dist current @ pyrmts main `e03c490`.

A consumer provides exactly four things — pyrmts owns everything else (`pyrmts:specs/done/pyrmts-ops-adoption.md`):

1. a **pyramid config YAML** (dims, metrics/monoids, tier ladder, storage, `multiScan:`),
2. one **raw → base-tier ingester** function,
3. **storage / D1 bindings + env**,
4. thin **CLI / handler shims**.

## 3. The two orthogonal indexes (both on by default) — and the correction

These are **separate structures with separate jobs**; don't conflate them (pyrmts's own correction, 2026-09-22):

- **Multi-scan (MS) store** — `pyrmts:specs/multi-scan-consolidation.md`. Folds the re-observation (scan) axis into shards with interval-encoded SCD-2. **Bounds storage** and **serves the over-time line** (`seriesFor`, `seriesAcrossGroups`). It does **not** give cheap diffs between arbitrary scans: a pair straddling an archive junction has no stored delta → O(fleet).
- **Diff-index** — always-on, landed pyrmts main `e03c490`. A dyadic **changeset hierarchy**: per scan, one adjacency changeset (`k → k+1`, the single O(fleet) step, paid once at ingest) and one **immutable composed node per power-of-2 level**. `diff(a, b)` composes only the `popcount(|j − i|)` = O(log) disjoint nodes → **O(log N + changes-in-span)**, no snapshot read at query time. Verified against the 2-snapshot oracle for every pair, incl. remove-then-re-add netting out (changeset composition is non-invertible, hence disjoint blocks). **Serves the diff-treemap** (`diffOverSpan`). **Keep the dTM off the MS archives.**

Storage: MS = O(log N) archives under `scheme: exponential` (§3.1); diff-index = O(N log N) tiny nodes (fine on low churn). Both are independent of *whether* individual scans are consolidated.

### 3.1 MS grouping: use `scheme: exponential` from the start

The user's demo was the motivating case for this option. Fixed-K grouping ("every K scans → one archive") leaves the archive count O(N/K) — unboundedly many tiny archives for a low-churn demo kept forever. **Exponential** (Bentley–Saxe / logarithmic method) coalesces old scans into power-of-2-sized archives so the **archive count stays O(log N)**: for daily scans kept forever, ~9 archives/year instead of ~365.

- Config: `multiScan: { scheme: exponential, base: 2, dataset, tier, shard, drop: true }`.
- Run: `multiscan seal <config>` — **idempotent**; a post-scan stage fires it each cycle and it compacts as needed.
- **D1 gotcha:** exponential *compacts* — it deletes superseded manifest rows as blocks merge — so the routing-manifest → D1 sync must **mirror the row set, not append-only** (fixed-K is append-only; exponential isn't). `MultiScanD1Index` (`pyrmts-cfw`) backs the manifest.
- **Read-path gotcha (exponential-only):** an archive's **key moves as it compacts** (the key encodes `first-scan-label + block-size`; a scan starts in a size-1 archive, then merges into size-2, size-4, …). The manifest + `resolveScan` / `seriesAcrossGroups` always route to the *current* archive (self-correcting), **but any embed must resolve `(dataset, scan/path)` through the manifest per read — never cache or hard-code an archive URL.** Cache the query, not the resolved key. `seriesAcrossGroups` re-lists each read (`list_multiscans → filter tile → per-group seriesFor`), so it is automatically current.
- Reassuring corollary: **dropping individuals after `seal` loses nothing** — `extract_scan` reconstructs any single day's tree from its current archive, so "show the tree as of day X" survives compaction.

### 3.2 The disk-tree mapping: union path-index → `Pyramid` **(pyrmts to review)**

Our served artifact is a **snapshot** path-index keyed `(path)` with no event-time axis (no created-date `binCol` at the path grain). pyrmts's contract for this is explicit: a **constant `binCol`** (a `dt=0`-style column, as its unit tests construct shards) — consolidation keys on `(binCol, *dims)`, so a constant binCol reduces the logical key to `(*dims)` = `(path)` cleanly. **Precondition satisfied:** the binCol is scan-invariant per key (trivially, it's constant), so a key's `(tier, period)` placement is stable across scans and shards align 1:1.

- **dims:** `path` (+ `bucket` if we scope per location — see below). Per-depth structure stays what the path-index already carries (`depth`, `usr`).
- **metrics / monoids:** `b` (bytes, sum), `o` (objects, sum), `c2/c3/c4` (class bytes, sum), `wts`/`wb` (created-weighted, sum) — all additive, so the existing coarse-tier floors (`COARSE_EXPS`) map to pyramid tiers.
- **tier ladder:** the path-index's coarse tiers (E = 16/20/24 by subtree-byte floor) are a *value*-thresholded ladder, not a *bin*-coarsening ladder — that's the mapping to pin with pyrmts (a tier per floor? or the floor stays a disk-tree-side filter over one pyrmts tier?). **Open question #1.**

**Dataset scoping — one union dataset vs. one per location. Open question #2, and the federated-scans lever.** Today the ingestion builds **one** union path-index (three buckets grouped as the Map's top cells). pyrmts's guidance is "per-location dataset = its own dyadic ladder." Two shapes:

- **(a) one dataset for the union** — matches today's single index; simplest; one MS ladder + one diff-index. But couples the three buckets' scan cadences and makes "add a bucket / a new cloud location" a re-consolidation.
- **(b) one dataset per bucket/location** (`ctbk`, `crashes`, `jc-taxes`) — each with its own MS ladder + diff-index; the union Map reads across datasets via the manifest. This is exactly the [federated-scans](./federated-scans.md) north-star (each location consolidates independently; the union reader routes via the manifest) and lets a new location join without touching the others. Cost: the union view composes N dataset reads.

Leaning **(b)** — it is the federated shape and pyrmts's stated intent — pending pyrmts's review of the per-location scoping.

## 4. Ingest wiring (ours — `daily-ingest.yml`)

After the existing `dt-cloud path-index` build + upload, two **idempotent** post-scan stages (both no-op when nothing is new):

1. **MS seal:** `multiscan seal <config>` (`scheme: exponential`) → consolidates the new scan into the dyadic ladder; then sync the routing manifest → D1 **mirroring the row set**.
2. **Diff-index update:** `pyrmts-engine diffindex update -D <dataset> -k <tile-key> -R <scans-root> -o <index-root> <config>` → appends every not-yet-indexed scan in order (one adjacency changeset + the composed power-of-2 nodes). Layout: `<index-root>/diffidx/<dataset>/index.json` (ordered scan labels) + `L{level}/{i}.parquet` (standard changeset rows: key cols + `{c}__a` / `{c}__b`). Nodes are **append-only / immutable**, so the R2 sync is plain object puts.

Both run per dataset (per §3.2 (b), once per bucket). The existing generation dir (`listing/<date>/index/<gen>/`) keeps serving the *current* subtree/map read until the pyrmts read path replaces it; individuals are only `drop`ped after digest-verify (Phase 2c verify-then-drop).

## 5. Serve wiring (ours — `site/functions`, the base; inherited by cw/gcs)

- **Over-time line** (`/api/series`): `pyrmts-cfw` + `MultiScanD1Index` for the D1-backed manifest; `resolveScan` / `readMultiScan` for routing; `seriesAcrossGroups` for a per-path line across archives. Per-read manifest resolution (never a cached archive URL — §3.1). Footer-pruned per-path reads stay O(pruned).
- **Diff-treemap** (`/api/diff`): `diffOverSpan(scans, schema, a, b, loadNode)` from `pyrmts` — `scans = parseDiffIndexManifest(index.json bytes)`; `loadNode(level, i)` = our R2 fetch of `L{level}/{i}.parquet` → `readChangesetNode(bytes)`. Returns changeset rows (the same shape `diffTables` gives), sorted `(*dims, binCol)`; reversed pairs (a after b) are handled (before/after swapped). **This read is NOT per-key prunable** — a changeset is cross-key; it prunes by *span* (only the jump nodes), which is the whole win. Do **not** try to footer-prune it per path the way the over-time read does.
- **"Tree as of day X"** (subtree/map for a historical scan): `extract_scan` from the current archive once individuals are dropped.
- The age chart (currently hidden on r2) un-hides once the pyramid serves the created-date tiers.

## 6. What this retires (in order, each behind a working replacement)

1. the over-time index (`_lib/overTime.ts`, `overtime.py`, `api/series.ts` additions) → MS store reads,
2. the persisted diff-index backend (`/api/compare` parquet) → `diffOverSpan`,
3. `write_age_pyramid` / `age-pyramid-<bin>.parquet` → the pyramid's created-date tiers,
4. eventually the per-generation full path-index tiers → `extract_scan` + the MS store (storage collapses by the §1 factors).

## 7. Phases

1. **Pin the mapping with pyrmts** (this spec's §3.2 + open questions #1/#2). No code.
2. **Pyramid config + ingester** for the r2 datasets (per bucket); wire `multiscan seal` + `diffindex update` into `daily-ingest.yml` (idempotent stages), R2 puts + mirrored D1 manifest. Individuals kept (no `drop`) until the read path is proven.
3. **Serve path in `site/functions`**: over-time via `pyrmts-cfw` manifest routing; diff via `diffOverSpan`. Behind a flag / parallel endpoint until byte-equivalent to today's answers.
4. **Cut over + retire** per §6; enable `drop: true` after verify-then-drop; un-hide the age chart.

## 8. Coordination

- **pyrmts** reviews this spec (Pyramid mapping, per-location dataset scoping, diff-index wiring). Ping the pyrmts session when it's up (done on commit).
- **cw-s3** owns the DuckDB producer + CFW reader being upstreamed into pyrmts, and is wiring MS for its 12 h GCS fleet; our serve wiring in the base is what cw/gcs inherit on rebase — coordinate via the mgu handoff (`/Users/ryan/c/oa/marin-gcs-usage/specs/disk-tree-as-base.md`).
- Related specs: [`union-of-roots.md`](./union-of-roots.md) (the base plan), [`federated-scans.md`](./federated-scans.md) (per-location scans + union reader — §3.2 (b) is its realization), `pyrmts:specs/multi-scan-consolidation.md` (§"grouping policy" / Phase 2d for exponential), `pyrmts:specs/pyrmts-column-cube.md` (the *other* redundancy — across tiers within a scan; separate axis, not this spec).
