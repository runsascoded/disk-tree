# Unified path store: objects and dirs in one served table

**Status:** draft, 2026-09-30. Base work (`cloud`). Origin: Ryan's pushback in the cw-s3 session (12:11, 13:26 UTC) on a proposed "objects tier": *"the simplest mental model, to me, is a unified path store that holds objs and dirs, bc that's what a TM needs: paths by size … what I especially don't want is thresholded queries that will merge independent result sets from objs and dirs"*. This spec is the from-scratch design, and the retirement of the raw listing that follows from it.

## 0. Where we are, and why it got here

Every served tier today holds **directory rows only**. `cloud/src/dt_cloud/index.py:85` builds cw's path index `FROM read_parquet(l2) WHERE kind = 'dir'`; gcs's `viz.write_path_index` rolls objects up into `(bucket, dir)` groups and never emits them. Objects exist only in the raw listing (`cw-l2/<scan>/<bucket>.parquet`, `listing/<date>/<bucket>/*.parquet`) and reach the browser only through `/files`. The consequences are the hacks Ryan keeps hitting: the "objects aren't listed yet" note (`App.tsx:1051`), the `o ≤ 1 → pin instead of drill` rule (`Treemap.tsx:605`), children-table rows that aren't links (`ChildrenTable.tsx:229`), `DiffRow.k` hard-coded to `'dir'` (`view.ts:742`), a diff that can't say which object was added.

It got here by descent, not decision. The served index is mgu's August 2026 gcs rollup, built to fit a 613M-object fleet into something a Worker could range-read; dropping objects was the shortcut that made it fit. DT's own layer-2 listing has always had file rows (`aggregate_duckdb.py`, `kind` on every row, sorted `(depth, path)`), and the DT demo's `ui/functions/api/scan.ts` reads it directly, objects included. When the site moved onto the DT base, the *writer* was ported and the L2 was kept alongside as an archive; each later feature (coarse tiers, age pyramid, over-time, diff) was built on the dir-only rows and added its own leaf special-case. Nobody reconciled the two because the raw listing "held" the objects and `/files` was the escape hatch.

The cw L2 is, in fact, already ~90% of the unified store: 54.2M rows, objects and dirs, `(depth, path)` sort, per-row `size`/`mtime`/`kind`/`n_children`. The gap is 64K-row groups instead of 8k with a D1 footer, three columns (`usr`, `a`, class bytes) that cw doesn't use anyway, and readers that never open it. The design below makes that file *the* served index, so there is one artifact instead of two.

## 1. The store

One table per scan: **one row per path**, object or directory. There is no objects tier and no dirs tier; a tier is a *filter* or a *sort* of these rows, never a different table.

### 1.1 Columns

| col | type | object row | dir row | notes |
|---|---|---|---|---|
| `path` | string | bucket-prefixed key | bucket-prefixed prefix, no trailing `/` | depth 1 = bucket; depth 0 = fleet root `''` |
| `depth` | int16 | | | segments in `path` |
| `k` | int8 | `1` | `0` | kind. Sort key inside a `(depth, path)` tie is unnecessary: a path is one kind |
| `b` | int64 | own size | Σ descendant object bytes | monoid: `+` |
| `o` | int64 | `1` | descendant object count | monoid: `+` |
| `n` | int32 | `0` | direct children (objects + dirs) | what `(other).f` and "flat dir" detection need |
| `mt` | int64 | native primary stamp | max over descendants | monoid: `max`. Epoch seconds |
| `wts` | double | `b·mt` | Σ `b·mt` | monoid: `+`; `wts/b` = size-weighted mean stamp (today's `d`) |
| `ct` | int64 \| null | native created, where the platform has it | max over descendants | GCS only; S3/R2 null (RLE, free) |
| `usr` | string \| null | owner (attribution) | owner slice; one row per `(path, usr)` as today | null = unclaimed |
| `a` | int32 \| null | last-read day | max over descendants | gcs access log, cw null |
| `c2 c3 c4` | int64 | `b` or 0 | Σ per class | as today; cw all 0 |
| `h` | list<int32> \| null | null | descendant size histogram, whole-log2 bins, counts | monoid: vector `+`. §1.4 |
| `hb` | list<int64> \| null | null | same bins, bytes | optional; `h` alone answers top-N |

Rules that make this *one* table rather than two glued together:

- **Every column is defined for both kinds, with the same meaning.** `b` is "bytes at or under this path"; for an object that's its size. `mt` is "latest stamp at or under this path"; for an object that's its own. `n`/`h` are "direct children" / "descendant sizes"; for an object those are `0` / null. Nothing is a different shape per kind, so a reader never branches on `k` to interpret a value. `k` exists for the *renderer* (leaf vs branch) and the *executor* (delete key vs prefix), not for the reader.
- **Dir mtime is one scalar.** Ryan's worry was that a per-dir *distribution* would be a different shape from an object's scalar and so "should arguably be a different col". It is: the distribution is the **age pyramid tier**, keyed by path, exactly as today (`(path, depth, binstart, b, o)`). The row itself carries the two monoidal scalars that every platform can produce (`max`, size-weighted mean), which happen to be what the treemap's age lens and the children table use.
- **Native stamps are kept verbatim, per platform.** S3/R2 has only `LastModified` (upload time) → `mt`, `ct` null. GCS has `timeCreated` and `updated` → `ct` and `mt` (objects are immutable, so these differ only on metadata rewrites; today's gcs index uses `created`, and `ct` keeps that available). The store's config says which stamp `mt` is (`mtime: last_modified | updated`), so the FE's "age" label is honest per store. We never store a synthesized stamp in place of a native one; that is the one thing that couldn't be undone.
- **Rollup columns on object rows cost nothing.** `n=0`, `h=null`, `ct=null` on 46M consecutive-ish rows are RLE/dictionary-encoded to bytes. Serving cost is column projection: a treemap read projects `path, depth, k, b, o, wts` and never decodes `h`, `usr`, or the class columns. Parquet column chunks are independent, so the columns objects don't use are not read, let alone decompressed.

### 1.2 Sorts (variants of the same rows)

Two sorts, plus the owner sort gcs already has. Each is the *same rows*, so a query that reads one variant gets one result set. There is never a case where objects come from one file and dirs from another and the reader merges and truncates.

1. **`path`: `(depth, path)`.** Today's sort. Subtree and children range reads, point lookups, ancestor chains. Row groups of 8k rows, footer in D1 (`index_row_groups`: `d_min/d_max`, `p_min/p_max`, `b_max`, `u_min/u_max`) — unchanged shape, `b_max` now covers objects too.
2. **`size`: `(depth, parent, b desc, path)`.** One contiguous range per parent, biggest first. This is the answer to "largest N children of P, page K" as a single small range, and to the flat-directory problem (§2.2). Footer stats: `p_min/p_max` over `parent`, `b_max` = first row's `b`. **Filtered to flat parents by default** (§2.2): only rows whose parent has `n ≥ FLAT_N` (8192, one row group) are in it. For every other parent the `path` variant's child range fits in one or two groups and sorting 8k rows in the Worker is free. That keeps `size` at a few percent of `path` while giving exactly the parents that need it.
3. **`user`: `(usr NULLS LAST, depth, path)`** — gcs's owner-lens sort, unchanged, now with object rows (each object has exactly one owner, so it adds one row per object, no slices).

Ryan asked whether the size-descending sort could *replace* the byte-floor tiers. Not as a single global `(b desc)` sort: its prefix down to floor F is the coarse tier's row set, but in the wrong order for a subtree read under P (every fleet-wide path ≥ F, not just P's), and the treemap wants ancestor-closed subtrees, not top-K lists. So the byte-floor tiers stay, as filters of the `path` sort (§1.3), and the size sort is per-parent, which is the shape the children table and flat dirs need. Both, as he guessed.

### 1.3 Byte-floor tiers (filters of the `path` sort)

`coarse<E>` = the `path` sort restricted to `b ≥ F_E`, `F_E = 2^(round(log2 fleet) − E)`, unchanged rule (`index.py:89-113`). Objects now qualify: a 20 GiB object is in `coarse16` beside 20 GiB dirs. Because `b` is the same monoid on both kinds, the kept set is still ancestor-closed, and `(other)` still equals `parent.b − Σ kept`. Ryan is "open to more granular tiers": with objects the tier is a pure filter, so `E ∈ {16, 18, 20, 22, 24, 26}` costs only the rows kept. The reader's rule (coarsest tier with `thrAll ≥ floor`, `view.ts:396`) doesn't change.

The dirs-only "table" Ryan wondered about (*"one table that's the union and one that's just dirs-only?"*) is `WHERE k = 0` over these same rows, and the only reader that wants it is the over-time builder (§4.4), which applies that filter itself. Nothing stores it.

### 1.4 Descendant size histogram `h`

Whole-log2 bins, `h[i]` = number of descendant objects with `2^i ≤ size < 2^(i+1)`, `i ∈ [0, 48)`, so 48 int32s per dir row, monoidal by vector add, computed in the same aggregation pass as `b`/`o` (the engine already computes a 41-bin `size_hist_n`/`size_hist_bytes` per dir — mgu-scale-unification item E — this is that, kept). Ryan floated tenth-of-a-log2 bins: 10× the row cost (480 ints per dir, ~2.9B ints on cw's 6.2M dirs before compression, most zero). The whole-log2 version is what §2.3 needs; a quarter-log2 refinement is a one-constant change if a view wants it, and can be measured then.

### 1.5 Cost

Per-row bytes today: cw dir rows ~40 B (0.25 GiB / 6.2M); gcs raw objects ~17 B (10.5 GB / 613M; `name` dominates). Estimates for the unified `path` variant, Snappy, before zstd (~×0.65) and before cross-scan consolidation:

| store | rows/scan | est. bytes/scan | today (listing + indexes) |
|---|---|---|---|
| cw (one bucket) | 52M (46M obj + 6.2M dir) | ~2 GiB | 3.6 GiB + 0.56 GiB |
| gcs fleet | ~800M (613M obj + 179M dir; +owner slices on dirs) | ~18–25 GiB | ~10.5 GB listing + fine tier 220M rows |
| r2 demo | ~1M | tens of MiB | — |

The store **replaces** the raw listing rather than adding to it (§4), so cw's per-scan bytes go *down* (2 vs 4.2 GiB) and gcs's roughly double the listing's while retiring `listing/` (2.45 TiB today). The `size` variant is a few percent on top (flat parents only). `user` on gcs adds ~one more `path`-sized file, as it does today for dirs. What the daily bytes really turn on is retention: per-scan materialization is only needed for the latest scan(s); history goes to the object-level SCD-2 archive (listing-slim phase 3, reshaped: §4.5), which at cw's 3–4%/12h churn is 10–20 GiB for *all* scans instead of 2 GiB per scan.

D1: the fine tier's footer grows with rows. cw 6.4k groups (from 0.8k); gcs ~100k groups (from 27k) × ~250 B = ~25 MB/scan/variant. At `INDEX_RETAIN=120` and three variants that is ~7 GB of a 10 GB D1. Two levers, both existing: retain fine-tier footers for fewer scans than coarse ones (history reads go to the archive), and the `.groups.json` blob fallback (`index.ts:240`) for older generations. Measure first (phase 0); pick the retention split then.

## 2. Reads

### 2.1 Subtree (treemap)

Unchanged in shape: `readRows(tier, dP+1 … , P/ … P0, thrAt)` over the chosen byte-floor tier, keep `b ≥ thrAt(depth)`. Objects that pass the threshold come back as rows with `k=1`; the FE draws them as leaves. `(other)` per parent = `parent.b − Σ kept`, `f = parent.n − kept`, now counting both kinds. **No second read, no merge.** The `o ≤ 1` and "childless dir" special cases go away because a row says what it is.

The one new branch in `readView` is which *variant* answers, not which *kind* is included (§2.2).

### 2.2 Flat directories and the children table

A parent with millions of direct children (gcs `datakit/store*`: 164M dirs under a few prefixes; 96% of gcs dirs hold one object) breaks range reads: the child range spans hundreds of groups and the 700k-row cap (`index.ts:444`) fires. This is not an objects problem; it exists for dirs today, and objects make it universal. The `size` variant (§1.2) answers every query that is "children of P sorted by size": treemap drill-one-level (`thr` prefix of P's range), children-table paging (offset/limit within P's range), largest-N-children. `readView` uses it when `P.n ≥ FLAT_N`; otherwise the `path` variant's child range (≤ 1–2 groups) is sorted in memory. Either way one variant, one result set.

Children-table sorts other than size (name, mtime) on flat parents page through the `path` range for name (already sorted) and are unsupported on flat parents for mtime (say so in the UI; a flat parent's mtime order needs a third sort nobody has asked for).

### 2.3 "Largest N under P", recursive

Read `P.h` (one row, point lookup), walk bins from the top until the cumulative count reaches N → threshold `t` (the bin's lower edge). Then one thresholded `path`-variant range read under P with `b ≥ t` and `b_max` group pruning; it returns ≤ ~2N rows (the top bin may overshoot by its population), objects and dirs alike, sorted in memory. This is the query Ryan named as the anti-pattern's home ("thresholded queries that … fetch up to a full page's worth from each, and drop half") and it is answered from one file with one predicate because the histogram picks the predicate.

### 2.4 Diff, series, marks, sweep

- `/api/diff`: both scans read at one shared threshold as today; rows now carry `k`, so added/removed *objects* are real diff rows. `DiffRow.k` stops being a constant.
- `/api/series` and over-time: unchanged; §4.4 says what the over-time builder reads.
- Staged delete / marks: a staged URI may be an object; the executor already deletes by key. `readAsks`'s point lookup works on object rows as-is (`(depth, path)` sort).
- Sweep manifests (`sweep.py:122`, `cli.py:1219`, `sweep_exec.py`): read `k=1` rows from the store instead of the raw listing; they need `path, b, mt, ct, c2..c4, parent`, all present (`parent` is derived from `path`, as `uri` is in listing v2).

## 3. The browser

`k` goes on every node (`ViewNode.k`, `types.ts`). Then:

- **Treemap:** `k=1` cells are leaves. Click opens the object (below); no pin-instead-of-drill, no `(`-prefix name test. `k=0` cells drill, always, including childless ones under budget.
- **Children table:** every row is a link: dirs navigate, objects open. The "N objects and no directory ≥ thr" note, the `leaf` canvas class, and the `!n.c` date-colouring guard are deleted, not conditioned.
- **Opening an object = file-tree inside disky.** Disky owns navigation (treemap, table, breadcrumbs, marks); the leaf viewer is `@rdub/file-tree`'s renderer for the object's type (parquet schema/row-group paging, CSV, JSON, text, images) reading the object through the gated `/v1/files/get`. This is multi-store phase 3b's "combine FT and DT" made general: it's how *every* store opens a blob, not just `/meta`. The `/files` page retires once this lands; `/v1/files` stays as the raw read API.
- **Diff treemap:** the "a leaf here is still a directory" rule (`DiffTreemap.tsx:389`) goes; `k` decides.

Per the `objects-first-class` rule: every special case that exists only because objects were absent is removed, not kept behind a flag.

## 4. Writers, and the raw listing's retirement

### 4.1 cw (`index-write`)

`index_rows_sql` drops `WHERE kind = 'dir'`, adds `k`, `n` (`n_children`), `mt` (`mtime`), `wts` (`mtime_mean·size`, exact from L2), `h` (`size_hist_n`), and writes the `size` variant for flat parents. Everything else (bucket prefixing, `depth+1`, coarse floors, age pyramid) is as today, except the age pyramid and over-time read from the store instead of the L2. At that point `cw-l2/<scan>/<bucket>.parquet` holds nothing the store doesn't: the job stops copying it (the store *is* the layer-2; `disk-tree import` writes it directly in 8k groups, `BLOB_ROW_GROUP_SIZE` per-target), and `publish-r2 -L` (e1fea12) becomes moot because the served file is the only file.

### 4.2 gcs (`path-index`)

`dir_stats`/`ptu` keep producing dir rows with slices; object rows pass through from `prepare_listing` with the same deepest-prefix attribution join (no ancestor explosion for objects: they are leaves) and `ct`/`mt` from `created`/`updated`. The union is sorted `(depth, path)` and written with the same coarse-tier and by-user passes. DuckDB spills; the Batch highmem node already does the 220M-row dir build. Phase 0 measures the 800M-row build's wall time and peak there before committing the schedule.

### 4.3 r2 demo (`daily-ingest.yml` → `path-index`)

Same writer as gcs, ~1M rows, public. This is where phases 1–3 ship first: no auth, small, CI-deployed, CIC-able at r2.rbw.sh.

### 4.4 Age pyramid, over-time

Age pyramid: built from `k=1` rows of the store (today: `kind='file'` rows of the L2) — same SQL, different source. Over-time: from `k=0` rows (§1.3), unchanged rows in, unchanged output. Both filters are on the store's `kind`; neither needs the listing.

### 4.5 listing-slim, re-shaped

Phase 1 (v2 format) and phase 2 (`recompress`, `-L`/`prune-r2-listings`) stand: they shrink and dedup the listings that exist *until* the store replaces them, and the store's writer reuses `listing_format`'s codec and KV conventions. Phase 3 (object-level SCD-2 across scans) becomes **"consolidate the path store across scans"**: same pyrmts mechanism as over-time (`Pyramid(binCol='depth', dims=[path], metrics=…)`, K-scan sealed groups), over the whole row set. That archive is what makes per-scan retention short: an old scan's rows are reconstructible from the intervals covering it. The raw listing then has no reader and no writer.

### 4.6 What "retire the raw listing" means, concretely

| reader today | after |
|---|---|
| `index-write` / `path-index` (index from listing) | the store *is* the output of the scan; no separate index build from a listing |
| age pyramid, over-time | read the store (§4.4) |
| `/files`, `/v1/files` browsing of `cw-l2/`, `listing/` | the store's leaf viewer (§3); `/v1/files` remains for the store's own files |
| sweep manifest builders | read `k=1` rows (§2.4) |
| `recompress` targets | the existing back-catalogue, until consolidated (§4.5) |

## 5. Phases

0. **Measure** (base, this session + a node): build the unified `path` + `size` variants for one cw scan from its L2 (a DuckDB query on `mgu`, not the laptop) and for the r2 demo; report rows, bytes per variant, groups, D1 footer bytes, and the gcs 800M-row build's cost on the Batch node. Confirm or correct §1.5 before phase 1.
1. **Writer** (base): `index.py` + `viz.py` emit the unified rows, `k/n/mt/ct/h`, coarse tiers over both kinds, the flat-parent `size` variant; `index_schema.version` bumps so a reader knows the store has objects; `index-sync` learns the `size` variant. Golden tests: dir rows byte-identical to today's index on the fixture (the store is a superset).
2. **Readers** (base): `k` through `readRows`/`readView`/`buildDiff`/subtree/children; variant choice for flat parents; §2.3 top-N. A reader on a pre-phase-1 generation (no objects) behaves exactly as today, so deploy order is free.
3. **Browser** (base): §3, including the file-tree leaf viewer and the deletion of every leaf-dir special case; `/files` retires (multi-store 3b lands here).
4. **Ship on r2.rbw.sh** (CI, no go needed): phases 1–3 live on the demo; CIC.
5. **cw** (cw session, Ryan's go): image rebuild, first unified scan, `size` variant synced, listing copy stops; then gcs (gcs session): Batch sizing from phase 0, then the same.
6. **Consolidate + retire** (base then deployments): §4.5 archive, retention split for D1 footers, sweep readers on the store, `recompress` finishes the back-catalogue, `listing/` and `cw-l2/` listings deleted after the archive verifies against them.

## 6. Non-goals / open

- Not a new file format: parquet, `(depth, path)` sort, 8k groups, D1 footer — everything the readers already know.
- Not a change to marks, staged delete, or the executors beyond reading object rows.
- Open: whether `hb` (bytes per bin) is worth storing beside `h` (counts). Only a "bytes above size X under P" view needs it; defer until one asks.
- Open: gcs `user` variant size with objects (one row per object); if it doubles the daily bytes, the owner lens can prune the `path` variant by `u_min/u_max` instead, which the footer already carries.
- Open (from cw-s3): which store `/meta` hangs off. Orthogonal to this spec; `/meta` is just another store with the same table.

[view-serving]: ../wt/gcs/specs/view-serving.md
[mgu-scale-unification]: mgu-scale-unification.md
[listing-slim]: listing-slim.md
[multi-store]: multi-store.md
[obs-axis-indexing]: obs-axis-indexing.md
