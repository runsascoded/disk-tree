# Public diff demo (r2.rbw.sh): periodic re-scans + a served compare view

Grow the public open demo (`disk-tree-demo`, r2.rbw.sh) toward the mgu/cw-s3
shape: accumulate dated scans of the public buckets so the **diff treemap +
diff table** (`@rdub/treemap`'s `DiffTreemap`/`DiffTable`, already used by
`ui/src/components/CompareView.tsx`) can be served at the edge.

## Terminology

r2.rbw.sh is **Cloudflare Pages + serverless Pages Functions** over an R2
bucket — the same serverless model as mgu's marin site and cw-s3, *not*
static-HTML-only. "Serving compare" means adding a Pages Function; there is no
inherent perf penalty. The two candidate mechanisms (below) are both serverless.

## Current state

- The demo was populated **once, by hand** (`specs/done/cfn-demo-and-flask-localhost-peer.md`):
  `disk-tree index r2://<bucket> --to r2://disk-tree-demo/scans/` for
  `nj-crashes` / `jc-taxes` / `ctbk` → **one scan per bucket**. Nothing
  re-scans them; `reduce.yml` is `workflow_dispatch`-only, `sync` writes only to
  the local/private store, and the sole scheduled job is the 6 h launchd scan of
  the private `/Users/ryan`.
- Compare is **not served statically**: `ui/functions/api/capabilities.ts` says
  `compare: false`; there is no `/api/compare` / `/api/diff/status` Function, so
  those routes 501. `CompareView` works fully against the Flask server only.
- The core `DiffTreemap` + `DiffTable` are complete (`packages/treemap/src/diff/`)
  and consumed by `CompareView` — the widgets are *ahead* of what marin ships
  (marin's site diff-treemap is a bespoke reimplementation with no diff table).

## Phases

### Phase 1 — periodic re-scans ✅ (`.github/workflows/rescan-demo.yml`)

Daily cron + `workflow_dispatch`: for each demo bucket,
`disk-tree index -C -q -D r2://<b> --to r2://disk-tree-demo/scans/`. Each run
adds a `<uuid>.parquet` + `.scan.json` under `scans/`; the Functions already
list newest-per-path and expose `/api/scans/history`, so dated points just
accumulate. Same R2 auth as `reduce.yml`. `-D` because a stateless runner has no
prior scan locally (diff indexes are Phase 2). **Needs the user to enable the
schedule / first-run it; cron time (`23 8 * * *`) is a guess — align with
nj-crashes' daily refresh.**

### Phase 2 — diff computation  ⟵ OPEN DECISION (storage policy)

A diff index `<a>-<b>.parquet` is the per-path outer join of two scans (old/new
size+count + status, sorted `(depth,path)`); its size ≈ **one scan**,
independent of N. The only question is *how many pairs we persist* — a policy
choice, not inherent to any mechanism. **Never pre-build all i×j pairs (O(N²),
unbounded).** Options, from least to most caching:

- **(B) Pure on-the-fly (default).** No persisted diff index. The compare
  Function reads the path-prefix *slice* of both scans from R2 and outer-joins
  them at request time (bounded per request — thousands of rows for a subtree,
  not the whole blob), edge-cached per `(scan1,scan2,path,budget)`. This is what
  marin's `site/functions/api/diff.ts` does. **O(1) diff storage**, bounded
  compute. The "compute diffs on the fly" reading.
- **(A-consecutive) Cache consecutive pairs only.** Persist just scan_i↔scan_{i+1}
  — exactly what `disk-tree index` already builds ("diff index against the
  path's *previous* scan"): **one** new index per re-scan → **O(N)** total, each
  O(scan), GC the oldest (keep last K). Makes the default "latest vs previous"
  view an instant slice (`ui/cfn/parquet.ts` pushdown, like `scan.ts`).
  Non-adjacent pairs still fall through to (B). Publish step needed: teach
  `index --to` to also upload the diff index it builds, or a `diff-index --to
  <url>` (mirrors `snapshots --diffs`, which writes the *file-tree* layout, not
  this scans-manifest tier).

**Recommendation: start with (B)**, add the (A-consecutive) O(N) cache only if
the default diff feels slow. Storage stays O(1)→O(N), never O(N²); per-request
compute is bounded either way. Phase 3's Function implements (B) first; the
cache is a later, transparent fast-path (Function checks for a persisted index,
else recomputes).

### Phase 3 — serverless compare Function

`ui/functions/api/compare.ts` (+ `/api/diff/status` if kept), the direct analog
of `scan.ts`: read the diff blob (A) or the two scan blobs (B) from R2, rebase
to the requested `uri`, apply `max_rows`/`min_frac`. Flip `compare: true` in
`capabilities.ts`. `CompareView` already renders the core widgets and calls
`compareScans`/`compareScansRecursive`/`fetchDiffIndexStatus`; wire those to the
Function (with (A) the progressive walk→index refetch collapses to a single
index read). Surface a compare entry point in the demo nav.

### Phase 4 — union `path` / `hbt`

Both are DVC projects publishing to **S3**: `s3://hudcostreets/path/.dvc/cache`
and `s3://hudcostreets/hbt/.dvc/cache` (same DVC-cache shape as `ctbk`). Two
ways in, both cloud/GHA-friendly (no laptop dependency):

- **Scan S3 directly** — add `s3://hudcostreets/path` / `…/hbt` to the Phase 1
  loop; disk-tree lists `s3://` natively. Needs the `hudcostreets` bucket's AWS
  creds in the runner (a second credential alongside the R2 token).
- **Migrate to R2 first** (user is handling separately) — copy to `r2://path` /
  `r2://hbt` (or a shared bucket), then they join the loop under the existing R2
  credential, no extra secret.

### Phase 5 — demo UX

A discoverable compare/diff entry point (nav item or a default landing on the
freshest diff), analogous to marin's `#diff` section. The marking / ownership /
sweep machinery of mgu/cw-s3 is checkpoint-domain-specific and out of scope;
disk-tree's demo angle is treemap + diff + age-lens + filter over public data.

## Decisions

1. **Phase 2 storage policy** — start **(B) pure on-the-fly** (O(1) storage,
   bounded compute, edge-cached); add the **(A-consecutive) O(N) cache** later
   only if the default diff feels slow. Never O(N²). *(Resolved: on-the-fly
   default per user; the persisted set, if any, is O(N) consecutive.)*
2. **Phase 4 source** — `s3://hudcostreets/{path,hbt}` (DVC/S3). Scan S3 directly
   (extra AWS cred) or user migrates to R2 first (user handling separately).
3. **Phase 1 cadence** — daily; time non-critical (nj-crashes' GHA drifts hours),
   so no tight alignment needed.
