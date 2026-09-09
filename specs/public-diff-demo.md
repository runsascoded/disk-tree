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

### Phase 2 — publish diff indexes  ⟵ OPEN DECISION

The compare view needs a per-pair diff. Two serverless mechanisms:

- **(A) Persisted-index slice (recommended).** Publish a `<a>-<b>.parquet` diff
  index (same layout as a scan blob, so `ui/cfn/parquet.ts` pushdown reuses) +
  a small manifest into the demo bucket, and Phase 3's Function slices it with
  range reads. *Fastest per request.* Infra cost: the re-scan runner must know
  the previous scan to build the index — bootstrap by
  `disk-tree scans register r2://disk-tree-demo/scans/` (imports the bucket's
  existing manifests) before `index` (drop `-D`), then **upload the built diff
  index** (`~/.config/disk-tree/diffs/<a>-<b>.parquet`) to the bucket. Needs a
  publish step: either teach `index --to` to also upload the diff index it
  builds, or a small `diff-index --to <url>` (mirrors `snapshots --diffs`, which
  today writes the *file-tree* snapshot layout, not this scans-manifest tier).
- **(B) Recompute in the Function (marin's way).** No persisted index: the
  compare Function reads both scan blobs from R2 and computes the diff at
  request time (edge-cached per `(scan1,scan2,path,budget)`), as
  `site/functions/api/diff.ts` does in marin. Lower publish infra, heavier per
  request (bounded — it reads only the path-prefix slice of each scan, not the
  whole blob).

Recommendation: **(A)** — flat per-request cost, reuses `scan.ts`/`parquet.ts`,
matches disk-tree's own diff-index architecture. (B) is the lower-infra fallback
if the diff-index publish step proves fiddly.

### Phase 3 — serverless compare Function

`ui/functions/api/compare.ts` (+ `/api/diff/status` if kept), the direct analog
of `scan.ts`: read the diff blob (A) or the two scan blobs (B) from R2, rebase
to the requested `uri`, apply `max_rows`/`min_frac`. Flip `compare: true` in
`capabilities.ts`. `CompareView` already renders the core widgets and calls
`compareScans`/`compareScansRecursive`/`fetchDiffIndexStatus`; wire those to the
Function (with (A) the progressive walk→index refetch collapses to a single
index read). Surface a compare entry point in the demo nav.

### Phase 4 — union `path` / `hbt`  ⟵ OPEN: data source

`~/c/hccs/{path,hbt}` are public-data projects (like `crashes`/`ctbk`) but have
**no R2 bucket** (unlike `nj-crashes`/`ctbk`/`jc-taxes`). To include them the
source must be decided:

- If their published data lives in a bucket / at a URL, scan *that* (cloud,
  GHA-friendly — just add to the Phase 1 loop).
- If it's the **laptop working dir** (`~/c/hccs/{path,hbt}`, 3.3 G / 1.2 G), a
  GHA runner can't reach it → the scan must run laptop-side (a launchd job or a
  manual `index --to r2://disk-tree-demo/scans/`), and the listing includes
  repo internals (`.git`, `node_modules`, build artifacts) — fine for a
  disk-usage demo, but confirm scope (whole dir vs a subpath).

### Phase 5 — demo UX

A discoverable compare/diff entry point (nav item or a default landing on the
freshest diff), analogous to marin's `#diff` section. The marking / ownership /
sweep machinery of mgu/cw-s3 is checkpoint-domain-specific and out of scope;
disk-tree's demo angle is treemap + diff + age-lens + filter over public data.

## Open decisions (need user input)

1. Phase 2 mechanism: **(A) persisted-index slice** (recommended) vs (B)
   recompute-in-Function.
2. Phase 4 source for `path`/`hbt`: a public bucket/URL (cloud) vs the laptop
   working dir (laptop-side scan); and, if the latter, scan scope.
3. Phase 1 cron cadence/time (daily assumed; align with nj-crashes' refresh).
